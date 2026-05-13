import boto3
import json
import time
import pymysql
import creds
from datetime import datetime

STREAM_NAME = "aircraft-telemetry-stream"

kinesis = boto3.client("kinesis", region_name="us-east-1")
dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("AircraftLiveData")


def get_db_connection():
    return pymysql.connect(
        host=creds.host,
        user=creds.user,
        password=creds.password,
        database=creds.db,
        cursorclass=pymysql.cursors.DictCursor
    )


def update_live_aircraft_state(data):
    #store recent telemetry data
    #get most recent state
    table.put_item(
        Item={
            "aircraft_id": data["aircraft_id"],
            "timestamp": data["timestamp"],
            "altitude": data["altitude"],
            "airspeed": data["airspeed"],
            "engine_temp": data["engine_temp"],
            "fuel_level": data["fuel_level"]
        }
    )

def store_history_rds(data):
    connection = get_db_connection()

    try:
        with connection.cursor() as cursor:
            sql = """
                INSERT INTO telemetry_history 
                (aircraft_id, timestamp, altitude, airspeed, engine_temp, fuel_level)
                VALUES (%s, %s, %s, %s, %s, %s)
            """

            #Convert timestamp to MySQL DATETIME format
            ts = data["timestamp"]

            #timestamp is number
            if isinstance(ts, (int, float)):
                ts = datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

            #timestamp is ISO string like 2025-05-11T19:02:31Z
            elif isinstance(ts, str) and "T" in ts:
                ts = datetime.fromisoformat(ts.replace("Z", "")).strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute(sql, (
                data["aircraft_id"],
                ts,
                data["altitude"],
                data["airspeed"],
                data["engine_temp"],
                data["fuel_level"]
            ))

        connection.commit()
        print("RDS INSERT SUCCESS") 

    except Exception as e:
        print("RDS INSERT FAILED:", e)  

    finally:
        connection.close()


def consume_stream():
    print("Ground station connected. Waiting for telemetry...")

    #get all shards 
    response = kinesis.describe_stream(StreamName=STREAM_NAME)

    #got assitance from ChatGPT to fix this for loop
    shard_iterators = []
    for shard in response["StreamDescription"]["Shards"]:
        shard_id = shard["ShardId"]
        iterator = kinesis.get_shard_iterator(
            StreamName=STREAM_NAME,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]

        shard_iterators.append(iterator)

    while True:
        new_iterators = []

        for shard_iterator in shard_iterators:
            response = kinesis.get_records(
                ShardIterator=shard_iterator,
                Limit=25
            )

            records = response["Records"]
            new_iterators.append(response["NextShardIterator"])

            if records:
                for record in records:
                    data = json.loads(record["Data"])
                    print("Received telemetry:", data)

                    update_live_aircraft_state(data)
                    store_history_rds(data)
            else:
                #ocassionally show if RDS has not recieved telemetry in 10 seconds
                if int(time.time()) % 10 == 0:
                    print("Waiting for telemetry...")

        shard_iterators = new_iterators
        time.sleep(2)


if __name__ == "__main__":
    consume_stream()