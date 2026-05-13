import boto3
import json
import random
import time
from datetime import datetime, timezone

STREAM_NAME = "aircraft-telemetry-stream"

kinesis = boto3.client("kinesis", region_name="us-east-1")
sts = boto3.client("sts")
print("SIMULATOR AWS ACCOUNT:", sts.get_caller_identity())
print("SIMULATOR REGION:", kinesis.meta.region_name)

AIRCRAFT_ID = "AC101"

print("Starting Aircraft Simulator...")

def run_simulator():
    while True:
        telemetry = {
            "aircraft_id": AIRCRAFT_ID,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "altitude": random.randint(30000, 36000),
            "airspeed": random.randint(450, 520),
            "engine_temp": random.randint(650, 950),
            "fuel_level": random.randint(20, 100)
        }

        print("Sending telemetry--",telemetry)

        response = kinesis.put_record(
            StreamName=STREAM_NAME,
            Data=json.dumps(telemetry),
            PartitionKey=AIRCRAFT_ID
        )

        print("KINESIS RESPONSE:", response)
        time.sleep(3)