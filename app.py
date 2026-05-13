from flask import Flask, render_template
import boto3
import threading
import subprocess
import pymysql
import creds

app = Flask(__name__)

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
table = dynamodb.Table("AircraftLiveData")

#Used ChatGPT to figure out how to run both .py files at same time (subprocess/thread)
def run_simulator():
    subprocess.run(["python3", "-u", "simulator.py"])

def run_consumer():
    subprocess.run(["python3", "-u", "consumer.py"])


def start_background_services():
    print("Starting backend services...")

    t1 = threading.Thread(target=run_simulator, daemon=True)
    t2 = threading.Thread(target=run_consumer, daemon=True)

    t1.start()
    t2.start()


def get_latest_aircraft():
    response = table.scan()
    return response.get("Items", [])


def get_history(limit=20):
    connection = pymysql.connect(
        host=creds.host,
        user=creds.user,
        password=creds.password,
        database=creds.db,
        cursorclass=pymysql.cursors.DictCursor
    )
    #pull last n records from RDS
    try:
        with connection.cursor() as cursor:
            cursor.execute("""
                SELECT * FROM telemetry_history
                ORDER BY id DESC
                LIMIT %s
            """, (limit,))
            return cursor.fetchall()
    finally:
        connection.close()


@app.route("/")
def dashboard():
    aircraft = get_latest_aircraft()
    return render_template("dashboard.html", aircraft=aircraft)


@app.route("/history")
def history():
    data = get_history()
    return render_template("history.html", data=data)


if __name__ == "__main__":
    start_background_services()
    app.run(host="0.0.0.0", port=5000, debug=False)