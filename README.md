**CS178: Cloud and Database Systems Final Project -- Ground Telemetry Simulator**
**Author:** [Chidera Agu]
**GitHub:** [chefagunwani76]

---

## Overview

<!-- Describe your project in 2-4 sentences. What does it do? Who is it for? What problem does it solve? -->
In this project, I utitlized an AWS kinesis stream to simulate a telemetry system recieving information from
a singular simulated aircraft "AC101". Every few seconds, the simulator program sends random telemetry stats to the consumer program
where the data is stored into a dynamoDB live data(most recent record) and a historical RDS (all previous). The fuel level and 
engine temp are checked and graded, where the health of the aircraft is displayed along with the live data to the flask dashboard.

---

## Technologies Used

- **Flask** — Python web framework
- **AWS EC2** — hosts the running Flask application
- **AWS RDS (MySQL)** — relational database for [historical aircraft state data]
- **AWS DynamoDB** — non-relational database for [current aircraft state data]
- **AWS Kinesis Stream** 
- **GitHub Actions** — auto-deploys code from GitHub to EC2 on push

---

## Project Structure

```
GroundTelemetryStation/
├── app.py           #Main Flask application — routes and app logic
├── simulator.py     #Simulate aircraft (Stats to be consumed by ground telemetry)
├── consumer.py      #Database helper functions (MySQL connection + consume stream from simulator)
├── creds_sample.py  #Sample Credential file 
├── requirements.txt #Python dependencies


├── templates/
│   ├── dashboard.html       #Landing page, displays current telemetry data & shows health of aircraft
│   ├── history.html         #Displays history stored in RDS table (telemetry_history)
├── .gitignore               #Excludes creds.py and other sensitive files
└── README.md
```


---

## How to Run Locally

1. Clone the repository:

   ```bash
   git clone https://github.com/chefagunwani76/cs178-flask-app.git
   cd ground-telemetry-final
   ```

2. Install dependencies:

   ```bash
   pip3 install flask pymysql boto3
   ```

3. Set up your credentials (see Credential Setup below)
   ```bash
   pip install awscli
   aws configure
      input on prompts
   ```
   create creds.py file with your  own username and password 
   reference creds_sample.py
   ```

4. Run the app:

   ```bash
   python3 app.py
   ```

5. Open your browser and go to `http://52.91.47.166:5000`

---

## How to Access in the Cloud

The app is deployed on an AWS EC2 instance. To view the live version:

```
http://52.91.47.166:5000
```

_(Note: the EC2 instance will not be running after project submission.)_

---

## Credential Setup

This project requires a `creds.py` file that is **not included in this repository** for security reasons.

Create a file called `creds.py` in the project root with the following format (see `creds_sample.py` for reference)


---

## Database Design

### SQL (MySQL on RDS)

<!-- Briefly describe your relational database schema. What tables do you have? What are the key relationships? -->

**Example:**
- **Table name:** `[telemetry_history]`
- `[id]` —the identifier for the record PRIMARY KEY
- `[aircraft_id]`—the specific id of the aircraft to group all telemetry records
- `[timestamp]` — 
- `[altitude]` — randint altitude of aircraft
- `[airspeed]` — randint speed of air surrounding aircraft
- `[engine_temp]` — randint temperature of the aircraft's engine
- `[fuel_level]` — randint fuel level

**Used for:** Stores telemtry records by id of record(auto) each row is a small window of time and is sorted sequentially by timestamp (recent @ top, past @ bottom).
The RDS is displayed in history.html, last 10 records.


### DynamoDB

<!-- Describe your DynamoDB table. What is the partition key? What attributes does each item have? How does it connect to the rest of the app? -->

- **Table name:** `[AircraftLiveData]`
- **Partition key:** `[aircraft_id]`
- **Used for:** This table keeps track of only the most recent record, replaced every 6 seconds. This db is used to display that record in the the dashboard in a way that is 
easily readable.


## AI Assistance

<!-- List any AI tools you used (e.g., ChatGPT) and briefly describe what you used them for. Per course policy, AI use is allowed but must be cited in code comments and noted here. -->
I used ChatGPT for the bootstraps and formatting of the two html files, so they would continuously replace the data displayed with a loop. I also used ChatGPT to convert the timestamp data in a way that was readable to RDS mysql. Then I finally utilized it to debug my shardIterators in consume stream, the stream was only looking at the first iterator. Finally, I used Chat to create my start_back_ground_services function in app.py so both the simulator.py and consumer.py could run with threading. Only one was running without it.