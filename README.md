# Web Server Log Analysis using Hadoop MapReduce

## About the Project
This project analyzes web server log files using **Java and Hadoop MapReduce** to understand website traffic.

It calculates:
- Total number of requests
- Number of requests for each URL
- HTTP status code counts (200, 404, 500, etc.)
- Number of requests from each IP address

A web dashboard can also be used to display the results visually.

## Technologies Used
- Java
- Apache Hadoop MapReduce
- Python Flask (dashboard)
- HTML, CSS, JavaScript

## Project Structure
- `src/WebLogAnalysis.java` – MapReduce code
- `input/access.log` – Input log file
- `WebLogAnalysis.jar` – Executable JAR file
- `output/` – Generated results
- `dashboard/` – Web dashboard files (if integrated)

## How to Run

Run these commands in your Ubuntu/WSL terminal.

**1. Go to the project folder**
```bash
cd ~/WebServerLogAnalysis
```

**2. Run the Hadoop job**
```bash
rm -rf output
~/hadoop/bin/hadoop jar WebLogAnalysis.jar WebLogAnalysis input/access.log output
```

**3. View the results**
```bash
cat output/part-r-00000
```

**4. Run the dashboard (optional)**

If the dashboard is available, open another terminal:

```bash
cd ~/WebServerLogAnalysis/dashboard
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python3 app.py
```

Open http://127.0.0.1:5000 in your browser.

## How It Works
The Mapper extracts information from log records, and the Reducer combines the counts. Hadoop processes the data and saves the results, which can then be displayed on the dashboard.

**Note:** The project currently runs in Hadoop local mode on a single machine.
