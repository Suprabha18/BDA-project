# LogScope — Web Server Log Analytics Dashboard

A modern, interactive web dashboard integrated with an existing Apache Hadoop MapReduce Java project (**Web Server Log Analysis using Hadoop and MapReduce**).

---

## 1. Overview & Architecture

LogScope connects a lightweight, robust Python Flask backend with the compiled Java Hadoop MapReduce engine (`WebLogAnalysis.jar`). Every metric displayed in the dashboard is derived from the real MapReduce output (`output/part-r-00000`).

### Execution Flow:
```
[User Browser]
      │
      │ 1. Uploads .log/.txt or selects default sample
      ▼
[Flask Backend (dashboard/app.py)]
      │
      │ 2. Validates file, resets output directory,
      │    and executes Hadoop MapReduce via subprocess
      ▼
[Hadoop MapReduce (WebLogAnalysis.jar)]
      ├── Mapper: WebLogAnalysis$LogMapper (Regex parsing)
      ├── Shuffle & Sort: Groups by keys (TOTAL_REQUESTS, URL:*, STATUS:*, IP:*)
      └── Reducer: WebLogAnalysis$SumReducer (Aggregates counts)
      │
      │ 3. Writes reducer results to output/part-r-00000
      ▼
[Flask Parser]
      │
      │ 4. Reads part-r-00000, computes KPIs, categories, shares
      ▼
[Dashboard UI (HTML5 + CSS3 + Vanilla JS + Chart.js)]
      │
      └─ Displays updated metrics, charts, and IP distribution table
```

---

## 2. Prerequisites

The dashboard runs within **Ubuntu WSL (Windows Subsystem for Linux)** where Hadoop and Java are configured:

1. **Ubuntu 22.04 / 24.04 on WSL2**
2. **Java JDK 8 or 11/17+** (`java -version`)
3. **Apache Hadoop 3.x** installed (e.g. `~/hadoop` or `/home/suprabha/hadoop`)
4. **Python 3.10+** with `venv` (`python3 --version`)

---

## 3. Installation & Setup (Ubuntu WSL)

Open your Ubuntu WSL terminal and follow these steps:

### Step 1: Navigate to the Dashboard Directory
```bash
cd /home/suprabha/WebServerLogAnalysis/dashboard
```
*(Or `cd /mnt/f/projects/BDA/WebServerLogAnalysis/dashboard`)*

### Step 2: Create a Python Virtual Environment
```bash
python3 -m venv .venv
```

### Step 3: Activate the Virtual Environment
```bash
source .venv/bin/activate
```

### Step 4: Install Dependencies
```bash
pip install -r requirements.txt
```

---

## 4. Starting the Dashboard

Ensure Hadoop is in your PATH (or Hadoop will be detected automatically at `~/hadoop/bin/hadoop`):
```bash
export PATH="$HOME/hadoop/bin:$PATH"
```

Start the Flask application:
```bash
python app.py
```

You should see output similar to:
```
 * Serving Flask app 'app'
 * Debug mode: off
 * Running on http://127.0.0.1:5000
Press CTRL+C to quit
```

Open your browser on Windows and navigate to:
👉 **`http://127.0.0.1:5000`**

---

## 5. How to Run Analysis

1. **Default Sample Analysis**:
   - The dashboard opens by default in "Default Sample" mode (`input/access.log`).
   - Click **▶ Analyze Logs**.
   - The backend runs Hadoop MapReduce and populates the KPI cards, charts, and IP table.
2. **Custom Log Upload**:
   - Click **📂 Choose Log File** and select any `.log` or `.txt` file (Max 16 MB) adhering to Apache Common/Combined Log format.
   - Click **▶ Analyze Logs**.
   - The file is saved safely to `dashboard/uploads/` without modifying the original sample dataset.
   - MapReduce processes the file and updates the dashboard.
3. **Switch Back to Sample**:
   - Click **↺ Reset to Default Sample** at any time.

---

## 6. Project Structure

```
WebServerLogAnalysis/
├── classes/                        # Compiled Java classes
│   ├── WebLogAnalysis$LogMapper.class
│   ├── WebLogAnalysis$SumReducer.class
│   └── WebLogAnalysis.class
├── input/
│   └── access.log                  # Original sample log (preserved)
├── output/                         # MapReduce generated output
│   ├── _SUCCESS
│   └── part-r-00000                # Tab-separated reducer output
├── src/
│   └── WebLogAnalysis.java         # MapReduce Java source code
├── WebLogAnalysis.jar              # Executable Hadoop JAR
└── dashboard/                      # Web Dashboard Integration
    ├── app.py                      # Flask backend & MapReduce subprocess runner
    ├── requirements.txt            # Python dependencies (Flask, Werkzeug)
    ├── README.md                   # Setup and usage documentation
    ├── uploads/                    # Isolated directory for uploaded log files
    ├── templates/
    │   └── index.html              # Modern dark-theme responsive UI
    └── static/
        ├── style.css               # Professional dark charcoal styling
        ├── app.js                  # Frontend controller & API fetch logic
        └── js/
            └── chart.umd.min.js    # Bundled Chart.js for offline visualizations
```

---

## 7. Metrics & Correctness

- **Total Requests**: Read directly from the `TOTAL_REQUESTS` key in `part-r-00000`. Matches the total count of log lines matching the Apache regex.
- **Unique URLs**: Count of distinct `URL:*` keys in `part-r-00000`.
- **Distinct IPs**: Count of distinct `IP:*` keys in `part-r-00000`.
- **HTTP Status Categories**: Counts distinct HTTP status classes (e.g. `2xx Success`, `4xx Client Error`, `5xx Server Error`).
- **Error Count & Rate**: Sum of requests with status `4xx` and `5xx` divided by Total Requests.
- **Match Rate**: Compares total raw lines in the input log with `TOTAL_REQUESTS`. Any malformed or non-HTTP/1.1 lines skipped by the Java regex are transparently reported.

---

## 8. Stopping the Dashboard

To stop the Flask development server:
- In the WSL terminal where the server is running, press **`CTRL + C`**.
- Deactivate the virtual environment:
  ```bash
  deactivate
  ```

---

## 9. Troubleshooting

- **Hadoop executable not found (`unavailable` status)**:
  Ensure Hadoop is installed at `~/hadoop` or set `HADOOP_BIN`:
  ```bash
  export HADOOP_BIN="/home/suprabha/hadoop/bin/hadoop"
  ```
- **Port 5000 is already in use**:
  You can run on a different port by editing the last line in `app.py`:
  ```python
  app.run(host="127.0.0.1", port=5001, debug=False)
  ```
- **Permission denied when executing Hadoop**:
  Ensure execute permissions on the Hadoop binary:
  ```bash
  chmod +x ~/hadoop/bin/hadoop
  ```
