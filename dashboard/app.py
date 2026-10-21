"""
LogScope — Web Server Log Analytics Dashboard
Backend Flask Application

Integrates with the existing Java Hadoop MapReduce implementation (WebLogAnalysis.jar).
Executes MapReduce jobs via subprocess and parses the actual reducer output.
"""

import os
import re
import sys
import time
import shutil
import logging
import threading
import subprocess
from datetime import datetime
from pathlib import Path
from flask import Flask, render_template, request, jsonify
from werkzeug.utils import secure_filename

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger("LogScope")

app = Flask(__name__)

# Max upload size: 16 MB
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024

# Directory and Path Resolution
# Uses resolved parents so it works regardless of working directory
DASHBOARD_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DASHBOARD_DIR.parent
JAR_FILE = PROJECT_ROOT / "WebLogAnalysis.jar"
DEFAULT_INPUT_DIR = PROJECT_ROOT / "input"
DEFAULT_INPUT_FILE = DEFAULT_INPUT_DIR / "access.log"
OUTPUT_DIR = PROJECT_ROOT / "output"
OUTPUT_FILE = OUTPUT_DIR / "part-r-00000"
UPLOADS_DIR = DASHBOARD_DIR / "uploads"

# Ensure uploads directory exists
UPLOADS_DIR.mkdir(parents=True, exist_ok=True)

ALLOWED_EXTENSIONS = {'.log', '.txt'}

# Global execution lock to prevent concurrent MapReduce corruption
JOB_LOCK = threading.Lock()

# Thread-safe in-memory state tracking the last analysis run
LAST_RUN_STATE = {
    "status": "idle",       # "idle", "running", "success", "failed"
    "timestamp": None,
    "source_name": None,
    "filesize_bytes": 0,
    "raw_lines": 0,
    "duration_seconds": 0.0,
    "results": None,
    "error": None,
    "error_detail": None,
}


def get_hadoop_bin():
    """
    Locates the Hadoop executable reliably.
    Checks environment variables, standard PATH, and typical WSL installations.
    """
    # 1. Explicit environment variable
    env_bin = os.environ.get("HADOOP_BIN")
    if env_bin and Path(env_bin).is_file() and os.access(env_bin, os.X_OK):
        return Path(env_bin)

    hadoop_home = os.environ.get("HADOOP_HOME")
    if hadoop_home:
        candidate = Path(hadoop_home) / "bin" / "hadoop"
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate

    # 2. System PATH
    which_hadoop = shutil.which("hadoop")
    if which_hadoop:
        return Path(which_hadoop)

    # 3. Standard WSL / Linux locations
    candidate_paths = [
        Path.home() / "hadoop" / "bin" / "hadoop",
        Path("/home/suprabha/hadoop/bin/hadoop"),
        Path("/usr/local/hadoop/bin/hadoop"),
        Path("/opt/hadoop/bin/hadoop"),
        Path("/usr/bin/hadoop"),
    ]
    for p in candidate_paths:
        if p.is_file() and os.access(p, os.X_OK):
            return p

    return None


def get_java_bin():
    """Locates the Java executable."""
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidate = Path(java_home) / "bin" / "java"
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    which_java = shutil.which("java")
    if which_java:
        return Path(which_java)
    return None


def check_engine_health():
    """
    Performs an actual readiness check of Hadoop, Java, the MapReduce JAR,
    and the sample dataset.
    """
    hadoop_path = get_hadoop_bin()
    java_path = get_java_bin()
    jar_present = JAR_FILE.is_file()
    sample_present = DEFAULT_INPUT_FILE.is_file()

    hadoop_version = None
    if hadoop_path:
        try:
            res = subprocess.run(
                [str(hadoop_path), "version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            first_line = (res.stdout or "").strip().splitlines()
            if first_line:
                hadoop_version = first_line[0]
        except Exception as e:
            logger.warning(f"Failed to query hadoop version: {e}")

    is_ready = bool(hadoop_path and jar_present)
    
    if is_ready:
        status = "ready"
        message = "Hadoop engine and MapReduce JAR are ready."
    elif not hadoop_path:
        status = "unavailable"
        message = "Hadoop executable not found in PATH or standard directories."
    elif not jar_present:
        status = "degraded"
        message = f"MapReduce JAR missing at {JAR_FILE.name}."
    else:
        status = "degraded"
        message = "Engine components are incomplete."

    return {
        "status": status,
        "is_ready": is_ready,
        "hadoop_found": bool(hadoop_path),
        "hadoop_path": str(hadoop_path) if hadoop_path else None,
        "hadoop_version": hadoop_version,
        "java_found": bool(java_path),
        "java_path": str(java_path) if java_path else None,
        "jar_found": jar_present,
        "jar_path": str(JAR_FILE) if jar_present else None,
        "sample_log_found": sample_present,
        "message": message,
    }


def parse_hadoop_output(output_file: Path, raw_line_count: int = 0):
    """
    Parses the actual Hadoop MapReduce reducer output file (part-r-00000).
    Format per line: KEY<TAB>VALUE

    Expected key prefixes generated by WebLogAnalysis.java:
      - TOTAL_REQUESTS
      - URL:<url_path>
      - STATUS:<http_status_code>
      - IP:<client_ip>

    Returns a structured analytics dictionary.
    """
    if not output_file.is_file():
        return None

    total_requests = 0
    urls = {}
    statuses = {}
    ips = {}

    content = output_file.read_text(encoding="utf-8", errors="replace")
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        
        # Hadoop TextOutputFormat separates key and value with a tab
        parts = line.split("\t")
        if len(parts) != 2:
            # Fallback to whitespace split if tab is not present
            parts = line.rsplit(maxsplit=1)
            if len(parts) != 2:
                continue

        key, val_str = parts[0].strip(), parts[1].strip()
        try:
            count = int(val_str)
        except ValueError:
            continue

        if key == "TOTAL_REQUESTS":
            total_requests = count
        elif key.startswith("URL:"):
            urls[key[4:]] = count
        elif key.startswith("STATUS:"):
            statuses[key[7:]] = count
        elif key.startswith("IP:"):
            ips[key[3:]] = count

    # Categorize HTTP Status Codes into formal categories
    # 2xx: Success, 3xx: Redirection, 4xx: Client Error, 5xx: Server Error, etc.
    status_categories = {}
    total_errors = 0
    for code_str, count in statuses.items():
        try:
            code_num = int(code_str)
            cat_digit = code_num // 100
            if cat_digit == 2:
                cat_name = "2xx Success"
            elif cat_digit == 3:
                cat_name = "3xx Redirection"
            elif cat_digit == 4:
                cat_name = "4xx Client Error"
                total_errors += count
            elif cat_digit == 5:
                cat_name = "5xx Server Error"
                total_errors += count
            elif cat_digit == 1:
                cat_name = "1xx Informational"
            else:
                cat_name = f"{cat_digit}xx Other"
        except ValueError:
            cat_name = "Unknown Status"
        status_categories[cat_name] = status_categories.get(cat_name, 0) + count

    # Sort URLs descending by request count
    sorted_urls = dict(sorted(urls.items(), key=lambda item: item[1], reverse=True))
    
    # Sort Statuses numerically ascending
    def sort_status_key(code):
        try:
            return int(code)
        except ValueError:
            return 999
    sorted_statuses = dict(sorted(statuses.items(), key=lambda item: sort_status_key(item[0])))

    # Sort IPs descending by request count
    sorted_ips = dict(sorted(ips.items(), key=lambda item: item[1], reverse=True))

    # Calculate percentages and client stats
    ip_stats = []
    for ip, count in sorted_ips.items():
        percentage = round((count / total_requests * 100), 1) if total_requests > 0 else 0
        ip_stats.append({
            "ip": ip,
            "count": count,
            "percentage": percentage
        })

    # Transparent record match reporting
    unmatched_records = max(0, raw_line_count - total_requests) if raw_line_count > 0 else 0
    match_percentage = round((total_requests / raw_line_count * 100), 1) if raw_line_count > 0 else (100.0 if total_requests > 0 else 0.0)

    return {
        "total_requests": total_requests,
        "unique_urls": len(urls),
        "unique_ips": len(ips),
        "status_categories_count": len(status_categories),
        "status_categories": status_categories,
        "error_count": total_errors,
        "error_rate": round((total_errors / total_requests * 100), 1) if total_requests > 0 else 0.0,
        "urls": sorted_urls,
        "statuses": sorted_statuses,
        "ips": ip_stats,
        "raw_lines": raw_line_count,
        "unmatched_records": unmatched_records,
        "match_percentage": match_percentage,
    }


def count_file_lines(filepath: Path) -> int:
    """Counts non-empty lines in the input file safely."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            return sum(1 for line in f if line.strip())
    except Exception:
        return 0


# Load initial state from existing output if present on startup
if OUTPUT_FILE.is_file():
    try:
        raw_count = count_file_lines(DEFAULT_INPUT_FILE) if DEFAULT_INPUT_FILE.is_file() else 0
        initial_results = parse_hadoop_output(OUTPUT_FILE, raw_count)
        if initial_results:
            LAST_RUN_STATE.update({
                "status": "success",
                "timestamp": datetime.fromtimestamp(OUTPUT_FILE.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
                "source_name": DEFAULT_INPUT_FILE.name if DEFAULT_INPUT_FILE.is_file() else "Existing output",
                "filesize_bytes": DEFAULT_INPUT_FILE.stat().st_size if DEFAULT_INPUT_FILE.is_file() else 0,
                "raw_lines": raw_count,
                "duration_seconds": 0.0,
                "results": initial_results,
            })
    except Exception as e:
        logger.warning(f"Failed to load initial output file: {e}")


@app.route("/")
def index():
    """Serves the main LogScope dashboard page."""
    health = check_engine_health()
    return render_template(
        "index.html",
        health=health,
        last_run=LAST_RUN_STATE
    )


@app.route("/api/health", methods=["GET"])
def api_health():
    """Returns engine readiness and component check details."""
    health = check_engine_health()
    status_code = 200 if health["is_ready"] else 503
    return jsonify(health), status_code


@app.route("/api/results", methods=["GET"])
def api_results():
    """Returns the most recent valid analytics results, if any."""
    if LAST_RUN_STATE["status"] == "success" and LAST_RUN_STATE["results"]:
        return jsonify({
            "ok": True,
            "status": "success",
            "timestamp": LAST_RUN_STATE["timestamp"],
            "source_name": LAST_RUN_STATE["source_name"],
            "filesize_bytes": LAST_RUN_STATE["filesize_bytes"],
            "raw_lines": LAST_RUN_STATE["raw_lines"],
            "duration_seconds": LAST_RUN_STATE["duration_seconds"],
            "results": LAST_RUN_STATE["results"]
        })
    elif LAST_RUN_STATE["status"] == "running":
        return jsonify({
            "ok": True,
            "status": "running",
            "message": "A MapReduce job is currently processing."
        }), 202
    elif LAST_RUN_STATE["status"] == "failed":
        return jsonify({
            "ok": False,
            "status": "failed",
            "error": LAST_RUN_STATE.get("error", "Previous analysis failed."),
            "detail": LAST_RUN_STATE.get("error_detail"),
            "timestamp": LAST_RUN_STATE.get("timestamp")
        }), 500
    else:
        return jsonify({
            "ok": True,
            "status": "empty",
            "message": "No analysis results available yet. Run an analysis to view metrics."
        })


@app.route("/api/analyze", methods=["POST"])
def api_analyze():
    """
    Triggers the Hadoop MapReduce job.
    Accepts an uploaded .log/.txt file OR a request to process the default sample.
    """
    global LAST_RUN_STATE

    # Prevent concurrent execution
    acquired = JOB_LOCK.acquire(blocking=False)
    if not acquired:
        return jsonify({
            "ok": False,
            "error": "A Hadoop MapReduce job is already running. Please wait for it to finish."
        }), 409

    target_input = None
    source_name = None
    filesize_bytes = 0
    raw_lines = 0

    try:
        # 1. Health check prior to starting execution
        health = check_engine_health()
        if not health["is_ready"]:
            LAST_RUN_STATE.update({
                "status": "failed",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source_name": None,
                "duration_seconds": 0.0,
                "results": None,
                "error": "Hadoop engine is not ready.",
                "error_detail": health["message"]
            })
            return jsonify({
                "ok": False,
                "error": "Hadoop engine is not ready.",
                "detail": health["message"]
            }), 503

        # 2. Determine input source (upload vs default sample)
        uploaded_file = request.files.get("logfile")
        use_sample = request.form.get("use_sample") == "true" or (not uploaded_file and not request.files)

        if uploaded_file and uploaded_file.filename.strip():
            filename = secure_filename(uploaded_file.filename)
            ext = os.path.splitext(filename)[1].lower()

            if ext not in ALLOWED_EXTENSIONS:
                LAST_RUN_STATE.update({
                    "status": "failed",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "source_name": filename,
                    "results": None,
                    "error": f"Invalid file type '{ext}'.",
                    "error_detail": "Only .log and .txt files are supported."
                })
                return jsonify({
                    "ok": False,
                    "error": f"Invalid file type '{ext}'. Please upload a .log or .txt file."
                }), 400

            # Store in dedicated managed upload directory with timestamp prefix
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_name = f"{timestamp_str}_{filename}"
            target_input = UPLOADS_DIR / safe_name
            uploaded_file.save(target_input)

            filesize_bytes = target_input.stat().st_size
            if filesize_bytes == 0:
                if target_input.is_file():
                    target_input.unlink()
                LAST_RUN_STATE.update({
                    "status": "failed",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "source_name": filename,
                    "results": None,
                    "error": "Uploaded file is empty.",
                    "error_detail": "File size was 0 bytes."
                })
                return jsonify({
                    "ok": False,
                    "error": "The uploaded file is empty (0 bytes). Please upload a valid log file."
                }), 400

            source_name = filename
            raw_lines = count_file_lines(target_input)
            logger.info(f"Received upload '{filename}' ({filesize_bytes} bytes, {raw_lines} lines)")

        else:
            # Using default sample log (PRESERVE input/access.log)
            if not DEFAULT_INPUT_FILE.is_file():
                LAST_RUN_STATE.update({
                    "status": "failed",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "results": None,
                    "error": "Sample file not found.",
                    "error_detail": "input/access.log is missing."
                })
                return jsonify({
                    "ok": False,
                    "error": "Default sample log file (input/access.log) was not found."
                }), 404

            target_input = DEFAULT_INPUT_FILE
            source_name = "access.log (Sample)"
            filesize_bytes = DEFAULT_INPUT_FILE.stat().st_size
            raw_lines = count_file_lines(DEFAULT_INPUT_FILE)
            logger.info(f"Using default sample log ({filesize_bytes} bytes, {raw_lines} lines)")

        # 3. Clean only the generated Hadoop output directory
        # Hadoop requires that the destination output directory does not exist prior to job start
        if OUTPUT_DIR.exists():
            try:
                shutil.rmtree(OUTPUT_DIR)
                logger.info(f"Safely reset output directory: {OUTPUT_DIR}")
            except Exception as e:
                LAST_RUN_STATE.update({
                    "status": "failed",
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "results": None,
                    "error": "Failed to reset output directory.",
                    "error_detail": str(e)
                })
                return jsonify({
                    "ok": False,
                    "error": "Failed to clear previous Hadoop output directory.",
                    "detail": str(e)
                }), 500

        # Mark state as running
        LAST_RUN_STATE["status"] = "running"
        start_time = time.time()

        # 4. Invoke Hadoop MapReduce
        hadoop_bin = get_hadoop_bin()
        cmd = [
            str(hadoop_bin),
            "jar",
            str(JAR_FILE),
            "WebLogAnalysis",
            str(target_input),
            str(OUTPUT_DIR)
        ]

        logger.info(f"Executing: {' '.join(cmd)}")

        try:
            proc = subprocess.run(
                cmd,
                cwd=str(PROJECT_ROOT),
                capture_output=True,
                text=True,
                timeout=180
            )
            duration = round(time.time() - start_time, 2)
        except subprocess.TimeoutExpired:
            # Reset output and update state
            if OUTPUT_DIR.exists():
                shutil.rmtree(OUTPUT_DIR)
            LAST_RUN_STATE.update({
                "status": "failed",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source_name": source_name,
                "duration_seconds": 180.0,
                "results": None,
                "error": "Hadoop job timed out after 180 seconds.",
                "error_detail": "Execution took too long and was terminated."
            })
            return jsonify({
                "ok": False,
                "error": "Hadoop job timed out after 3 minutes.",
                "detail": "The MapReduce execution exceeded the 180-second threshold."
            }), 504

        # 5. Check exit code and output existence
        if proc.returncode != 0:
            error_output = (proc.stderr or proc.stdout or "").strip()
            # Clean server paths if needed, keep last 2000 chars of trace
            clean_error = error_output[-2000:] if error_output else "Unknown Hadoop error."
            logger.error(f"Hadoop job failed with code {proc.returncode}:\n{clean_error}")

            if OUTPUT_DIR.exists():
                shutil.rmtree(OUTPUT_DIR)

            LAST_RUN_STATE.update({
                "status": "failed",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source_name": source_name,
                "duration_seconds": duration,
                "results": None,
                "error": f"Hadoop MapReduce job exited with status code {proc.returncode}.",
                "error_detail": clean_error
            })

            return jsonify({
                "ok": False,
                "error": "Hadoop MapReduce execution failed.",
                "detail": clean_error,
                "duration_seconds": duration
            }), 500

        if not OUTPUT_FILE.is_file():
            LAST_RUN_STATE.update({
                "status": "failed",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "source_name": source_name,
                "duration_seconds": duration,
                "results": None,
                "error": "Hadoop job completed but output file (part-r-00000) was not created.",
                "error_detail": (proc.stderr or proc.stdout)[-1500:]
            })
            return jsonify({
                "ok": False,
                "error": "Output file part-r-00000 not found after job completion.",
                "detail": (proc.stderr or proc.stdout)[-1500:]
            }), 500

        # 6. Parse actual Hadoop output
        results = parse_hadoop_output(OUTPUT_FILE, raw_lines)
        if results is None:
            return jsonify({
                "ok": False,
                "error": "Failed to parse Hadoop output file.",
                "detail": "Output file exists but could not be parsed."
            }), 500

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        LAST_RUN_STATE.update({
            "status": "success",
            "timestamp": timestamp,
            "source_name": source_name,
            "filesize_bytes": filesize_bytes,
            "raw_lines": raw_lines,
            "duration_seconds": duration,
            "results": results,
            "error": None,
            "error_detail": None,
        })

        return jsonify({
            "ok": True,
            "status": "success",
            "timestamp": timestamp,
            "source_name": source_name,
            "filesize_bytes": filesize_bytes,
            "raw_lines": raw_lines,
            "duration_seconds": duration,
            "results": results,
            "notice": "No log records matched the expected Apache Common Log format." if results["total_requests"] == 0 else None
        })

    except Exception as e:
        logger.exception("Unexpected error during analysis")
        LAST_RUN_STATE.update({
            "status": "failed",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "source_name": source_name or "Unknown",
            "results": None,
            "error": "Internal server error during analysis.",
            "error_detail": str(e)
        })
        return jsonify({
            "ok": False,
            "error": "Internal server error occurred.",
            "detail": str(e)
        }), 500

    finally:
        JOB_LOCK.release()


@app.errorhandler(413)
def request_entity_too_large(error):
    return jsonify({
        "ok": False,
        "error": "File size exceeds limit (Max: 16 MB). Please upload a smaller log file."
    }), 413


@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "ok": False,
        "error": "Resource not found."
    }), 404


@app.errorhandler(500)
def server_error(error):
    return jsonify({
        "ok": False,
        "error": "Internal server error."
    }), 500


if __name__ == "__main__":
    # Standard development server on localhost
    app.run(host="127.0.0.1", port=5000, debug=False)
