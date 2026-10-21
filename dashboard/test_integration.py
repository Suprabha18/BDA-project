"""
LogScope Integration and Correctness Test Suite
Tests endpoints, MapReduce invocation, parsing, validation, and security.
"""

import os
import sys
import io
import time
import json
import unittest
from pathlib import Path

# Add dashboard to sys.path
DASHBOARD_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(DASHBOARD_DIR))

from app import (
    app,
    get_hadoop_bin,
    check_engine_health,
    parse_hadoop_output,
    DEFAULT_INPUT_FILE,
    JAR_FILE,
    OUTPUT_FILE,
    PROJECT_ROOT
)

class TestLogScopeIntegration(unittest.TestCase):

    def setUp(self):
        app.config['TESTING'] = True
        self.client = app.test_client()

    def test_01_verify_existing_files_intact(self):
        """Confirm original Hadoop JAR and Java source are intact."""
        java_src = PROJECT_ROOT / "src" / "WebLogAnalysis.java"
        self.assertTrue(java_src.is_file(), "Java source file src/WebLogAnalysis.java must exist")
        self.assertTrue(JAR_FILE.is_file(), "WebLogAnalysis.jar must exist")
        self.assertTrue(DEFAULT_INPUT_FILE.is_file(), "input/access.log must exist")
        
        # Verify content of original sample log
        sample_text = DEFAULT_INPUT_FILE.read_text()
        self.assertIn("192.168.1.10", sample_text)
        self.assertIn("/home", sample_text)
        print("✓ Test 1 Passed: Original Java source, JAR, and sample access.log are intact.")

    def test_02_health_endpoint(self):
        """Confirm /api/health accurately reports Hadoop readiness."""
        res = self.client.get("/api/health")
        data = res.get_json()
        self.assertEqual(res.status_code, 200)
        self.assertTrue(data.get("is_ready"))
        self.assertTrue(data.get("hadoop_found"))
        self.assertTrue(data.get("jar_found"))
        self.assertTrue(data.get("sample_log_found"))
        print(f"✓ Test 2 Passed: /api/health returns ready (Hadoop: {data.get('hadoop_version')})")

    def test_03_dashboard_html_serves(self):
        """Confirm GET / serves LogScope HTML with dark theme and static assets."""
        res = self.client.get("/")
        self.assertEqual(res.status_code, 200)
        html = res.get_data(as_text=True)
        self.assertIn("LogScope", html)
        self.assertIn("Web Server Log Analytics", html)
        self.assertIn("style.css", html)
        self.assertIn("chart.umd.min.js", html)
        print("✓ Test 3 Passed: GET / renders HTML dashboard successfully.")

    def test_04_run_mapreduce_on_sample(self):
        """Confirm dashboard invokes real Hadoop job on input/access.log and parses output."""
        # Read sample mtime before run to ensure sample file is preserved
        mtime_before = DEFAULT_INPUT_FILE.stat().st_mtime
        content_before = DEFAULT_INPUT_FILE.read_text()

        res = self.client.post("/api/analyze", data={"use_sample": "true"})
        self.assertEqual(res.status_code, 200, f"Analysis failed: {res.get_json()}")
        
        data = res.get_json()
        self.assertTrue(data.get("ok"))
        self.assertEqual(data.get("status"), "success")
        
        results = data.get("results")
        self.assertIsNotNone(results)
        
        # Real Hadoop calculations for input/access.log:
        # Total requests: 10
        self.assertEqual(results["total_requests"], 10)
        # Distinct URLs: 5 (/home, /login, /products, /about, /contact)
        self.assertEqual(results["unique_urls"], 5)
        self.assertEqual(results["urls"]["/home"], 3)
        self.assertEqual(results["urls"]["/products"], 3)
        self.assertEqual(results["urls"]["/login"], 2)
        self.assertEqual(results["urls"]["/about"], 1)
        self.assertEqual(results["urls"]["/contact"], 1)
        
        # Status codes: 200 -> 6, 404 -> 2, 500 -> 2
        self.assertEqual(results["statuses"]["200"], 6)
        self.assertEqual(results["statuses"]["404"], 2)
        self.assertEqual(results["statuses"]["500"], 2)
        
        # Distinct IPs: 5
        self.assertEqual(results["unique_ips"], 5)
        self.assertEqual(len(results["ips"]), 5)
        self.assertEqual(results["ips"][0]["ip"], "192.168.1.10")
        self.assertEqual(results["ips"][0]["count"], 4)
        
        # Categories: 3 (2xx, 4xx, 5xx)
        self.assertEqual(results["status_categories_count"], 3)
        # Errors: 404 (2) + 500 (2) = 4
        self.assertEqual(results["error_count"], 4)
        self.assertEqual(results["error_rate"], 40.0)

        # Verify sample file was untouched
        mtime_after = DEFAULT_INPUT_FILE.stat().st_mtime
        content_after = DEFAULT_INPUT_FILE.read_text()
        self.assertEqual(content_before, content_after, "Original sample log must never be modified")
        print(f"✓ Test 4 Passed: Real Hadoop run succeeded in {data.get('duration_seconds')}s with exact matching metrics.")

    def test_05_results_endpoint(self):
        """Confirm /api/results returns the last valid run."""
        res = self.client.get("/api/results")
        self.assertEqual(res.status_code, 200)
        data = res.get_json()
        self.assertEqual(data.get("status"), "success")
        self.assertEqual(data["results"]["total_requests"], 10)
        print("✓ Test 5 Passed: /api/results returns cached valid results.")

    def test_06_upload_valid_custom_log(self):
        """Confirm uploading a custom log triggers Hadoop on the uploaded file and preserves sample."""
        sample_before = DEFAULT_INPUT_FILE.read_text()

        # Create a small custom log with 4 entries
        custom_log = (
            '10.0.0.1 - - [21/Oct/2026:12:00:00 +0000] "GET /api/v1/data HTTP/1.1" 200 1024\n'
            '10.0.0.1 - - [21/Oct/2026:12:01:00 +0000] "GET /api/v1/data HTTP/1.1" 200 1024\n'
            '10.0.0.2 - - [21/Oct/2026:12:02:00 +0000] "GET /api/v1/auth HTTP/1.1" 404 128\n'
            '10.0.0.3 - - [21/Oct/2026:12:03:00 +0000] "GET /api/v1/status HTTP/1.1" 500 256\n'
        )

        file_obj = (io.BytesIO(custom_log.encode("utf-8")), "custom_test.log")
        res = self.client.post(
            "/api/analyze",
            data={"logfile": file_obj, "use_sample": "false"},
            content_type="multipart/form-data"
        )
        self.assertEqual(res.status_code, 200, f"Upload analysis failed: {res.get_json()}")
        data = res.get_json()
        self.assertTrue(data.get("ok"))
        results = data.get("results")
        
        self.assertEqual(results["total_requests"], 4)
        self.assertEqual(results["unique_urls"], 3)
        self.assertEqual(results["urls"]["/api/v1/data"], 2)
        self.assertEqual(results["statuses"]["200"], 2)
        self.assertEqual(results["statuses"]["404"], 1)
        self.assertEqual(results["statuses"]["500"], 1)
        self.assertEqual(results["unique_ips"], 3)
        self.assertEqual(results["error_count"], 2)

        # Confirm original sample was not overwritten
        self.assertEqual(DEFAULT_INPUT_FILE.read_text(), sample_before)
        print("✓ Test 6 Passed: Custom log upload processed by Hadoop; sample access.log preserved.")

    def test_07_upload_empty_file_rejected(self):
        """Confirm empty upload (0 bytes) is rejected with HTTP 400."""
        empty_file = (io.BytesIO(b""), "empty.log")
        res = self.client.post(
            "/api/analyze",
            data={"logfile": empty_file, "use_sample": "false"},
            content_type="multipart/form-data"
        )
        self.assertEqual(res.status_code, 400)
        data = res.get_json()
        self.assertFalse(data.get("ok"))
        self.assertIn("empty", data.get("error").lower())
        print("✓ Test 7 Passed: Empty upload rejected cleanly with 400 error.")

    def test_08_upload_invalid_extension_rejected(self):
        """Confirm unsupported extensions are rejected with HTTP 400."""
        bad_file = (io.BytesIO(b"malicious executable"), "payload.exe")
        res = self.client.post(
            "/api/analyze",
            data={"logfile": bad_file, "use_sample": "false"},
            content_type="multipart/form-data"
        )
        self.assertEqual(res.status_code, 400)
        data = res.get_json()
        self.assertFalse(data.get("ok"))
        self.assertIn("invalid file type", data.get("error").lower())
        print("✓ Test 8 Passed: Invalid file extension rejected cleanly with 400 error.")

    def test_09_stale_results_not_shown_on_failure(self):
        """Confirm that a failed run updates state to failed and clears previous results."""
        import app as app_module
        # Simulate a failed run by passing a non-existent JAR temporarily
        original_jar = app_module.JAR_FILE
        try:
            app_module.JAR_FILE = PROJECT_ROOT / "non_existent.jar"
            res = self.client.post("/api/analyze", data={"use_sample": "true"})
            self.assertIn(res.status_code, [500, 503])
            
            # Now verify /api/results does NOT return stale success
            res2 = self.client.get("/api/results")
            data2 = res2.get_json()
            self.assertNotEqual(data2.get("status"), "success")
            print("✓ Test 9 Passed: Failed run does not preserve or display stale results.")
        finally:
            app_module.JAR_FILE = original_jar
            # Restore sample results
            self.client.post("/api/analyze", data={"use_sample": "true"})

    def test_10_parser_handles_malformed_lines(self):
        """Confirm parser handles non-matching lines transparently."""
        # Test directly with parse_hadoop_output
        res = parse_hadoop_output(OUTPUT_FILE, raw_line_count=15)
        self.assertIsNotNone(res)
        self.assertEqual(res["total_requests"], 10)
        self.assertEqual(res["raw_lines"], 15)
        self.assertEqual(res["unmatched_records"], 5)
        print("✓ Test 10 Passed: Parser accurately accounts for raw vs matched log records.")

if __name__ == "__main__":
    unittest.main()
