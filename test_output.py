"""
Quick smoke-test for the Teams output format.
Runs entirely without Selenium / a real Databricks connection.
Posts one notification to the real Teams Flow URL so you can verify the card in Teams.
"""
import sys
import os
import time

# Allow importing Automation without triggering the selenium import error
# by monkey-patching the missing module if selenium is not installed.
try:
    import selenium  # noqa: F401
except ModuleNotFoundError:
    from unittest.mock import MagicMock
    sys.modules["selenium"] = MagicMock()
    sys.modules["selenium.webdriver"] = MagicMock()
    sys.modules["selenium.common"] = MagicMock()
    sys.modules["selenium.common.exceptions"] = MagicMock()
    sys.modules["selenium.webdriver.common"] = MagicMock()
    sys.modules["selenium.webdriver.common.by"] = MagicMock()
    sys.modules["selenium.webdriver.support"] = MagicMock()
    sys.modules["selenium.webdriver.support.expected_conditions"] = MagicMock()
    sys.modules["selenium.webdriver.support.ui"] = MagicMock()

# Import the real functions from Automation.py
sys.path.insert(0, os.path.dirname(__file__))
from Automation import (
    build_running_summary_message,
    build_adaptive_card,
    send_power_automate_notification,
    DEFAULT_TEAMS_FLOW_URL,
)

# ---------------------------------------------------------------------------
# Mock data — two divisions, one clean, one with lag
# ---------------------------------------------------------------------------
NOW_MS = int(time.time() * 1000)
ONE_HOUR_MS = 3_600_000

mock_jobs = [
    {
        "run_id": "111000111",
        "job_id": "263687637974905",
        "env": "prod",
        "app": "SDOVS",
        "dc": "WSPK",
        "div_nbr": "WSPK",
        "text": "ENV=prod APP=SDOVS DC=WSPK",
        "start_time": NOW_MS - (2 * ONE_HOUR_MS + 15 * 60_000),  # 2h 15m ago
    },
    {
        "run_id": "222000222",
        "job_id": "263687637974905",
        "env": "prod",
        "app": "SDOVS",
        "dc": "WMET",
        "div_nbr": "WMET",
        "text": "ENV=prod APP=SDOVS DC=WMET",
        "start_time": NOW_MS - (1 * ONE_HOUR_MS + 30 * 60_000),  # 1h 30m ago
    },
    {
        "run_id": "333000333",
        "job_id": "263687637974905",
        "env": "prod",
        "app": "SDOVS",
        "dc": "WDFW",
        "div_nbr": "WDFW",
        "text": "ENV=prod APP=SDOVS DC=WDFW",
        "start_time": NOW_MS - (45 * 60_000),  # 45m ago
    },
]

mock_validations = [
    {
        "run_id": "111000111",
        "raw_data_opened": True,
        "is_zero_lag": True,
        "note": "Zero lag",
        "metrics": {
            "avgOffsetsBehindLatest": "0.0",
            "estimatedTotalBytesBehindLatest": "0.0",
            "maxOffsetsBehindLatest": "0",
            "minOffsetsBehindLatest": "0",
        },
    },
    {
        "run_id": "222000222",
        "raw_data_opened": True,
        "is_zero_lag": False,
        "note": "Lag/offset detected",
        "metrics": {
            "avgOffsetsBehindLatest": "842.5",
            "estimatedTotalBytesBehindLatest": "1048576.0",
            "maxOffsetsBehindLatest": "1500",
            "minOffsetsBehindLatest": "185",
        },
    },
    {
        "run_id": "333000333",
        "raw_data_opened": True,
        "is_zero_lag": True,
        "note": "Zero lag",
        "metrics": {
            "avgOffsetsBehindLatest": "0.0",
            "estimatedTotalBytesBehindLatest": "0.0",
            "maxOffsetsBehindLatest": "0",
            "minOffsetsBehindLatest": "0",
        },
    },
]

PIPELINE = "SDDOV00000.nt_kafka_read_stream_prod"

# ---------------------------------------------------------------------------
# 1. Print the plain-text summary (what Teams chat message body will look like)
# ---------------------------------------------------------------------------
print("=" * 70)
print("PLAIN TEXT SUMMARY (teams message body)")
print("=" * 70)
msg = build_running_summary_message(mock_jobs, mock_validations)
print(msg.replace("\r\n", "\n"))

# ---------------------------------------------------------------------------
# 2. Print the Adaptive Card JSON (what Teams card will render)
# ---------------------------------------------------------------------------
import json
print("\n" + "=" * 70)
print("ADAPTIVE CARD JSON")
print("=" * 70)
card = build_adaptive_card(PIPELINE, mock_jobs, mock_validations)
print(json.dumps(card, indent=2))

# ---------------------------------------------------------------------------
# 3. Post to Teams (optional — requires TEAMS_FLOW_URL or default)
# ---------------------------------------------------------------------------
flow_url = os.getenv("TEAMS_FLOW_URL", DEFAULT_TEAMS_FLOW_URL).strip()
if flow_url:
    print("\n" + "=" * 70)
    print("POSTING TEST NOTIFICATION TO TEAMS...")
    print("=" * 70)
    try:
        send_power_automate_notification(
            flow_url,
            mock_jobs,
            mock_validations,
            pipeline_name=PIPELINE,
        )
        print("OK: Notification posted — check your Teams channel!")
    except Exception as exc:
        print(f"ERROR: {exc}")
else:
    print("\nINFO: No TEAMS_FLOW_URL set; skipping Teams post.")
