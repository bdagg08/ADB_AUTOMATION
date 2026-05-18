import json
import os
import re
import sys
import threading
import time
import urllib.request
from datetime import datetime, timezone

from selenium import webdriver
from selenium.common.exceptions import InvalidSessionIdException, TimeoutException, WebDriverException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

WORKSPACE_URL = "https://adb-4078265107808022.2.azuredatabricks.net"
ORG_ID = "4078265107808022"
JOB_ID = "263687637974905"
JOB_ID_2 = "1085815862998531"
JOB_URL = f"{WORKSPACE_URL}/jobs/{JOB_ID}?o={ORG_ID}"
JOB_URL_2 = f"{WORKSPACE_URL}/jobs/{JOB_ID_2}?o={ORG_ID}"
JOB_IDS = [(JOB_ID, JOB_URL), (JOB_ID_2, JOB_URL_2)]
JOB_PIPELINE = {
    JOB_ID: "SDDOV00000.nt_kafka_read_stream_prod",
    JOB_ID_2: "SDDOVS00152.nt_Kafka_ReadStream_Shipment_OV",
}
JOBS_URL = f"{WORKSPACE_URL}/jobs?o={ORG_ID}"
DEFAULT_ENV = os.getenv("DEFAULT_ENV", "prod")
DEFAULT_APP = os.getenv("DEFAULT_APP", "SDOVS")
ENABLE_OFFSET_VALIDATION_DEFAULT = os.getenv("ENABLE_OFFSET_VALIDATION", "1").strip().lower() in {"1", "true", "yes"}
DEFAULT_TEAMS_FLOW_URL = (
    "https://defaultb7f604a000a94188924842f3a5aac2.e9.environment.api.powerplatform.com:443/"
    "powerautomate/automations/direct/workflows/cfc07469ec104fc1917e42c3bda51e78/triggers/manual/"
    "paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=pwf5YlmBjWvpVy3ArQvd-R6UGhCwW2SbV1SPMUg_-6M"
)


def get_running_jobs_via_api(pat_token: str, job_id: str = JOB_ID) -> list[dict]:
    """Use Databricks REST API with PAT token to fetch running jobs. Most reliable method."""
    url = f"{WORKSPACE_URL}/api/2.1/jobs/runs/list?job_id={job_id}&active_only=true&limit=50"
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {pat_token}",
            "Content-Type": "application/json",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))

    runs = data.get("runs", [])
    processed = []
    for run in runs:
        run_id = str(run.get("run_id", "N/A"))
        run_job_id = str(run.get("job_id", job_id))
        # Extract ENV, APP, DIV_NBR/DC from run_name or notebook_params or job_params.
        params = run.get("overriding_parameters", {}) or {}
        notebook_params = params.get("notebook_params", {}) or {}
        jar_params = params.get("jar_params", []) or []
        run_name = run.get("run_name", "") or ""
        param_text = " ".join([
            run_name,
            " ".join(f"{k}={v}" for k, v in notebook_params.items()),
            " ".join(jar_params),
        ])
        env_value, app_value, dc_value = parse_run_parameters(param_text)
        processed.append({
            "run_id": run_id,
            "job_id": run_job_id,
            "env": env_value,
            "app": app_value,
            "dc": dc_value,
            "div_nbr": dc_value,
            "text": param_text,
            "start_time": run.get("start_time"),
        })
    return processed


def parse_run_parameters(text: str) -> tuple[str, str, str]:
    env_value = "N/A"
    app_value = "N/A"
    dc_value = "N/A"

    env_match = re.search(r"(?:ENV|END)\s*[:=]\s*([A-Za-z0-9_-]+)", text, flags=re.IGNORECASE)
    app_match = re.search(r"APP\s*[:=]\s*([A-Za-z0-9_-]+)", text, flags=re.IGNORECASE)
    dc_match = re.search(r"(?:DC|DIV_NBR)\s*[:=]\s*([A-Za-z0-9_-]+)", text, flags=re.IGNORECASE)

    if env_match:
        env_value = env_match.group(1).strip().lower()
    if app_match:
        app_value = app_match.group(1).strip()
    if dc_match:
        dc_value = dc_match.group(1).strip()

    # Fallback for formats like "APP: SDOVS, WSPK" when explicit DC field is missing.
    if dc_value == "N/A":
        compact_text = re.sub(r"\s+", " ", text)
        app_dc_match = re.search(r"APP\s*[:=]\s*([A-Za-z0-9_-]+)\s*,\s*([A-Za-z0-9_-]+)", compact_text, flags=re.IGNORECASE)
        if app_dc_match:
            app_value = app_dc_match.group(1).strip()
            dc_value = app_dc_match.group(2).strip()

    # Common DC code fallback pattern like WSPK, WMET, etc.
    if dc_value == "N/A":
        dc_guess = re.search(r"\bW[A-Z]{3}\b", text)
        if dc_guess:
            dc_value = dc_guess.group(0)

    # Default values for this pipeline when row text omits parameters.
    if env_value == "N/A":
        env_value = DEFAULT_ENV
    if app_value == "N/A":
        app_value = DEFAULT_APP

    return env_value, app_value, dc_value


def _format_duration(start_time_ms: int | None) -> str:
    """Format elapsed time since start_time_ms (epoch milliseconds) as a human-readable string."""
    if start_time_ms is None:
        return "N/A"
    elapsed_ms = int(time.time() * 1000) - start_time_ms
    if elapsed_ms < 0:
        return "N/A"
    total_secs = elapsed_ms // 1000
    hours = total_secs // 3600
    minutes = (total_secs % 3600) // 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def build_running_summary_message(running_jobs: list[dict], validation_results: list[dict] | None = None) -> str:
    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    count = len(running_jobs)
    validation_by_run_id = {
        str(item.get("run_id", "")): item for item in (validation_results or [])
    }

    lines = [
        "Databricks - RUNNING status summary : SDDOV00000.nt_kafka_read_stream_prod",
        "",
        "Continuously running",
        "",
        f"  Count: {count}   Collected at: {collected_at}",
        "",
    ]

    if running_jobs:
        col_div    = "Division"
        col_lag    = "Offset Lag (Max)"
        col_dur    = "Duration of Run"
        col_status = "Status"

        # rows: (division, max_lag, duration, status, job_id)
        rows: list[tuple[str, str, str, str, str]] = []
        for item in running_jobs:
            validation = validation_by_run_id.get(str(item["run_id"]), {})
            metrics    = validation.get("metrics", {})
            division   = item.get("div_nbr", item.get("dc", "N/A"))
            max_lag    = metrics.get("maxOffsetsBehindLatest", "N/A")
            duration   = _format_duration(item.get("start_time"))
            job_id     = item.get("job_id", "N/A")
            if metrics:
                status = "Continuously running" if validation.get("is_zero_lag", False) else "Lag Detected"
            else:
                status = "Continuously running"
            rows.append((division, max_lag, duration, status, job_id))

        # Column width must also account for the indented job_id sub-row under Division
        w_div    = max(len(col_div),    max(len(r[0]) for r in rows), max(2 + len(r[4]) for r in rows))
        w_lag    = max(len(col_lag),    max(len(r[1]) for r in rows))
        w_dur    = max(len(col_dur),    max(len(r[2]) for r in rows))
        w_status = max(len(col_status), max(len(r[3]) for r in rows))

        def _row(a: str, b: str, c: str, d: str) -> str:
            return f"  {a.ljust(w_div)}  | {b.ljust(w_lag)}  | {c.ljust(w_dur)}  | {d.ljust(w_status)}"

        sep = f"  {'-' * (w_div + 2)}+{'-' * (w_lag + 2)}+{'-' * (w_dur + 2)}+{'-' * (w_status + 2)}"
        lines.append(_row(col_div, col_lag, col_dur, col_status))
        lines.append(sep)
        for r in rows:
            lines.append(_row(r[0], r[1], r[2], r[3]))
            lines.append(f"     Job ID: {r[4]}")
    else:
        lines.append("  No active running jobs found.")

    # Teams/Power Automate often render line breaks more reliably with CRLF.
    return "\r\n".join(lines)


def _collect_running_jobs_from_dom(driver: webdriver.Edge) -> list[dict]:
    script = r"""
const rows = [];

// Strategy 1: look for /job-runs/ links (standard Databricks run detail links).
const runAnchors = Array.from(document.querySelectorAll('a[href*="/job-runs/"]'));
for (const anchor of runAnchors) {
  const href = anchor.getAttribute('href') || '';
  const matchRun = href.match(/\/job-runs\/(\d+)/);
  if (!matchRun) continue;
  const container = anchor.closest('tr,[role="row"],[class*="row"],[class*="Row"]') || anchor.parentElement;
  const text = (container ? container.innerText : anchor.innerText || '').replace(/\s+/g, ' ').trim();
  const isTerminal = /failed|succeeded|canceled|skipped|terminated/i.test(text);
  if (isTerminal) continue;
  const jobLink = container ? container.querySelector('a[href*="/jobs/"]') : null;
  const jobHref = jobLink ? (jobLink.getAttribute('href') || '') : '';
  const matchJob = jobHref.match(/\/jobs\/(\d+)/);
  rows.push({ run_id: matchRun[1], job_id: matchJob ? matchJob[1] : 'N/A', text });
}

// Strategy 2: look for /run/ links (alternate Databricks URL pattern).
if (rows.length === 0) {
  const runAnchors2 = Array.from(document.querySelectorAll('a[href*="/run/"]'));
  for (const anchor of runAnchors2) {
    const href = anchor.getAttribute('href') || '';
    const matchRun = href.match(/\/run\/(\d+)/);
    if (!matchRun) continue;
    const container = anchor.closest('tr,[role="row"],[class*="row"],[class*="Row"]') || anchor.parentElement;
    const text = (container ? container.innerText : anchor.innerText || '').replace(/\s+/g, ' ').trim();
    const isTerminal = /failed|succeeded|canceled|skipped|terminated/i.test(text);
    if (isTerminal) continue;
    rows.push({ run_id: matchRun[1], job_id: 'N/A', text });
  }
}

// Strategy 3: scan all table rows for numeric IDs that look like run IDs (15+ digits).
if (rows.length === 0) {
  const trs = Array.from(document.querySelectorAll('tr,[role="row"]'));
  for (const tr of trs) {
    const text = tr.innerText || '';
    const isTerminal = /failed|succeeded|canceled|skipped|terminated/i.test(text);
    if (isTerminal) continue;
    const isRunning = /running|pending|queued|starting/i.test(text);
    if (!isRunning) continue;
    const idMatch = text.match(/\b(\d{10,})\b/);
    if (!idMatch) continue;
    rows.push({ run_id: idMatch[1], job_id: 'N/A', text: text.replace(/\s+/g, ' ').trim() });
  }
}
const dedup = new Map();
for (const row of rows) {
  if (!dedup.has(row.run_id)) dedup.set(row.run_id, row);
}
return Array.from(dedup.values());
"""
    return driver.execute_script(script)


def _try_click_runs_tab(driver: webdriver.Edge) -> None:
    """Try to click the Runs tab on the job page to reveal run rows."""
    tab_selectors = [
        'button[role="tab"]',
        '[role="tab"]',
    ]
    for selector in tab_selectors:
        try:
            tabs = driver.find_elements(By.CSS_SELECTOR, selector)
            for tab in tabs:
                role = (tab.get_attribute("role") or "").strip().lower()
                label = (tab.text or tab.get_attribute("aria-label") or "").strip().lower()
                # Safety guard: only click tab controls, never action buttons like "Run now".
                if role != "tab":
                    continue
                if "run now" in label or "rerun" in label or "trigger" in label:
                    continue
                if label in {"runs", "run history"} or label.startswith("runs"):
                    tab.click()
                    time.sleep(2)
                    return
        except Exception:
            continue


def get_running_jobs(driver: webdriver.Edge, limit: int | None = None, wait_seconds: int = 90, job_url: str = JOB_URL, job_id: str = JOB_ID) -> list[dict]:
    driver.get(job_url)

    # Wait for page to load then try to activate the Runs tab.
    try:
        WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "main")))
        time.sleep(4)
    except TimeoutException:
        pass

    _try_click_runs_tab(driver)
    time.sleep(3)

    deadline = time.time() + wait_seconds
    latest = []

    while time.time() < deadline:
        latest = _collect_running_jobs_from_dom(driver)
        if latest:
            break
        time.sleep(5)
        try:
            driver.refresh()
            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(4)
            _try_click_runs_tab(driver)
            time.sleep(3)
        except Exception:
            pass

    selected = latest if limit is None else latest[:limit]
    processed = []
    for run in selected:
        run_id = run.get("run_id", "N/A")
        # job_id is always the job we navigated to.
        run_job_id = run.get("job_id", "N/A")
        if run_job_id == "N/A":
            run_job_id = job_id
        text_source = run.get("text", "")
        env_value, app_value, dc_value = parse_run_parameters(text_source)

        processed.append(
            {
                "run_id": run_id,
                "job_id": run_job_id,
                "env": env_value,
                "app": app_value,
                "dc": dc_value,
                "div_nbr": dc_value,
                "text": text_source,
                "start_time": None,
            }
        )

    return processed


def _open_raw_data_view(driver: webdriver.Edge) -> bool:
    """Try to open Raw Data in job run details using common Databricks UI patterns."""
    # Scroll down first so the stream status section is rendered and visible.
    try:
        print("   DEBUG: Scrolling to bottom of page to find Raw Data section...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        print("   DEBUG: Scrolling back up 500px to center Raw Data controls...")
        driver.execute_script("window.scrollBy(0, -500);")
        time.sleep(0.5)
    except Exception as e:
        print(f"   DEBUG: Scroll attempt failed: {e}")

    # Some run pages require expanding the stream status panel before Raw Data tabs appear.
    expand_locators = [
        (By.CSS_SELECTOR, "div.stream-status-header.pointer"),
        (By.CSS_SELECTOR, "div.header-block.stream-details-toggle"),
        (By.XPATH, "//div[contains(@class, 'stream-status-header') and contains(@class, 'pointer')]"),
    ]
    for by, locator in expand_locators:
        try:
            expand_elem = WebDriverWait(driver, 2).until(EC.presence_of_element_located((by, locator)))
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", expand_elem)
            try:
                expand_elem.click()
            except Exception:
                driver.execute_script("arguments[0].click();", expand_elem)
            time.sleep(1)
            break
        except Exception:
            continue

    # Only target explicit Raw Data controls. Avoid broad tab/menu clicks that can open Event Log.
    raw_locators = [
        (By.CSS_SELECTOR, "button[role='tab'][aria-controls*='content-raw']"),
        (By.CSS_SELECTOR, "button[role='tab'][id*='trigger-raw']"),
        (By.XPATH, "//*[self::button or self::a or @role='tab'][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'raw data')]"),
        (By.XPATH, "//*[@role='menuitem'][contains(translate(normalize-space(.), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'raw data')]"),
    ]
    for by, locator in raw_locators:
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            elem = WebDriverWait(driver, 4).until(EC.presence_of_element_located((by, locator)))
            text_value = (elem.text or "").strip().lower()
            aria_controls = (elem.get_attribute("aria-controls") or "").lower()
            elem_id = (elem.get_attribute("id") or "").lower()

            # Keep this strict when matching broad role='tab' selectors.
            is_raw_target = (
                "raw data" in text_value
                or "content-raw" in aria_controls
                or "trigger-raw" in elem_id
            )
            if not is_raw_target:
                continue

            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", elem)
            try:
                elem.click()
            except Exception:
                # Fallback click for custom-styled tab components.
                driver.execute_script("arguments[0].click();", elem)
            time.sleep(1.5)
            return True
        except Exception:
            continue

    return False


def _extract_offset_metrics(page_text: str) -> dict[str, str]:
    metrics: dict[str, str] = {}
    patterns = {
        "avgOffsetsBehindLatest": r'"avgOffsetsBehindLatest"\s*:\s*"?([0-9.]+)"?',
        "estimatedTotalBytesBehindLatest": r'"estimatedTotalBytesBehindLatest"\s*:\s*"?([0-9.]+)"?',
        "maxOffsetsBehindLatest": r'"maxOffsetsBehindLatest"\s*:\s*"?([0-9]+)"?',
        "minOffsetsBehindLatest": r'"minOffsetsBehindLatest"\s*:\s*"?([0-9]+)"?',
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, page_text, flags=re.IGNORECASE)
        if match:
            metrics[key] = match.group(1)
    return metrics


def validate_job_offsets_from_raw_data(driver: webdriver.Edge, running_jobs: list[dict], max_jobs: int | None = 10) -> list[dict]:
    """Open run detail pages in separate tabs and validate offset lag metrics. If max_jobs=None, check all runs."""
    results: list[dict] = []

    run_entries: list[dict] = []
    for item in running_jobs[:max_jobs]:
        run_id = item.get("run_id", "N/A")
        job_id = item.get("job_id", JOB_ID)
        run_url = f"{WORKSPACE_URL}/jobs/{job_id}/runs/{run_id}?o={ORG_ID}"
        fallback_run_url = f"{WORKSPACE_URL}/job-runs/{run_id}?o={ORG_ID}"
        run_entries.append(
            {
                "run_id": run_id,
                "url": run_url,
                "fallback_url": fallback_run_url,
            }
        )

    # Validate runs in the current tab to avoid new-window session instability.
    for entry in run_entries:
        result = {
            "run_id": entry["run_id"],
            "url": entry["url"],
            "raw_data_opened": False,
            "metrics": {},
            "is_zero_lag": False,
            "note": "",
        }
        try:
            driver.get(entry["url"])
            WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(1.5)

            # If this route doesn't render expected run detail UI, try legacy fallback.
            current_url = driver.current_url or ""
            if "/jobs/" not in current_url and "/runs/" not in current_url:
                driver.get(entry["fallback_url"])
                WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(1.5)

            # Requested manual step: go to the bottom before opening Raw Data.
            print(f"   DEBUG: Scrolling run {entry['run_id']} page to bottom...")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)

            result["raw_data_opened"] = _open_raw_data_view(driver)
            print(f"   DEBUG: Raw Data tab opened = {result['raw_data_opened']} for RUN_ID={entry['run_id']}")

            body_text = driver.find_element(By.TAG_NAME, "body").text
            combined_text = f"{body_text}\n{driver.page_source}"

            frames = driver.find_elements(By.TAG_NAME, "iframe")
            if frames:
                print(f"   DEBUG: Found {len(frames)} iframe(s); scanning for Raw Data inside...")
            for frame in frames:
                try:
                    driver.switch_to.frame(frame)
                    if not result["raw_data_opened"]:
                        result["raw_data_opened"] = _open_raw_data_view(driver)
                    frame_text = driver.find_element(By.TAG_NAME, "body").text
                    combined_text += f"\n{frame_text}\n{driver.page_source}"
                except Exception:
                    pass
                finally:
                    try:
                        driver.switch_to.default_content()
                    except Exception:
                        pass

            metrics = _extract_offset_metrics(combined_text)
            result["metrics"] = metrics
            print(f"   DEBUG: Extracted metrics for RUN_ID={entry['run_id']}: {metrics}")

            if metrics:
                is_zero_lag = (
                    metrics.get("avgOffsetsBehindLatest") == "0.0"
                    and metrics.get("estimatedTotalBytesBehindLatest") == "0.0"
                    and metrics.get("maxOffsetsBehindLatest") == "0"
                    and metrics.get("minOffsetsBehindLatest") == "0"
                )
                result["is_zero_lag"] = is_zero_lag
                result["note"] = "Zero lag" if is_zero_lag else "Lag/offset detected"
                print(f"   DEBUG: Offset status for RUN_ID={entry['run_id']}: is_zero_lag={is_zero_lag}")
            else:
                result["note"] = "Offset metrics not found in page/raw data"
                print(f"   DEBUG: No offset metrics extracted for RUN_ID={entry['run_id']}")
        except InvalidSessionIdException as exc:
            result["note"] = f"Validation aborted: browser session ended unexpectedly ({exc})"
            results.append(result)
            break
        except WebDriverException as exc:
            result["note"] = f"WebDriver error during validation: {exc}"
        except Exception as exc:
            result["note"] = f"Validation error: {exc}"

        results.append(result)

    return results


def build_adaptive_card(pipeline_name: str, running_jobs: list[dict], validation_results: list[dict] | None = None) -> dict:
    """Build a Teams Adaptive Card: bold title + coloured status line. If offset detected, include run details."""
    validation_by_run_id = {str(v.get("run_id", "")): v for v in (validation_results or [])}

    has_offset = any(
        not v.get("is_zero_lag", True)
        for v in (validation_results or [])
        if v.get("metrics")
    )

    job_count = len(running_jobs)
    if has_offset:
        status_text = f"{pipeline_name} {job_count} jobs are running with offset detected"
    else:
        status_text = f"{pipeline_name} {job_count} jobs are running successfuly without offset"
    status_color = "Good"  # force green in Adaptive Cards

    body: list[dict] = [
        {
            "type": "TextBlock",
            "text": "ADB MONITORING NOTIFICATION",
            "weight": "Bolder",
            "size": "Small",
            "wrap": True,
        },
        {
            "type": "TextBlock",
            "text": status_text,
            "color": status_color,
            "size": "Medium",
            "wrap": True,
        },
    ]

    # If offset detected, append a detail block for each affected run.
    if has_offset:
        for item in running_jobs:
            val = validation_by_run_id.get(str(item["run_id"]), {})
            metrics = val.get("metrics", {})
            if val.get("is_zero_lag", True):
                continue  # skip runs that are clean
            body.append({"type": "TextBlock", "text": "---", "separator": True, "spacing": "Small"})
            body.append({
                "type": "FactSet",
                "facts": [
                    {"title": "Division", "value": item.get("div_nbr", item.get("dc", "N/A"))},
                    {"title": "Job ID", "value": item.get("job_id", "N/A")},
                    {"title": "Offset Lag (Max)", "value": metrics.get("maxOffsetsBehindLatest", "N/A")},
                    {"title": "Duration of Run", "value": _format_duration(item.get("start_time"))},
                    {"title": "Status", "value": "Lag Detected"},
                ],
            })

    return {
        "type": "AdaptiveCard",
        "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
        "version": "1.4",
        "body": body,
    }


def _post_json(endpoint_url: str, payload: dict) -> None:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        endpoint_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        status_code = response.getcode()
        if status_code < 200 or status_code >= 300:
            raise RuntimeError(f"Notification endpoint failed with status code {status_code}")


def send_power_automate_notification(flow_url: str, running_jobs: list[dict], validation_results: list[dict] | None = None, pipeline_name: str = "SDDOV00000.nt_kafka_read_stream_prod") -> None:
    summary_subject = f"ADB MONITORING NOTIFICATION - {pipeline_name}"
    adaptive_card = build_adaptive_card(pipeline_name, running_jobs, validation_results)
    has_offset = any(
        not v.get("is_zero_lag", True)
        for v in (validation_results or [])
        if v.get("metrics")
    )
    job_count = len(running_jobs)
    if has_offset:
        status_line = f"{pipeline_name} {job_count} jobs are running with offset detected"
    else:
        status_line = f"{pipeline_name} {job_count} jobs are running successfuly without offset"
    status_color_html = "#1B8E3E"

    detail_lines_plain: list[str] = []
    if has_offset:
        validation_map = {str(v.get("run_id", "")): v for v in (validation_results or [])}
        for item in running_jobs:
            val = validation_map.get(str(item["run_id"]), {})
            if val.get("is_zero_lag", True):
                continue
            metrics = val.get("metrics", {})
            detail_lines_plain.append(
                f"Division={item.get('div_nbr', item.get('dc', 'N/A'))} | "
                f"Offset Lag (Max)={metrics.get('maxOffsetsBehindLatest', 'N/A')} | "
                f"Duration of Run={_format_duration(item.get('start_time'))} | "
                f"Status=Lag Detected"
            )

    collected_at_display = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    status_color_css = "#D83B01" if has_offset else "#107C10"

    th  = "padding:8px 14px;border:1px solid #D0D0D0;text-align:left;white-space:nowrap;"
    thr = "background:#1F4E79;color:#fff;"
    td  = "padding:8px 14px;border:1px solid #D0D0D0;vertical-align:top;"

    validation_map_html = {str(v.get("run_id", "")): v for v in (validation_results or [])}

    table_rows_html = ""
    for i, item in enumerate(running_jobs):
        val     = validation_map_html.get(str(item["run_id"]), {})
        metrics = val.get("metrics", {})
        div     = item.get("div_nbr", item.get("dc", "N/A"))
        jid     = item.get("job_id", "N/A")
        lag     = metrics.get("maxOffsetsBehindLatest", "N/A")
        dur     = _format_duration(item.get("start_time"))
        if metrics:
            st      = "Continuously running" if val.get("is_zero_lag", False) else "Lag Detected"
            st_col  = "#107C10" if val.get("is_zero_lag", False) else "#D83B01"
        else:
            st, st_col = "Continuously running", "#107C10"
        row_bg = "#F8F8F8" if i % 2 == 0 else "#FFFFFF"
        table_rows_html += (
            f"<tr style='background:{row_bg};'>"
            f"<td style='{td}'><strong>{div}</strong><br/>"
            f"<span style='color:#666;font-size:11px;'>{jid}</span></td>"
            f"<td style='{td}'>{lag}</td>"
            f"<td style='{td}'>{dur}</td>"
            f"<td style='{td}font-weight:600;color:{st_col};'>{st}</td>"
            f"</tr>"
        )

    teams_html_body = (
        "<div style='font-family:Segoe UI,Arial,sans-serif;font-size:14px;'>"
        "<div style='font-size:16px;font-weight:700;margin-bottom:6px;'>ADB MONITORING NOTIFICATION</div>"
        f"<div style='margin-bottom:4px;color:{status_color_css};font-weight:600;'>{status_line}</div>"
        f"<div style='margin-bottom:12px;color:#555;font-size:12px;'>Collected at: {collected_at_display} &nbsp;|&nbsp; Count: {job_count}</div>"
        "<table style='border-collapse:collapse;width:100%;'>"
        f"<thead><tr style='{thr}'>"
        f"<th style='{th}{thr}'>Division / Job ID</th>"
        f"<th style='{th}{thr}'>Offset Lag (Max)</th>"
        f"<th style='{th}{thr}'>Duration of Run</th>"
        f"<th style='{th}{thr}'>Status</th>"
        "</tr></thead>"
        f"<tbody>{table_rows_html}</tbody>"
        "</table></div>"
    )

    collected_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    trace_id = datetime.now(timezone.utc).strftime("TRACE-%Y%m%d-%H%M%S")
    validation_by_run_id = {
        str(item.get("run_id", "")): item for item in (validation_results or [])
    }
    jobs_payload = [
        {
            "run_id": item["run_id"],
            "job_id": item["job_id"],
            "env": item["env"],
            "app": item["app"],
            "dc": item["dc"],
            "div_nbr": item.get("div_nbr", item["dc"]),
            "raw_text": item["text"],
            "raw_data_opened": validation_by_run_id.get(str(item["run_id"]), {}).get("raw_data_opened", False),
            "zero_lag": validation_by_run_id.get(str(item["run_id"]), {}).get("is_zero_lag", False),
            "validation_status": validation_by_run_id.get(str(item["run_id"]), {}).get("note", "N/A"),
            "avgOffsetsBehindLatest": validation_by_run_id.get(str(item["run_id"]), {}).get("metrics", {}).get("avgOffsetsBehindLatest", "N/A"),
            "estimatedTotalBytesBehindLatest": validation_by_run_id.get(str(item["run_id"]), {}).get("metrics", {}).get("estimatedTotalBytesBehindLatest", "N/A"),
            "maxOffsetsBehindLatest": validation_by_run_id.get(str(item["run_id"]), {}).get("metrics", {}).get("maxOffsetsBehindLatest", "N/A"),
            "minOffsetsBehindLatest": validation_by_run_id.get(str(item["run_id"]), {}).get("metrics", {}).get("minOffsetsBehindLatest", "N/A"),
            "display_block": "",
        }
        for item in running_jobs
    ]

    offset_details = []
    if has_offset:
        offset_details = detail_lines_plain

    payload = {
        "source": "databricks-selenium-automation",
        "workspace_url": WORKSPACE_URL,
        "org_id": ORG_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "active_run_count": len(running_jobs),
        "count": len(running_jobs),
        "collected_at": collected_at,
        "title": summary_subject,
        "summary_subject": summary_subject,
        "pipeline_name": pipeline_name,
        "trace_id": trace_id,
        "summary_message": teams_html_body,
        "message": teams_html_body,
        "text": teams_html_body,
        "display_blocks_joined": teams_html_body,
        "teams_message_body": teams_html_body,
        "teams_html_body": teams_html_body,
        "status_line": status_line,
        "status_color": status_color_html,
        "offset_details": offset_details,
        "adaptive_card": adaptive_card,
        "adaptive_card_json": json.dumps(adaptive_card),
        "validation_results": validation_results or [],
        "jobs": jobs_payload,
    }
    _post_json(flow_url, payload)


def send_legacy_webhook_notification(
    webhook_url: str,
    running_jobs: list[dict],
    validation_results: list[dict] | None = None,
    pipeline_name: str = "SDDOV00000.nt_kafka_read_stream_prod",
) -> None:
    has_offset = any(
        not v.get("is_zero_lag", True)
        for v in (validation_results or [])
        if v.get("metrics")
    )
    job_count = len(running_jobs)
    if has_offset:
        status_line = f"{pipeline_name} {job_count} jobs are running with offset detected"
    else:
        status_line = f"{pipeline_name} {job_count} jobs are running successfuly without offset"
    theme_color = "008000"

    text_lines = [
        "ADB MONITORING NOTIFICATION",
        "",
        status_line,
    ]

    if has_offset:
        validation_map = {str(v.get("run_id", "")): v for v in (validation_results or [])}
        text_lines.append("")
        for item in running_jobs:
            val = validation_map.get(str(item["run_id"]), {})
            if val.get("is_zero_lag", True):
                continue
            metrics = val.get("metrics", {})
            text_lines.append(
                f"Division={item.get('div_nbr', item.get('dc', 'N/A'))} | "
                f"Offset Lag (Max)={metrics.get('maxOffsetsBehindLatest', 'N/A')} | "
                f"Duration of Run={_format_duration(item.get('start_time'))} | "
                f"Status=Lag Detected"
            )

    payload = {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "summary": f"ADB MONITORING NOTIFICATION - {pipeline_name}",
        "themeColor": theme_color,
        "title": "ADB MONITORING NOTIFICATION",
        "text": "\r\n".join(text_lines),
    }
    _post_json(webhook_url, payload)


def _type_if_present(driver: webdriver.Edge, locator: tuple[str, str], value: str, timeout: int = 10) -> bool:
    try:
        field = WebDriverWait(driver, timeout).until(EC.presence_of_element_located(locator))
        field.clear()
        field.send_keys(value)
        return True
    except TimeoutException:
        return False


def _click_if_present(driver: webdriver.Edge, locator: tuple[str, str], timeout: int = 10) -> bool:
    try:
        button = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable(locator))
        button.click()
        return True
    except TimeoutException:
        return False


def attempt_entra_form_login(driver: webdriver.Edge) -> None:
    username = os.getenv("ENTRA_USERNAME") or os.getenv("DBX_USERNAME")
    password = os.getenv("ENTRA_PASSWORD") or os.getenv("DBX_PASSWORD")

    if not username or not password:
        print("INFO: ENTRA_USERNAME/ENTRA_PASSWORD not set; skipping credential auto-fill")
        return

    entered_user = _type_if_present(driver, (By.ID, "i0116"), username, timeout=12)
    if entered_user:
        _click_if_present(driver, (By.ID, "idSIButton9"), timeout=8)
        print("OK: Entered Entra username")

    entered_password = _type_if_present(driver, (By.ID, "i0118"), password, timeout=15)
    if entered_password:
        _click_if_present(driver, (By.ID, "idSIButton9"), timeout=8)
        print("OK: Entered Entra password")

    if _click_if_present(driver, (By.ID, "idSIButton9"), timeout=5):
        print("OK: Confirmed stay signed in prompt")


def build_driver() -> webdriver.Edge:
    options = webdriver.EdgeOptions()
    options.add_experimental_option("excludeSwitches", ["enable-logging"])
    options.add_argument("--log-level=3")
    options.add_argument("--disable-logging")

    profile_dir = os.path.join(os.path.expanduser("~"), ".databricks_edge_profile")
    options.add_argument(f"--user-data-dir={profile_dir}")
    options.add_argument("--profile-directory=Default")

    return webdriver.Edge(options=options)


def ensure_logged_in(driver: webdriver.Edge, wait: WebDriverWait) -> None:
    driver.get(WORKSPACE_URL)

    try:
        wait.until(EC.url_contains(f"o={ORG_ID}"))
        print("OK: Existing authenticated session detected")
        return
    except TimeoutException:
        pass

    try:
        entra_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, '[data-component-id="webapp.login.aad.submit"]'))
        )
        entra_button.click()
        print("OK: Clicked continue with Microsoft Entra ID")
    except TimeoutException:
        print("INFO: Entra button not visible; waiting for auth flow")

    attempt_entra_form_login(driver)

    try:
        wait.until(EC.url_contains(f"o={ORG_ID}"))
        print("OK: Login completed")
    except TimeoutException as exc:
        raise RuntimeError(
            "Login did not complete. If MFA or conditional access is required, complete it manually once."
        ) from exc


def main() -> None:
    driver = build_driver()
    keep_browser_open = os.getenv("KEEP_BROWSER_OPEN", "0").strip().lower() in {"1", "true", "yes"}
    enable_offset_validation = ENABLE_OFFSET_VALIDATION_DEFAULT

    try:
        wait = WebDriverWait(driver, 180)

        ensure_logged_in(driver, wait)

        print(f"INFO: Offset validation enabled = {enable_offset_validation} (set ENABLE_OFFSET_VALIDATION=0 to disable)")
        if enable_offset_validation:
            print("INFO: OFFSET CHECK STATUS = ENABLED. Script WILL check run offsets from Raw Data.")
        else:
            print("INFO: OFFSET CHECK STATUS = DISABLED. Script will NOT check run offsets.")

        pat_token = os.getenv("DATABRICKS_TOKEN", "").strip()
        teams_flow_url = os.getenv("TEAMS_FLOW_URL", DEFAULT_TEAMS_FLOW_URL).strip()
        teams_webhook_url = os.getenv("TEAMS_WEBHOOK_URL", "").strip()

        all_running_jobs: list[dict] = []
        all_validation_results: list[dict] = []
        
        # Store pipeline data and notification tasks
        pipeline_data: list[dict] = []
        notification_threads: list[threading.Thread] = []

        # Process each pipeline fully (collect → validate → store) before moving to the next.
        for jid, jurl in JOB_IDS:
            pipeline = JOB_PIPELINE.get(jid, jid)
            print(f"\n===== PIPELINE: {pipeline} =====")
            if enable_offset_validation:
                print(f"INFO: This pipeline WILL run offset checks (JOB_ID={jid}).")
            else:
                print(f"INFO: This pipeline will skip offset checks (JOB_ID={jid}).")

            driver.get(jurl)
            print(f"OK: Navigated to job page (JOB_ID={jid})")
            if pat_token:
                print(f"OK: Using Databricks REST API with PAT token for JOB_ID={jid}")
                pipeline_jobs = get_running_jobs_via_api(pat_token, job_id=jid)
            else:
                print(f"INFO: DATABRICKS_TOKEN not set; falling back to UI scraper for JOB_ID={jid}")
                pipeline_jobs = get_running_jobs(driver, limit=None, job_url=jurl, job_id=jid)

            print(f"OK: Retrieved {len(pipeline_jobs)} active job runs for '{pipeline}'")
            print("\n----- ACTIVE RUNS -----")
            for idx, item in enumerate(pipeline_jobs, start=1):
                print(
                    f"{idx}. Division={item.get('div_nbr', item['dc'])} | "
                    f"ENV={item['env']} | APP={item['app']}"
                )

            pipeline_validations: list[dict] = []
            if enable_offset_validation:
                print(f"\n----- RAW DATA OFFSET VALIDATION: {pipeline} -----")
                sys.stdout.flush()
                if not pipeline_jobs:
                    print("INFO: No active runs found; no offset checks to execute.")
                pipeline_validations = validate_job_offsets_from_raw_data(driver, pipeline_jobs, max_jobs=None)
                print(f"INFO: Offset checks executed for {len(pipeline_validations)} run(s) in '{pipeline}'.")
                sys.stdout.flush()
                zero_lag_count = 0
                for idx, check in enumerate(pipeline_validations, start=1):
                    metrics = check.get("metrics", {})
                    print(
                        f"{idx}. RUN_ID={check['run_id']} | RAW_DATA_OPENED={check['raw_data_opened']} | "
                        f"ZERO_LAG={check['is_zero_lag']} | NOTE={check['note']}"
                    )
                    if metrics:
                        print(
                            "   "
                            f"avgOffsetsBehindLatest={metrics.get('avgOffsetsBehindLatest', 'N/A')} | "
                            f"estimatedTotalBytesBehindLatest={metrics.get('estimatedTotalBytesBehindLatest', 'N/A')} | "
                            f"maxOffsetsBehindLatest={metrics.get('maxOffsetsBehindLatest', 'N/A')} | "
                            f"minOffsetsBehindLatest={metrics.get('minOffsetsBehindLatest', 'N/A')}"
                        )
                    if check["is_zero_lag"]:
                        zero_lag_count += 1

                print(
                    f"INFO: Zero-lag validated for {zero_lag_count}/{len(pipeline_validations)} runs "
                    "based on Raw Data metrics."
                )
                sys.stdout.flush()
            else:
                print(
                    "INFO: Offset validation disabled (ENABLE_OFFSET_VALIDATION=0). "
                    "Skipping run detail/Raw Data pages."
                )
                sys.stdout.flush()

            print(f"\n----- SUMMARY MESSAGE: {pipeline} -----")
            print(build_running_summary_message(pipeline_jobs, pipeline_validations))

            # Store pipeline data for concurrent notification
            pipeline_data.append({
                "pipeline": pipeline,
                "pipeline_jobs": pipeline_jobs,
                "pipeline_validations": pipeline_validations,
                "teams_flow_url": teams_flow_url,
                "teams_webhook_url": teams_webhook_url,
            })

            all_running_jobs.extend(pipeline_jobs)
            all_validation_results.extend(pipeline_validations)
            print(f"\n===== PIPELINE COMPLETE: {pipeline} =====")
            sys.stdout.flush()

        # Send all Teams notifications concurrently
        print(f"\n===== SENDING TEAMS NOTIFICATIONS (CONCURRENT) =====")
        sys.stdout.flush()
        
        def send_notification_thread(pipeline_info: dict) -> None:
            pipeline = pipeline_info["pipeline"]
            pipeline_jobs = pipeline_info["pipeline_jobs"]
            pipeline_validations = pipeline_info["pipeline_validations"]
            teams_flow_url = pipeline_info["teams_flow_url"]
            teams_webhook_url = pipeline_info["teams_webhook_url"]
            
            if teams_flow_url:
                workflow_match = re.search(r"/workflows/([A-Za-z0-9]+)/", teams_flow_url)
                workflow_id = workflow_match.group(1) if workflow_match else "UNKNOWN"
                print(f"INFO: [Thread] Posting to Power Automate workflow ID: {workflow_id} for '{pipeline}'")
                print(f"INFO: [Thread] Sending Teams notification for pipeline '{pipeline}' ({len(pipeline_jobs)} runs)")
                try:
                    send_power_automate_notification(teams_flow_url, pipeline_jobs, pipeline_validations, pipeline_name=pipeline)
                    print(f"OK: [Thread] Sent Teams notification for '{pipeline}'")
                except Exception as exc:
                    print(f"ERROR: [Thread] Teams notification FAILED for '{pipeline}' — {exc}")
            elif teams_webhook_url:
                print(f"INFO: [Thread] Sending legacy webhook notification for pipeline '{pipeline}'")
                try:
                    send_legacy_webhook_notification(
                        teams_webhook_url,
                        pipeline_jobs,
                        pipeline_validations,
                        pipeline_name=pipeline,
                    )
                    print(f"OK: [Thread] Sent legacy webhook notification for '{pipeline}'")
                except Exception as exc:
                    print(f"ERROR: [Thread] Teams webhook notification FAILED for '{pipeline}' — {exc}")
            else:
                print(f"INFO: [Thread] TEAMS_FLOW_URL not set; TEAMS_WEBHOOK_URL not set; skipped Teams notification for '{pipeline}'")

        # Launch notification threads for all pipelines
        for p_data in pipeline_data:
            thread = threading.Thread(target=send_notification_thread, args=(p_data,))
            notification_threads.append(thread)
            thread.start()

        # Wait for all notification threads to complete
        for thread in notification_threads:
            thread.join()
        
        print(f"INFO: All Teams notifications sent concurrently.")
        sys.stdout.flush()
        
    finally:
        if keep_browser_open:
            print("INFO: KEEP_BROWSER_OPEN enabled. Browser left open.")
        else:
            driver.quit()
            print("OK: Browser closed. Script completed.")


def test_teams_ping() -> None:
    """Send a quick test ping to Teams to confirm the Flow URL is working."""
    url = os.getenv("TEAMS_FLOW_URL", DEFAULT_TEAMS_FLOW_URL).strip()
    print(f"INFO: Sending test ping to Teams Flow URL...")
    payload = {
        "source": "databricks-selenium-automation",
        "workspace_url": WORKSPACE_URL,
        "org_id": ORG_ID,
        "title": "TEST PING — Databricks Automation",
        "summary_subject": "TEST PING — Databricks Automation",
        "summary_message": "This is a test ping from the Databricks automation script to confirm Teams delivery is working.",
        "message": "This is a test ping from the Databricks automation script to confirm Teams delivery is working.",
        "text": "This is a test ping from the Databricks automation script to confirm Teams delivery is working.",
        "teams_message_body": "This is a test ping from the Databricks automation script to confirm Teams delivery is working.",
        "active_run_count": 0,
        "count": 0,
        "jobs": [],
        "validation_results": [],
        "trace_id": f"PING-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "collected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    try:
        _post_json(url, payload)
        print("OK: Test ping sent successfully — check your Teams channel!")
    except Exception as exc:
        print(f"ERROR: Test ping FAILED — {exc}")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "ping":
        test_teams_ping()
    else:
        main()
