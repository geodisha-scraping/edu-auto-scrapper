"""
selenium_check_urls.py

What it does:
- Reads input CSV/Excel (auto-detects 'URL' column, case-insensitive)
- For each URL:
    1) Try a requests.get() with a short timeout to check basic HTTP status
    2) If requests is OK (status < 400) OR if requests fails, attempt to open in Chrome using Selenium
    3) If Selenium loads page and document.readyState == 'complete', mark as "valid", else "invalid"
- Writes output to CSV and Excel with an added 'status' column.

Requirements:
pip install pandas requests selenium webdriver-manager openpyxl
Also ensure Chrome is installed on the machine.

Run:
python selenium_check_urls.py
"""

"""
selenium_check_urls_with_checkpoint.py

Selenium + requests URL validator with robust checkpointing/resume.

How it resumes:
- If OUTPUT_CSV exists, the script loads it and continues from the first row whose 'status'
  is empty (or missing). Each processed row is saved immediately so a crash/restart will
  continue without re-processing already-done rows.

Requirements:
pip install pandas requests selenium webdriver-manager openpyxl
Chrome must be installed.

Run:
python selenium_check_urls_with_checkpoint.py
"""

import os
import time
import json
import tempfile
import pandas as pd
import requests
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, TimeoutException, InvalidArgumentException
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# === CONFIG - adjust to your environment ===
INPUT_PATH = r"D:\Education - Valid Url\IRINS Dashboard.csv"
OUTPUT_CSV = r"D:\Education - Valid Url\IRINS_Dashboard_with_status.csv"
OUTPUT_XLSX = r"D:\Education - Valid Url\IRINS_Dashboard_with_status.xlsx"
CHECKPOINT_JSON = r"D:\Education - Valid Url\IRINS_Dashboard_checkpoint.json"

# Selenium / network timeouts
REQUESTS_TIMEOUT = 8
SELENIUM_PAGE_LOAD_TIMEOUT = 30
SELENIUM_WAIT_SHORT = 10
SELENIUM_WAIT_LONG = 45
SELENIUM_MAX_WAIT_PER_URL = 70
BODY_MIN_LENGTH = 20
EXTRA_JS_SETTLE = 1.0

# Checkpointing / saving control
# If AUTOSAVE_EVERY == 1 -> save after each URL (most robust). Increase to reduce disk writes.
AUTOSAVE_EVERY = 1
# If True, script will ignore existing output and start fresh (useful for debugging)
FORCE_RESTART = False

# === helper functions ===

def atomic_write_csv(df, path):
    """Write DataFrame to disk atomically using a temp file + replace."""
    dirn = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(dir=dirn, suffix=".tmp")
    os.close(fd)
    try:
        df.to_csv(tmp, index=False)
        # atomic replace
        os.replace(tmp, path)
    finally:
        # cleanup if tmp still exists
        if os.path.exists(tmp):
            try:
                os.remove(tmp)
            except Exception:
                pass

def save_checkpoint_meta(meta, path):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f)
    os.replace(tmp, path)

def load_checkpoint_meta(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None

def find_url_column(df):
    for col in df.columns:
        if col.strip().lower() in ("url", "link", "website"):
            return col
    for col in df.columns:
        sample = df[col].astype(str).fillna("").head(20)
        if sample.str.contains(r"^https?://", regex=True).any():
            return col
    raise ValueError("Could not find a URL column. Make sure there is a column named 'URL' or containing http(s) links.")

def try_requests_head_or_get(url):
    try:
        r = requests.get(url, timeout=REQUESTS_TIMEOUT, allow_redirects=True, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
        })
        return True, r.status_code
    except Exception as e:
        return False, str(e)

def setup_driver(headless=False):
    options = webdriver.ChromeOptions()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.set_page_load_timeout(SELENIUM_PAGE_LOAD_TIMEOUT)
    return driver

def wait_for_page_complete(driver, wait_seconds):
    try:
        wait = WebDriverWait(driver, wait_seconds)
        wait.until(lambda d: d.execute_script("return document.readyState") == "complete")
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(EXTRA_JS_SETTLE)
        body_len = driver.execute_script("return document.body ? document.body.innerHTML.length : 0") or 0
        if body_len >= BODY_MIN_LENGTH:
            return True, f"ready; body_len={body_len}"
        else:
            return True, f"ready_but_small_body; body_len={body_len}"
    except TimeoutException as te:
        return False, f"timeout_waiting:{te}"
    except WebDriverException as we:
        return False, f"webdriver_error:{we}"
    except Exception as e:
        return False, f"other_error:{e}"

def check_with_selenium(driver, url):
    start_ts = time.time()
    try:
        driver.get(url)
    except TimeoutException:
        # partial content may have loaded; continue to explicit waits below
        pass
    except InvalidArgumentException as iae:
        return False, f"invalid_url:{iae}"
    except WebDriverException as we:
        return False, f"webdriver_get_error:{we}"
    except Exception as e:
        return False, f"get_exception:{e}"

    ok, detail = wait_for_page_complete(driver, SELENIUM_WAIT_SHORT)
    if ok:
        return True, f"short_wait_ok:{detail}; elapsed={time.time()-start_ts:.1f}s"

    elapsed = time.time() - start_ts
    if elapsed < SELENIUM_MAX_WAIT_PER_URL:
        remaining = min(SELENIUM_WAIT_LONG, SELENIUM_MAX_WAIT_PER_URL - elapsed)
        ok2, detail2 = wait_for_page_complete(driver, int(remaining))
        if ok2:
            return True, f"long_wait_ok:{detail2}; elapsed={time.time()-start_ts:.1f}s"
        else:
            return False, f"both_waits_failed: short_detail={detail}; long_detail={detail2}; elapsed={time.time()-start_ts:.1f}s"
    else:
        return False, f"short_wait_failed_and_no_time_for_long_wait; detail={detail}; elapsed={elapsed:.1f}s"

# === main flow with resume logic ===

def main():
    # load original input
    if INPUT_PATH.lower().endswith((".xls", ".xlsx")):
        original_df = pd.read_excel(INPUT_PATH)
    else:
        original_df = pd.read_csv(INPUT_PATH)

    # Normalize: work with a reset integer index to avoid index mismatch on resume
    original_df = original_df.reset_index(drop=True)

    # Ensure URL column detection before anything else
    url_col = find_url_column(original_df)
    print(f"Detected URL column: '{url_col}'")

    # Prepare working dataframe (copy original and ensure status columns exist)
    if FORCE_RESTART:
        # delete any old outputs/checkpoints to force restart
        if os.path.exists(OUTPUT_CSV):
            os.remove(OUTPUT_CSV)
        if os.path.exists(CHECKPOINT_JSON):
            os.remove(CHECKPOINT_JSON)

    if os.path.exists(OUTPUT_CSV):
        try:
            prev = pd.read_csv(OUTPUT_CSV)
            prev = prev.reset_index(drop=True)
            # If prev has same columns as original (or at least URL column), merge status back into working df
            if url_col in prev.columns:
                # build new df that has original columns plus status columns from prev where urls match at same index
                # easiest approach: if sizes match, use prev as base; otherwise merge by URL content
                if len(prev) == len(original_df):
                    df = prev.copy()
                    print("Loaded existing output CSV and resuming by index.")
                else:
                    # merge by URL content to preserve processed rows even if order changed
                    merged = original_df.copy()
                    merged["__orig_index__"] = merged.index
                    prev_subset = prev[[url_col, "status", "status_detail"]] if "status" in prev.columns else prev[[url_col]]
                    merged = merged.merge(prev_subset, on=url_col, how="left", suffixes=("", "_prev"))
                    # prefer status from prev (if present); ensure columns exist
                    if "status" not in merged.columns:
                        merged["status"] = merged.get("status_prev")
                    if "status_detail" not in merged.columns:
                        merged["status_detail"] = merged.get("status_detail_prev")
                    merged = merged.drop(columns=[c for c in merged.columns if c.endswith("_prev")])
                    merged = merged.drop(columns=["__orig_index__"], errors="ignore")
                    df = merged
                    print("Merged existing output CSV by URL and will resume unprocessed rows.")
            else:
                # cannot merge; start fresh but warn
                print("Existing output CSV doesn't contain detected URL column; ignoring it and starting fresh.")
                df = original_df.copy()
                df["status"] = ""
                df["status_detail"] = ""
        except Exception as e:
            print("Could not read existing output CSV (will start fresh). Error:", e)
            df = original_df.copy()
            df["status"] = ""
            df["status_detail"] = ""
    else:
        # no existing output => create new df with status columns
        df = original_df.copy()
        df["status"] = ""
        df["status_detail"] = ""

    # locate first unprocessed row
    def is_unprocessed(val):
        return (pd.isna(val)) or (str(val).strip() == "")

    try:
        first_unprocessed = next(i for i, v in enumerate(df["status"].tolist()) if is_unprocessed(v))
    except StopIteration:
        print("All rows already processed in output. Exiting.")
        # still save to xlsx to ensure final excel exists
        try:
            df.to_excel(OUTPUT_XLSX, index=False)
        except Exception:
            pass
        return

    print(f"Resuming from row index: {first_unprocessed} / {len(df)-1}")

    # start Selenium driver
    driver = setup_driver(headless=False)

    # load checkpoint meta if present
    meta = load_checkpoint_meta(CHECKPOINT_JSON) or {}
    meta.setdefault("last_processed_index", None)
    processed_since_save = 0

    try:
        for idx in range(first_unprocessed, len(df)):
            raw = str(df.at[idx, url_col]).strip()
            if not raw or raw.lower() in ("nan", "none", ""):
                df.at[idx, "status"] = "invalid"
                df.at[idx, "status_detail"] = "empty url"
                print(f"[{idx}] empty -> invalid")
            else:
                url = raw if raw.lower().startswith(("http://", "https://")) else "http://" + raw

                ok_req, req_info = try_requests_head_or_get(url)
                req_ok_flag = False
                if ok_req and isinstance(req_info, int):
                    req_ok_flag = (req_info < 400)

                selenium_ok, detail = check_with_selenium(driver, url)

                if selenium_ok:
                    df.at[idx, "status"] = "valid"
                    df.at[idx, "status_detail"] = f"selenium:{detail}; requests_ok:{req_ok_flag}"
                    print(f"[{idx}] {url} -> valid ({detail})")
                else:
                    df.at[idx, "status"] = "invalid"
                    df.at[idx, "status_detail"] = f"selenium_error:{detail}; requests_ok:{req_ok_flag}"
                    print(f"[{idx}] {url} -> invalid ({detail})")

            # update checkpoint meta
            meta["last_processed_index"] = idx
            processed_since_save += 1

            # Save after every AUTOSAVE_EVERY rows (set AUTOSAVE_EVERY=1 to save each row)
            if processed_since_save >= AUTOSAVE_EVERY:
                try:
                    # atomic write of CSV
                    atomic_write_csv(df, OUTPUT_CSV)
                    # also save small JSON meta
                    save_checkpoint_meta(meta, CHECKPOINT_JSON)
                    print(f"Checkpoint saved at row {idx} -> {OUTPUT_CSV}")
                except Exception as e:
                    print("Warning: failed to save checkpoint:", e)
                processed_since_save = 0

    finally:
        try:
            driver.quit()
        except Exception:
            pass

        # final save
        try:
            atomic_write_csv(df, OUTPUT_CSV)
            # save final xlsx as well
            df.to_excel(OUTPUT_XLSX, index=False)
            meta["last_processed_index"] = meta.get("last_processed_index", None)
            save_checkpoint_meta(meta, CHECKPOINT_JSON)
            print("Final results saved.")
        except Exception as e:
            print("Error saving final results:", e)

    print("Done.")

if __name__ == "__main__":
    main()
