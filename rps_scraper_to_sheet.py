import os
import re
import time
import json
from datetime import datetime
from playwright.sync_api import sync_playwright
import gspread
from gspread.exceptions import APIError
from oauth2client.service_account import ServiceAccountCredentials

# === STEP 1: Scrape data from RPS site ===
def scrape_rps_data():
    all_data = []
    print("🌐 Launching Playwright (headless)...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        print("🌐 Navigating to parent page...")
        page.goto("http://smart.dsmsoft.com/FMSSmartApp/Safex_RPS_Reports/RPS_Reports.aspx?usergroup=NRM.101", wait_until="load")
        page.wait_for_timeout(5000)

        print("🔍 Locating iframe...")
        frame = page.frame_by_url(re.compile(r".*Safex_RPS_Reports_Details\.aspx.*"))
        if not frame:
            raise Exception("❌ Iframe not found!")

        print("📅 Clicking FromDate input...")
        frame.click('input[id="ctl00_ContentPlaceHolder1_dtFrom"]')
        page.wait_for_timeout(1000)

        today = datetime.today()
        day_xpath = f'//td[@data-date="{today.day}" and contains(@class, "xdsoft_date") and not(contains(@class, "xdsoft_disabled"))]'
        print(f"📅 Selecting date using XPath: {day_xpath}")
        try:
            frame.locator(f'xpath={day_xpath}').click()
            print("✅ FromDate selected successfully.")
        except Exception as e:
            raise Exception(f"❌ Failed to click calendar date: {e}")

        print("🚛 Selecting all vehicles...")
        vehicle_select = frame.locator('select[id="ctl00_ContentPlaceHolder1_ddlVehicle"]')
        options = vehicle_select.locator('option').all()
        for option in options:
            option.click()

        print("📤 Clicking Submit...")
        frame.click('input[id="ctl00_ContentPlaceHolder1_btnSubmit"]')
        page.wait_for_timeout(5000)

        print("🔍 Waiting for results table...")
        try:
            frame.wait_for_selector('table[id="ctl00_ContentPlaceHolder1_gvReport"] tbody tr', timeout=10000)
            rows = frame.locator('table[id="ctl00_ContentPlaceHolder1_gvReport"] tbody tr').all()
            print(f"✅ Found {len(rows)} rows")

            for row in rows:
                cells = row.locator('td').all()
                data = [cell.inner_text().strip() for cell in cells]
                all_data.append(data)

        except Exception as e:
            print(f"❌ Failed to load table: {e}")

        browser.close()

    print(f"🏁 Finished scraping. Total records: {len(all_data)}")
    return all_data


# === STEP 2: Google Sheet upload helpers ===
def retry_gspread_request(func, *args, retries=5, delay=2, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            if "503" in str(e):
                print(f"[{attempt + 1}/{retries}] 503 error. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    raise Exception("Max retries exceeded for gspread request.")

def write_to_google_sheet(sheet_id, sheet_name, data, header=None):
    print("🔐 Authorizing Google Sheets client...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

    creds_dict = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    client = gspread.authorize(creds)

    sheet = retry_gspread_request(lambda: client.open_by_key(sheet_id).worksheet(sheet_name))

    print("🧹 Clearing sheet...")
    retry_gspread_request(sheet.clear)
    if header:
        print("📝 Writing headers...")
        retry_gspread_request(sheet.insert_row, header, 1)
    print(f"📥 Inserting {len(data)} rows...")
    retry_gspread_request(sheet.insert_rows, data, 2 if header else 1)
    print("✅ Data written to Google Sheet.")


# === MAIN RUNNER ===
if __name__ == "__main__":
    print("🚀 RPS scraping started.")
    try:
        rps_data = scrape_rps_data()

        headers = [
            "RPS Number", "Vehicle Number", "Dispatch Date", "Closure Date",
            "Transit Time", "Route Name", "Target Time", "Extra"
        ]

        SHEET_ID = "1VyuRPidEfJkXk1xtn2uSmKGgcb8df90Wwx_TJ9qBLw0"
        SHEET_NAME = "All_RPS"

        write_to_google_sheet(SHEET_ID, SHEET_NAME, rps_data, headers)
    except Exception as e:
        print(f"❌ ERROR: {e}")
    print("🏁 Script completed.")
