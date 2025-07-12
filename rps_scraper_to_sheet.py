import pandas as pd
from datetime import datetime
import time
import logging
import os
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError
from playwright.sync_api import sync_playwright

# === LOAD .env FILE ===
load_dotenv()

# === LOGGING SETUP ===
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# === RETRY WRAPPER ===
def retry_gspread_request(func, *args, retries=5, delay=2, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except APIError as e:
            if "503" in str(e):
                logging.warning(f"[{attempt + 1}/{retries}] 503 APIError. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2
            else:
                raise
    raise Exception("Max retries exceeded for gspread request.")

# === GET GOOGLE SHEET OBJECT ===
def get_google_sheet(sheet_id, sheet_name):
    print("🔐 Authenticating with Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)
    sheet = retry_gspread_request(lambda: client.open_by_key(sheet_id).worksheet(sheet_name))
    print("✅ Google Sheet accessed successfully.")
    return sheet

# === SCRAPE RPS TABLE FOR TODAY
def scrape_rps_data():
    all_data = []
    print("🌐 Launching Playwright (headless)...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        print("🌐 Navigating to page...")
        page.goto("http://smart.dsmsoft.com/FMSSmartApp/Safex_RPS_Reports/RPS_Reports.aspx?usergroup=NRM.101", wait_until="domcontentloaded")

        today = datetime.today().strftime("%d-%m-%Y")
        print(f"📅 Filling From/To Date: {today}")
        page.fill('xpath=//input[@id="ctl00_ContentPlaceHolder1_dtFrom"]', today)
        page.fill('xpath=//input[@id="ctl00_ContentPlaceHolder1_dtTo"]', today)

        print("🚛 Selecting all vehicles...")
        vehicle_select = page.locator('xpath=//select[@id="ctl00_ContentPlaceHolder1_ddlVehicle"]')
        options = vehicle_select.locator('option').all()
        for option in options:
            option.click()

        print("📤 Clicking Submit...")
        page.click('xpath=//input[@id="ctl00_ContentPlaceHolder1_btnSubmit"]')
        page.wait_for_timeout(5000)  # Allow time for table to load

        print("🔍 Waiting for table...")
        try:
            page.wait_for_selector('xpath=//table[@id="ctl00_ContentPlaceHolder1_gvReport"]/tbody/tr', timeout=10000)
            rows = page.locator('xpath=//table[@id="ctl00_ContentPlaceHolder1_gvReport"]/tbody/tr').all()
            print(f"✅ Found {len(rows)} rows")

            for row in rows:
                cells = row.locator('td').all()
                data = [cell.inner_text().strip() for cell in cells]
                all_data.append(data)

        except Exception as e:
            print(f"❌ Could not load data table: {e}")

        browser.close()
    print(f"🏁 Scraping complete. Total records: {len(all_data)}")
    return all_data

# === WRITE TO GOOGLE SHEET ===
def write_to_sheet(sheet_id, sheet_name, data, header=None):
    print("📥 Preparing to write to Google Sheet...")
    sheet = get_google_sheet(sheet_id, sheet_name)
    print("🧹 Clearing existing data...")
    retry_gspread_request(sheet.clear)
    if header:
        print("🧾 Writing headers...")
        retry_gspread_request(sheet.insert_row, header, 1)
    print(f"📝 Inserting {len(data)} rows...")
    retry_gspread_request(sheet.insert_rows, data, 2 if header else 1)
    print("✅ Data successfully written to Google Sheet.")

# === MAIN ===
if __name__ == "__main__":
    print("🚀 RPS scraping started.")
    try:
        rps_data = scrape_rps_data()
        headers = [
            "RPS Number", "Vehicle Number", "Dispatch Date", "Closure Date",
            "Transit Time", "Route Name", "Target Time", "Extra"
        ]
        SHEET_ID = os.getenv("SHEET_ID")
        SHEET_NAME = os.getenv("SHEET_NAME", "All_RPS")
        write_to_sheet(SHEET_ID, SHEET_NAME, rps_data, headers)
    except Exception as e:
        print(f"❌ ERROR: {e}")
    print("🏁 Script completed.")
