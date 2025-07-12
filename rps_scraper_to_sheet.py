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
    print("🚀 Launching browser...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)  # Use headless=True for automation
        context = browser.new_context()
        page = context.new_page()

        # Step 1: Go to the page
        print("🌐 Navigating to page...")
        page.goto("http://smart.dsmsoft.com/FMSSmartApp/Safex_RPS_Reports/RPS_Reports.aspx?usergroup=NRM.101", wait_until="load")
        page.wait_for_timeout(5000)

        # Step 2: Click dropdown and select all vehicles
        print("🚛 Clicking vehicle dropdown...")
        page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[4]/div[2]').click()
        page.wait_for_timeout(1000)

        print("✅ Selecting all vehicles...")
        page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[4]/div[3]/div[2]/ul/li[1]/input').click()
        page.wait_for_timeout(1000)

        # Step 3: Open date picker and select today's date
        print("📅 Clicking date picker...")
        page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[1]/div[2]/input').click()
        page.wait_for_timeout(1000)

        today = datetime.now()
        day_xpath = f'//td[@data-date="{today.day}" and contains(@class, "xdsoft_date") and not(contains(@class, "xdsoft_disabled"))]'
        print(f"📅 Selecting day: {today.day}")
        page.locator(f'xpath={day_xpath}').nth(0).click()
        page.wait_for_timeout(1000)

        print("✅ Done.")

        browser.close()
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
