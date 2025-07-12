import pandas as pd
from datetime import datetime
import time
import logging
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from gspread.exceptions import APIError
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

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

def get_google_sheet(sheet_id, sheet_name):
    print("🔐 Authenticating with Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    credentials_path = "credentials.json"
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)
    sheet = retry_gspread_request(lambda: client.open_by_key(sheet_id).worksheet(sheet_name))
    print("✅ Google Sheet accessed successfully.")
    return sheet

def scrape_rps_data():
    all_data = []
    print("🌐 Launching Playwright (headless)...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        print("🌐 Navigating to RPS Reports page...")
        page.goto("http://smart.dsmsoft.com/FMSSmartApp/Safex_RPS_Reports/RPS_Reports.aspx?usergroup=NRM.101", wait_until="domcontentloaded")

        today = datetime.today().strftime("%d-%m-%Y")
        print(f"📅 Filling date: {today}")
        page.fill('input[formcontrolname="FromDate"]', today)
        page.fill('input[formcontrolname="ToDate"]', today)

        print("🚛 Selecting all vehicles...")
        page.click('input[formcontrolname="VehicleList"]')
        page.keyboard.press('Control+A')
        page.keyboard.press('Enter')

        print("📤 Submitting filter form...")
        page.click('button:has-text("Submit")')
        page.wait_for_timeout(5000)

        page_num = 1
        while True:
            print(f"📄 Scraping Page {page_num}...")
            try:
                page.wait_for_selector('table tbody tr', timeout=10000)
            except Exception as e:
                print(f"⚠️ Table not found on page {page_num}: {e}")
                break

            rows = page.query_selector_all('table tbody tr')
            for row in rows:
                cells = row.query_selector_all('td')
                data = [cell.inner_text().strip() for cell in cells]
                all_data.append(data)

            next_button = page.query_selector('li.pagination-next:not(.disabled)')
            if not next_button or 'disabled' in next_button.get_attribute("class"):
                print("✅ No more pages.")
                break
            else:
                next_button.click()
                page.wait_for_timeout(2000)
                page_num += 1

        browser.close()
    print(f"✅ Finished scraping. Total records: {len(all_data)}")
    return all_data

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

if __name__ == "__main__":
    print("🚀 RPS scraping started.")
    try:
        rps_data = scrape_rps_data()
        headers = [
            "RPS Number", "Vehicle Number", "Dispatch Date", "Closure Date",
            "Transit Time(HH:MM:SS)", "Route Name", "Carrier Name", "Closure Location"
        ]
        SHEET_ID = "1VyuRPidEfJkXk1xtn2uSmKGgcb8df90Wwx_TJ9qBLw0"
        SHEET_NAME = "All_RPS"
        write_to_sheet(SHEET_ID, SHEET_NAME, rps_data, headers)
    except Exception as e:
        print(f"❌ ERROR: {e}")
    print("🏁 Script completed.")
