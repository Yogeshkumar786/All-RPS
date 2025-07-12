# File: rps_scraper_to_sheet.py
import os
import json
import time
import pandas as pd
from datetime import datetime
from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials

# === Step 1: Write credentials from GitHub secret ===
def write_google_credentials():
    json_key = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not json_key:
        raise Exception("GOOGLE_APPLICATION_CREDENTIALS secret not found in environment.")
    with open("credentials.json", "w") as f:
        f.write(json_key)

# === Step 2: Download Excel from RPS page ===
def download_and_extract_rps_data():
    print("🚀 Starting RPS extraction via Excel download...")
    download_dir = os.path.abspath("downloads")
    os.makedirs(download_dir, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        print("🌐 Navigating to RPS page...")
        page.goto("http://smart.dsmsoft.com/FMSSmartApp/Safex_RPS_Reports/RPS_Reports.aspx?usergroup=NRM.101", wait_until="load")
        page.wait_for_timeout(4000)

        print("🚛 Selecting all vehicles...")
        page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[4]/div[2]').click()
        page.wait_for_timeout(1000)
        page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[4]/div[3]/div[2]/ul/li[1]/input').click()
        page.wait_for_timeout(1000)

        print("📅 Picking today's date...")
        page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[1]/div[2]/input').click()
        page.wait_for_timeout(1000)
        today = datetime.now()
        day_xpath = f'//td[@data-date="{today.day}" and contains(@class, "xdsoft_date") and not(contains(@class, "xdsoft_disabled"))]'
        page.locator(f'xpath={day_xpath}').nth(0).click()
        page.wait_for_timeout(1000)

        print("📤 Clicking Submit...")
        page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[3]/div/div[5]/div/button').click()
        page.wait_for_timeout(5000)

        print("📥 Clicking download button...")
        with page.expect_download() as download_info:
            page.locator('xpath=/html/body/form/div[5]/div/div/div/div/div/div/div[4]/div/table/div/div[4]/div/div/div[3]/div[1]/div/div/div').click()
        download = download_info.value
        downloaded_file_path = os.path.join(download_dir, download.suggested_filename)
        download.save_as(downloaded_file_path)
        print(f"✅ Excel downloaded to: {downloaded_file_path}")

        browser.close()
        return downloaded_file_path

# === Step 3: Push Excel data to Google Sheet ===
def push_excel_to_google_sheet(excel_path, sheet_id, tab_name):
    print("📥 Reading Excel...")
    df = pd.read_excel(excel_path)

    print("🔐 Authorizing with Google Sheets...")
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
    client = gspread.authorize(creds)

    print("📄 Opening sheet...")
    sheet = client.open_by_key(sheet_id).worksheet(tab_name)

    print("🧹 Clearing sheet...")
    sheet.clear()

    print("📤 Uploading new data...")
    rows = [df.columns.values.tolist()] + df.values.tolist()
    sheet.insert_rows(rows, row=1)

    print("✅ Sheet updated successfully.")

# === MAIN ===
if __name__ == "__main__":
    print("🚀 RPS scraping started.")
    write_google_credentials()
    downloaded_path = download_and_extract_rps_data()
    SHEET_ID = "1VyuRPidEfJkXk1xtn2uSmKGgcb8df90Wwx_TJ9qBLw0"
    SHEET_TAB = "All_RPS"
    push_excel_to_google_sheet(downloaded_path, SHEET_ID, SHEET_TAB)
    print("🏁 Script completed.")
