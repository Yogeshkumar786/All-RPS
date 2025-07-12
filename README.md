# RPS Data Scraper

This Python script automatically scrapes today's RPS data and uploads it to a Google Sheet.

## Features
- Headless scraping using Playwright
- Uploads to Google Sheets using service account
- Retry logic for gspread 503 errors
- Secure credentials via `.env` file

## Setup

1. Clone this repo.
2. Create a `.env` file like:
