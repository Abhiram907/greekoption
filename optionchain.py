import pandas as pd
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from pytz import timezone  # For handling timezones
from SmartApi import SmartConnect
import pyotp
from logzero import logger
from datetime import datetime, timedelta
import pytz
import os

api_key = 'ka8AoHBH'
username = 'R829267'
pwd = '3132'
api= SmartConnect(api_key)
try:
    token = "VZIVAZKQIJLUYVDKQM2QEQI2V4"
    totp = pyotp.TOTP(token).now()
except Exception as e:
    logger.error("Invalid Token: The provided token is not valid.")
    raise e

correlation_id = "abcde"
data = api.generateSession(username, pwd, totp)

if data['status'] == False:
    logger.error(data)

def is_trading_hours():
    ist = timezone('Asia/Kolkata')
    now = datetime.now(ist)
    start_time = now.replace(hour=9, minute=15, second=0, microsecond=0)
    end_time = now.replace(hour=15, minute=30, second=0, microsecond=0)
    return start_time <= now <= end_time


# Initialize an empty DataFrame to store the differences
difference_table = pd.DataFrame(columns=[
    "timestamp", 
    "name", 
    "price",  # New column for Last Traded Price (LTP)
    "delta_diff_CE", "gamma_diff_CE", "theta_diff_CE", "vega_diff_CE",
    "delta_diff_PE", "gamma_diff_PE", "theta_diff_PE", "vega_diff_PE"
])

# Variable to store the previous summary
previous_summary = None

# Function to fetch and process data
def get_data():
    params = {
        "name": "NIFTY",
        "expirydate": "13MAR2025"
    }
    try:
        # Fetch option Greeks data
        optionGreek = api.optionGreek(params)
        greeks = pd.DataFrame(optionGreek['data'])

        # Separate CE and PE options
        greeks_CE = greeks[greeks['optionType'] == 'CE']
        greeks_PE = greeks[greeks['optionType'] == 'PE']

        # Merge CE and PE data on strikePrice
        greeks_merged = pd.merge(greeks_CE, greeks_PE, on='strikePrice')

        # Add timestamp
        greeks_merged['timestamp'] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

        return greeks_merged

    except Exception as e:
        logger.error(f"Error fetching or processing data: {e}")
        raise e


# Authenticate with Google Sheets
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def authenticate_google_sheets():
    # Path to your service account JSON key file
    credentials_path = "/content/gen-lang-client-0864245587-f83783301092.json"

    # Define the scope of permissions
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    # Authenticate using the service account
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(credentials)

    # Open the Google Sheet by its name
    spreadsheet = client.open_by_key("1qkW03aN6DYLKHLUIbMjiC0toDD_3caGt37FgW6DbmWc")


    # Get references to both sheets
    sheet1 = spreadsheet.sheet1  # Sheet for difference_table
    sheet2 = spreadsheet.get_worksheet(1)  # Sheet for current_summary (index 1 refers to the second sheet)

    return sheet1, sheet2

# Main loop
if __name__ == "__main__":
    # Authenticate and get both sheets
    sheet1, sheet2 = authenticate_google_sheets()

    while True:
      ist = pytz.timezone('Asia/Kolkata')
      now = datetime.now(ist)

      
      if is_trading_hours():
            print(f"Running at {datetime.now(timezone('Asia/Kolkata')).strftime('%Y-%m-%d %H:%M:%S')} IST")

            option_chain = get_data()

            try:
                option_chain['strikePrice'] = pd.to_numeric(option_chain['strikePrice'], errors='coerce')
            except Exception as e:
                logger.error(f"Error converting strikePrice to numeric: {e}")
                continue

            # Step 1: Convert relevant columns to numeric
            try:
                numeric_columns = ['delta_x', 'gamma_x', 'theta_x', 'vega_x', 'delta_y', 'gamma_y', 'theta_y', 'vega_y']
                for col in numeric_columns:
                    option_chain[col] = pd.to_numeric(option_chain[col], errors='coerce').fillna(0)  # Replace NaN with 0
            except Exception as e:
                logger.error(f"Error converting columns to numeric: {e}")
                continue

            # Step 2: Extract the first strike price
            first_strike_price = option_chain['strikePrice'].iloc[0]

            # Step 3: Define the range [strikePrice - 750, strikePrice + 750]
            lower_bound = first_strike_price - 15 * 50
            upper_bound = first_strike_price + 15 * 50

            # Step 4: Filter rows where strikePrice is within the range
            filtered_option_chain = option_chain[
                (option_chain['strikePrice'] >= lower_bound) &
                (option_chain['strikePrice'] <= upper_bound)
            ]

            # Step 5: Sum the relevant columns
            current_summary = {
                "timestamp": pd.Timestamp.now(tz=timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                "name": "NIFTY",  # Name of the underlying asset
                "delta_x_sum": filtered_option_chain['delta_x'].sum(),
                "gamma_x_sum": filtered_option_chain['gamma_x'].sum(),
                "theta_x_sum": filtered_option_chain['theta_x'].sum(),
                "vega_x_sum": filtered_option_chain['vega_x'].sum(),
                "delta_y_sum": filtered_option_chain['delta_y'].sum(),
                "gamma_y_sum": filtered_option_chain['gamma_y'].sum(),
                "theta_y_sum": filtered_option_chain['theta_y'].sum(),
                "vega_y_sum": filtered_option_chain['vega_y'].sum()
            }

            # Step 6: Calculate differences
            if previous_summary is None:
                # First iteration: No previous summary, so differences are 0
                ce_differences = {
                    "delta_diff_CE": 0,
                    "gamma_diff_CE": 0,
                    "theta_diff_CE": 0,
                    "vega_diff_CE": 0
                }
                pe_differences = {
                    "delta_diff_PE": 0,
                    "gamma_diff_PE": 0,
                    "theta_diff_PE": 0,
                    "vega_diff_PE": 0
                }
            else:
                # Ensure all values are numeric before calculating differences
                ce_differences = {
                    "delta_diff_CE": float(current_summary["delta_x_sum"]) - float(previous_summary["delta_x_sum"]),
                    "gamma_diff_CE": float(current_summary["gamma_x_sum"]) - float(previous_summary["gamma_x_sum"]),
                    "theta_diff_CE": float(current_summary["theta_x_sum"]) - float(previous_summary["theta_x_sum"]),
                    "vega_diff_CE": float(current_summary["vega_x_sum"]) - float(previous_summary["vega_x_sum"])
                }
                pe_differences = {
                    "delta_diff_PE": float(current_summary["delta_y_sum"]) - float(previous_summary["delta_y_sum"]),
                    "gamma_diff_PE": float(current_summary["gamma_y_sum"]) - float(previous_summary["gamma_y_sum"]),
                    "theta_diff_PE": float(current_summary["theta_y_sum"]) - float(previous_summary["theta_y_sum"]),
                    "vega_diff_PE": float(current_summary["vega_y_sum"]) - float(previous_summary["vega_y_sum"])
                }

            # Step 7: Fetch the Last Traded Price (LTP)
            try:
                ltp_response = api.ltpData('NSE', 'NIFTY', '26000')  # Fetch LTP for NIFTY
                if ltp_response['status']:
                    ltp = ltp_response['data']['ltp']  # Extract LTP
                else:
                    ltp = None  # Handle cases where LTP data is unavailable
            except Exception as e:
                logger.error(f"Error fetching LTP: {e}")
                ltp = None

            # Step 8: Update the difference_table
            combined_differences = {
                "timestamp": current_summary["timestamp"],
                "name": "NIFTY",
                "price": ltp,  # Add LTP to the row
                **ce_differences,
                **pe_differences
            }

            if difference_table.empty:
                # If the table is empty, add the first row
                difference_table.loc[0] = combined_differences
            else:
                # If the table already has rows, append a new row
                difference_table.loc[len(difference_table)] = combined_differences

            # Print the updated difference table
            print("\nDifference Table:")
            print(difference_table)

            # Step 9: Write the difference_table to Sheet1
            try:
                # Convert the DataFrame to a list of lists
                records = difference_table.values.tolist()

                # Add column headers if the sheet is empty
                if not sheet1.get_all_values():
                    headers = difference_table.columns.tolist()
                    sheet1.append_row(headers)  # Write the headers

                # Append the latest row only
                sheet1.append_row(records[-1])  # Append the last row (latest update)
            except Exception as e:
                logger.error(f"Error writing to Sheet1: {e}")

            # Step 10: Write the current_summary to Sheet2
            try:
                # Convert the current_summary dictionary to a list
                summary_record = list(current_summary.values())

                # Add column headers if the sheet is empty
                if not sheet2.get_all_values():
                    headers = list(current_summary.keys())
                    sheet2.append_row(headers)  # Write the headers

                # Append the latest summary record
                sheet2.append_row(summary_record)
            except Exception as e:
                logger.error(f"Error writing to Sheet2: {e}")

            # Step 11: Update previous_summary for the next iteration
            previous_summary = current_summary

            # Wait for 60 seconds before the next iteration
            time.sleep(60)
      else:
            print("Outside trading hours. Waiting until 9:15 AM IST...")
            # Calculate the time to wait until the next trading day at 9:15 AM
            next_start = now.replace(hour=9, minute=15, second=0, microsecond=0)
            if now > next_start.replace(hour=15, minute=30):  # If past 3:30 PM, move to the next day
                next_start += timedelta(days=1)
            sleep_time = (next_start - now).total_seconds()
            time.sleep(sleep_time)
