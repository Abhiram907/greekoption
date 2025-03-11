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
import sys

api_key = 'BbxU1S7Z'
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
    "price",  # Price for CE
    "delta_diff_ce", "gamma_diff_ce", "theta_diff_ce", "vega_diff_ce",
    "delta_diff_pe", "gamma_diff_pe", "theta_diff_pe", "vega_diff_pe"
])

# Variable to store the previous summary
previous_summary = None

# Function to fetch and process data
import pandas as pd
import time

# Function to fetch and process data
def get_data_x():
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

        # # Add timestamp
        # greeks_CE.loc[:, 'timestamp'] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")


        return greeks_CE

    except Exception as e:
        logger.error(f"Error fetching or processing data: {e}")
        raise e

def get_data_y():
    params = {
        "name": "NIFTY",
        "expirydate": "13MAR2025"
    }
    try:
        # Fetch option Greeks data
        time.sleep(1)
        optionGreek = api.optionGreek(params)
        greeks = pd.DataFrame(optionGreek['data'])
        greeks_PE = greeks[greeks['optionType'] == 'PE']

        # Add timestamp
        # greeks_PE.loc[:, 'timestamp'] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")

        return greeks_PE

    except Exception as e:
        logger.error(f"Error fetching or processing data: {e}")
        raise e

# Authenticate with Google Sheets
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def authenticate_google_sheets():
    # Load credentials from the GitHub Secret
    credentials_json = os.getenv("GOOGLE_CREDENTIALS")
    if not credentials_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable is not set.")
    
    # Parse the JSON string into a dictionary
    credentials_dict = json.loads(credentials_json)
    
    # Define the scope of permissions
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]
    
    # Authenticate using the service account
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(credentials_dict, scope)
    client = gspread.authorize(credentials)
    
    # Open the Google Sheet by its ID
    spreadsheet = client.open_by_key("1qkW03aN6DYLKHLUIbMjiC0toDD_3caGt37FgW6DbmWc")  # Replace with your actual sheet ID
    
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
            # Fetch data for CE and PE
            option_chainx = get_data_x()  # CE data
            option_chainy = get_data_y()  # PE data
            
            # Process numeric columns for CE and PE
            numeric_columns = ['strikePrice', 'delta', 'gamma', 'theta', 'vega']
            for col in numeric_columns:
                option_chainx[col] = pd.to_numeric(option_chainx[col], errors='coerce').fillna(0)
                option_chainy[col] = pd.to_numeric(option_chainy[col], errors='coerce').fillna(0)

            # Filter CE and PE based on delta conditions
            calls_filtered = option_chainx[
                (option_chainx['delta'] >= 0.05) & 
                (option_chainx['delta'] <= 0.6)
            ]
            puts_filtered = option_chainy[
                (option_chainy['delta'] >= -0.6) & 
                (option_chainy['delta'] <= -0.05)
            ]

           
                   # Summarize CE and PE data
            current_summary_ce = {
                "timestamp": pd.Timestamp.now(tz=timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                "name": "NIFTY",
                "price": None,  # Placeholder for LTP
                "delta_x_sum": calls_filtered['delta'].sum(),
                "gamma_x_sum": calls_filtered['gamma'].sum(),
                "theta_x_sum": calls_filtered['theta'].sum(),
                "vega_x_sum": calls_filtered['vega'].sum()
            }
            current_summary_pe = {
                "timestamp": pd.Timestamp.now(tz=timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M:%S"),
                "name": "NIFTY",
                "delta_y_sum": puts_filtered['delta'].sum(),
                "gamma_y_sum": puts_filtered['gamma'].sum(),
                "theta_y_sum": puts_filtered['theta'].sum(),
                "vega_y_sum": puts_filtered['vega'].sum()
            }

            # Calculate differences for CE and PE
            if previous_summary is None:
                ce_differences = {
                    "delta_diff_ce": 0,
                    "gamma_diff_ce": 0,
                    "theta_diff_ce": 0,
                    "vega_diff_ce": 0
                }
                pe_differences = {
                    "delta_diff_pe": 0,
                    "gamma_diff_pe": 0,
                    "theta_diff_pe": 0,
                    "vega_diff_pe": 0
                }
            else:
                ce_differences = {
                    "delta_diff_ce": float(current_summary_ce["delta_x_sum"]) - float(previous_summary["delta_x_sum"]),
                    "gamma_diff_ce": float(current_summary_ce["gamma_x_sum"]) - float(previous_summary["gamma_x_sum"]),
                    "theta_diff_ce": float(current_summary_ce["theta_x_sum"]) - float(previous_summary["theta_x_sum"]),
                    "vega_diff_ce": float(current_summary_ce["vega_x_sum"]) - float(previous_summary["vega_x_sum"])
                }
                pe_differences = {
                    "delta_diff_pe": float(current_summary_pe["delta_y_sum"]) - float(previous_summary["delta_y_sum"]),
                    "gamma_diff_pe": float(current_summary_pe["gamma_y_sum"]) - float(previous_summary["gamma_y_sum"]),
                    "theta_diff_pe": float(current_summary_pe["theta_y_sum"]) - float(previous_summary["theta_y_sum"]),
                    "vega_diff_pe": float(current_summary_pe["vega_y_sum"]) - float(previous_summary["vega_y_sum"])
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

            # Combine all data into a single row
            combined_differences = {
                "timestamp": current_summary_ce["timestamp"],
                "name": "NIFTY",
                "price": ltp,  # LTP for CE
                "delta_diff_ce": ce_differences["delta_diff_ce"],
                "gamma_diff_ce": ce_differences["gamma_diff_ce"],
                "theta_diff_ce": ce_differences["theta_diff_ce"],
                "vega_diff_ce": ce_differences["vega_diff_ce"],
                "delta_diff_pe": pe_differences["delta_diff_pe"],
                "gamma_diff_pe": pe_differences["gamma_diff_pe"],
                "theta_diff_pe": pe_differences["theta_diff_pe"],
                "vega_diff_pe": pe_differences["vega_diff_pe"]
            }

            # Append the new row to the difference_table
            if difference_table.empty:
                difference_table.loc[0] = combined_differences
            else:
                difference_table.loc[len(difference_table)] = combined_differences

            # Print the updated difference table
            print("\nDifference Table:")
            print(difference_table)

            # Write the difference_table to Sheet1
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
            # Inside the main loop, after calculating combined_differences:

            # Step 1: Create merged summary for Sheet2
            merged_summary = {
                "timestamp": current_summary_ce["timestamp"],
                "name": "NIFTY",
                "price": ltp,
                # CE metrics
                "delta_x_sum": current_summary_ce["delta_x_sum"],
                "gamma_x_sum": current_summary_ce["gamma_x_sum"],
                "theta_x_sum": current_summary_ce["theta_x_sum"],
                "vega_x_sum": current_summary_ce["vega_x_sum"],
                # PE metrics
                "delta_y_sum": current_summary_pe["delta_y_sum"],
                "gamma_y_sum": current_summary_pe["gamma_y_sum"],
                "theta_y_sum": current_summary_pe["theta_y_sum"],
                "vega_y_sum": current_summary_pe["vega_y_sum"]
            }

            # Step 2: Write merged_summary to Sheet2
            try:
                # Convert to list in column order
                summary_record = [
                    merged_summary["timestamp"],
                    merged_summary["name"],
                    merged_summary["price"],
                    merged_summary["delta_x_sum"],
                    merged_summary["gamma_x_sum"],
                    merged_summary["theta_x_sum"],
                    merged_summary["vega_x_sum"],
                    merged_summary["delta_y_sum"],
                    merged_summary["gamma_y_sum"],
                    merged_summary["theta_y_sum"],
                    merged_summary["vega_y_sum"]
                ]
                
                # Add headers if sheet is empty
                if not sheet2.get_all_values():
                    headers = list(merged_summary.keys())
                    sheet2.append_row(headers)
                
                # Append the latest summary
                sheet2.append_row(summary_record)
            except Exception as e:
                logger.error(f"Error writing to Sheet2: {e}")

            # Step 3: Update previous_summary correctly
            previous_summary = {
                **current_summary_ce,
                **current_summary_pe
            }

            # Wait for 60 seconds before the next iteration
            time.sleep(30)
      else:
        print("Outside trading hours.")
        sys.exit()
