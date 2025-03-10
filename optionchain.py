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
        "expirydate": "06MAR2025"
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
    # Embedded JSON content of the service account key
    credentials_json = """
    {
      "type": "service_account",
      "project_id": "gen-lang-client-0821497990",
      "private_key_id": "b7d00535dc2e437ace75f34b10242aa17270082a",
      "private_key": "-----BEGIN PRIVATE KEY-----\\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQCvU4SPSuhLFaWK\\nkdyuFLRiWtDExnQN6Yafm0sx4SyjRHsGrG5m6efVG97iLqRGPG+laDH8Plrvo6RI\\nQ5pEthy+LMXWkdhmWV10A1XYypThtX0nvwX7P+NzPUbaejPia9153nwqsTEDxOnJ\\n9xyX2SAhiK57VDNSVUga9wnkqgZgS+ETKknpWuSnTHGR155QfP2yCf/wchw8eJFg\\nix0pA7kN1tX+3J3HQ/rmOfOzPSp1v3vlinsguvZvrurD4PHMkLI7YUd+3pZnmMSk\\n81naSMPap3JhzffEpIJV08/GB+5FBx9095NEb0I/O57avPoiY3NUz4/jVrd57Zlj\\npUUZq+t5AgMBAAECggEAD0NIxrWpb8HoaZ4FlVUBmA9aXcr99ukVumbJPsQgv+zZ\\n/ex8ZvKlZ0C4ID4ZuHCR8pcVxOUDwxlel5jlAObOrUKWDXYgXdaZQ1x3+Hm4SMbl\\nKJThVyxKZ3GV7baWNjeYLgAPlKLcxrx5csbToyd4e9rbf6qGljwM3SYU4yZnDDJh\\n8OENfriMjgDVZpZBtc+6y2wUBt36Xkm+c3RAJAK8+WZmZVgCSLVziLq0ErEmLh//\\ni/cSsdHilcYNHoYLnrGmNP9M/VgMu1zkcEQM6pMTd20FKgyNCU6NZTyt5+AoPIYg\\n1B3XuVFIIIVyzdp5INh3OBRFKhV9bFDT2h1oQ81U4QKBgQDchAU+G65GJNd8jM81\\ntMJnspdJ6qtBPYUwEapIC1GFGLWPCuV6iNrdqhtGZ2K4VV0YK4qZb8IMndQrAUA3\\n4f/nsWHW2sNii/WfvLURCbEoGpdj/WCPt3Aqd6urBkTY1B3ej9ON2jKwpqM8tEFE\\nSSV3g8wzmggnJdCR2DxreOlZMQKBgQDLifOAxQu+lCnY8fpiZAB3ub8sZQPhbyU+\\nUoPVS4PI3edYwX1lpJ226OVf168ENsoUIIv2KEIXRzJ3Ih33QV4aR4Vp/l+mWaEc\\niBuXTLphrQm8kSdwBcY7aBkVO4NoJwxk4b7JyRfx/7pNbcgut3YXhbKdkaSl/M5D\\nH+MbPgokyQKBgGL81ImjzWBpa13aq611HguErMsej4+yuRXx6Bl9EzQG+oFip86Q\\nnocAtEuvXy5WC3stGIN2GoqlUreXUSeEyOZNxxi2jRywrSruf+1NB3x0K19UP0Nk\\nWfKGU8ZrAv6+gUYGFDZKK5UGyKIYXG/10d7LiB/l1iEUpYLCqaSo2z8RAoGBAJQr\\nl91tFLCnMZOiDDFmNUTzLm2GF/4bqFQnQ5uZvpUSnaDqMnw1Cy8leh18aQc7T365\\ndso64moJxX4ekwv1RSkCWeggascxxmx71QIetCv5CPaCAOC3A2kpzeC8E1xV2Nrz\\nz60bvFfKX3iQa8M+gTR8etvkM6U2VX1eEDk7v6ypAoGBAK1GI16WdDuPDNJA5+J5\\nMEk2xz/ADGNHOXsSHaU2fKZRnQNxrAseerPoYF2ew9xiwD42YXG+h2SlPXlAjdgJ\\n28ovjL9QuVDy/nqy3x7XEU5g+7nxqO9CETXU38HPkpx20xLGYxZgUlkGrjBoFZRp\\nJeACa3Vf/nAgV3WzMo9x5cm5\\n-----END PRIVATE KEY-----\\n",
      "client_email": "option-chain-service@gen-lang-client-0821497990.iam.gserviceaccount.com",
      "client_id": "108563374482304923418",
      "auth_uri": "https://accounts.google.com/o/oauth2/auth",
      "token_uri": "https://oauth2.googleapis.com/token",
      "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
      "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/option-chain-service%40gen-lang-client-0821497990.iam.gserviceaccount.com",
      "universe_domain": "googleapis.com"
    }
    """

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

    # Open the Google Sheet by its name
    spreadsheet = client.open("optionchain")  # Replace "optionchain" with your actual sheet name

    # Get references to both sheets
    sheet1 = spreadsheet.get_worksheet(2) # Sheet for difference_table
    sheet2 = spreadsheet.get_worksheet(3)  # Sheet for current_summary (index 1 refers to the second sheet)
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
