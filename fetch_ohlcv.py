import ccxt
import pandas as pd
import datetime
import os
import json
from google.oauth2 import service_account
import pandas_gbq

# --- BigQuery Configuration ---
# IMPORTANT: Save your service account JSON content into a file named 'ema-analyzer-key.json'
# in the same directory as your Python script, or provide the full path to it.
SERVICE_ACCOUNT_FILE = 'ema-analyzer-key.json'
BIGQUERY_DATASET_ID = "emas_signals" # Your BigQuery dataset

# Initialize BigQuery related variables to None
bq_credentials = None
bq_project_id = None

# Attempt to load BigQuery credentials
if not os.path.exists(SERVICE_ACCOUNT_FILE):
    print(f"Error: Service account key file '{SERVICE_ACCOUNT_FILE}' not found.")
    print("Please save your service account JSON content into this file.")
    print("You can download it from Google Cloud Console > IAM & Admin > Service Accounts > Your Service Account > Keys > Add Key > Create new key (JSON).")
    # We will not exit here, but set credentials to None so BigQuery upload is skipped
else:
    try:
        bq_credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
        bq_project_id = bq_credentials.project_id
        print(f"Authenticated successfully for Google Cloud Project: {bq_project_id}")
        print(f"Data will be saved to BigQuery Dataset: {BIGQUERY_DATASET_ID}")

    except Exception as e:
        print(f"Failed to load service account credentials for BigQuery: {e}")
        print("Please ensure your service account JSON file is valid and accessible, and has sufficient BigQuery permissions.")
        bq_credentials = None
        bq_project_id = None

def fetch_btc_ohlcv_with_emas(exchange_id='binance', symbol='BTC/USDT', timeframe='1d', since_days=365*3):
    """
    Fetches historical OHLCV data for BTC/USDT, calculates EMAs, and returns a Pandas DataFrame.

    Args:
        exchange_id (str): The ID of the cryptocurrency exchange (e.g., 'binance', 'kraken').
        symbol (str): The trading pair (e.g., 'BTC/USDT', 'BTC/USD').
        timeframe (str): The OHLCV timeframe (e.g., '1d' for daily, '1h' for hourly).
        since_days (int): Number of days of historical data to fetch.

    Returns:
        pd.DataFrame: A DataFrame containing OHLCV data and calculated EMAs.
                      Returns an empty DataFrame if data fetching fails.
    """
    try:
        # Initialize the exchange
        exchange = getattr(ccxt, exchange_id)()
        exchange.load_markets()

        # Calculate milliseconds for 'since' parameter
        # Fetching data up to today
        end_timestamp_ms = exchange.parse8601(datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%fZ'))
        start_timestamp_ms = end_timestamp_ms - (since_days * 24 * 60 * 60 * 1000)

        # Ensure start_timestamp_ms is not too far in the past for exchanges with data limits
        # (e.g., some exchanges might only provide a few years of daily data)
        # It's good practice to fetch in chunks if you need a very long history.
        # For simplicity, we'll try to fetch all at once, `ccxt` handles pagination internally for `fetch_ohlcv` up to its limits.

        print(f"Fetching {symbol} {timeframe} data from {exchange_id}...")
        print(f"Approx. start date: {datetime.datetime.fromtimestamp(start_timestamp_ms / 1000).strftime('%Y-%m-%d')}")

        all_ohlcv = []
        current_since = start_timestamp_ms # Start fetching from this timestamp

        while True:
            # Some exchanges require `since` to be the start of the desired period.
            # Others take `since` as the point to continue from.
            # ccxt's fetch_ohlcv generally handles pagination.
            ohlcv_chunk = exchange.fetch_ohlcv(symbol, timeframe, since=current_since, limit=1000)
            if not ohlcv_chunk:
                break
            all_ohlcv.extend(ohlcv_chunk)
            
            # Update current_since to the timestamp of the last fetched candle + 1ms
            # to get the next chunk.
            current_since = ohlcv_chunk[-1][0] + 1

            # Stop if we've fetched data up to the recent past or no more data is returned
            if current_since > end_timestamp_ms or len(ohlcv_chunk) < 1000:
                break
        
        if not all_ohlcv:
            print("No data fetched.")
            return pd.DataFrame()

        # Create a Pandas DataFrame
        df = pd.DataFrame(all_ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])

        # Convert timestamp to datetime and set as index
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('datetime', inplace=True)
        df.drop('timestamp', axis=1, inplace=True) # Remove original timestamp column

        # Sort by date to ensure correct EMA calculation
        df.sort_index(ascending=True, inplace=True)

        # Calculate EMAs
        # adjust=False parameter ensures standard EMA calculation
        df['EMA_20'] = df['close'].ewm(span=20, adjust=False).mean()
        df['EMA_50'] = df['close'].ewm(span=50, adjust=False).mean()
        df['EMA_200'] = df['close'].ewm(span=200, adjust=False).mean()

        # Drop rows with NaN values resulting from EMA calculation (first 199 rows for EMA_200)
        df.dropna(inplace=True)

        print("Data fetching and EMA calculation complete.")
        return df

    except ccxt.NetworkError as e:
        print(f"Network error: {e}")
        return pd.DataFrame()
    except ccxt.ExchangeError as e:
        print(f"Exchange error: {e}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()

# --- Main execution block ---
if __name__ == '__main__':
    # --- Configuration for data fetching ---
    exchange_to_use = 'binance'  # e.g., 'binance', 'kraken', 'coinbasepro'
    trading_symbol = 'BTC/USDT'  # or 'BTC/USD' depending on the exchange
    data_timeframe = '1d'        # '1d' for daily, '1h' for hourly, etc.
    historical_days = 365 * 10    # Fetch 5 years of daily data

    # Fetch data and calculate EMAs
    btc_data_df = fetch_btc_ohlcv_with_emas(exchange_to_use, trading_symbol, data_timeframe, historical_days)

    if not btc_data_df.empty:
        print("\nDataFrame Head:")
        print(btc_data_df.head())
        print("\nDataFrame Info:")
        btc_data_df.info()

        # --- Saving DataFrame to Google BigQuery ---
        print("\n--- Saving DataFrame to Google BigQuery ---")

        # Only attempt BigQuery save if credentials and project_id were successfully loaded
        if bq_credentials and bq_project_id:
            table_name = "raw_ohlcv_emas" # Name of the table in BigQuery

            # Convert datetime index to a column for BigQuery upload
            # BigQuery doesn't directly support pandas datetime index, prefer a regular column
            df_to_upload = btc_data_df.reset_index()
            df_to_upload.rename(columns={'datetime': 'date'}, inplace=True)

            # Ensure 'date' column is in datetime format before converting to date
            df_to_upload['date'] = pd.to_datetime(df_to_upload['date'])

            # Convert datetime objects to date objects for BigQuery DATE type
            # If you want TIMESTAMP, leave as datetime.
            df_to_upload['date'] = df_to_upload['date'].dt.date

            destination_table = f"{BIGQUERY_DATASET_ID}.{table_name}"
            try:
                print(f"Saving data to BigQuery table '{destination_table}'...")
                pandas_gbq.to_gbq(
                    df_to_upload,
                    destination_table,
                    project_id=bq_project_id,
                    if_exists='replace',  # Options: 'fail', 'replace', 'append'
                    credentials=bq_credentials
                )
                print(f"Successfully saved data to BigQuery.")
            except Exception as e:
                print(f"Error saving data to BigQuery: {e}")
        else:
            print("\nSkipping BigQuery saving: Credentials or Project ID could not be loaded successfully. Please check the 'BigQuery Configuration' section for errors.")

    else:
        print("Failed to retrieve data, skipping BigQuery upload.")

    print("\nScript execution complete.")
