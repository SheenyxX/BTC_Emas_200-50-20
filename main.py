import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import json
from google.oauth2 import service_account
import pandas_gbq
import os

# --- Configuration ---
ticker = "BTC-USD"
# Fetch data for the last 10 years
end_date = datetime.now()
start_date = end_date - timedelta(days=10 * 365) # Using 365 days for a year for better accuracy

print(f"Fetching {ticker} data from {start_date.date()} to {end_date.date()}")

# --- BigQuery Configuration ---
# IMPORTANT: Save your service account JSON content into a file named 'ema-analyzer-key.json'
# in the same directory as your Python script, or provide the full path to it.
SERVICE_ACCOUNT_FILE = 'ema-analyzer-key.json'

# Initialize BigQuery related variables to None or default values
# This ensures they are defined even if the try block fails
credentials = None
project_id = None
dataset_id = "emas_signals" # This can be safely initialized here as it's a fixed string

if not os.path.exists(SERVICE_ACCOUNT_FILE):
    print(f"Error: Service account key file '{SERVICE_ACCOUNT_FILE}' not found.")
    print("Please save your service account JSON content into this file.")
    print("You can download it from Google Cloud Console > IAM & Admin > Service Accounts > Your Service Account > Keys > Add Key > Create new key (JSON).")
    exit() # Exit if the file itself is not found

try:
    credentials = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    project_id = credentials.project_id  # Project ID can be extracted directly from credentials

    # Optional: Verify dataset existence or create it (requires google-cloud-bigquery client)
    # This part is commented out by default, uncomment if you need it.
    # from google.cloud import bigquery
    # client = bigquery.Client(credentials=credentials, project=project_id)
    # try:
    #     client.get_dataset(f"{project_id}.{dataset_id}")
    #     print(f"BigQuery Dataset '{dataset_id}' exists.")
    # except Exception:
    #     print(f"BigQuery Dataset '{dataset_id}' does not exist. Attempting to create it...")
    #     dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    #     dataset.location = "US"  # Set your desired location
    #     client.create_dataset(dataset)
    #     print(f"BigQuery Dataset '{dataset_id}' created.")

    print(f"Authenticated successfully for Google Cloud Project: {project_id}")
    print(f"Data will be saved to BigQuery Dataset: {dataset_id}")

except Exception as e:
    print(f"Failed to load service account credentials or connect to BigQuery: {e}")
    print("Please ensure your service account JSON file is valid and accessible, and that it has sufficient BigQuery permissions.")
    # We set credentials and project_id to None to signal failure for the later BigQuery upload
    credentials = None
    project_id = None
    # We do NOT exit here directly anymore, so the rest of the script can still run
    # (e.g., data fetching, EMA calculations, local display), but BigQuery saving will be skipped.

# --- Data Fetching and EMA Calculation ---
try:
    df = yf.download(ticker, start=start_date, end=end_date, progress=False, auto_adjust=True)

    if df.empty:
        print("No data fetched. Check ticker or date range. Exiting.")
        exit()

    df['Close'] = df['Close'].astype(float)

    # Calculate EMAs
    df['EMA_20'] = df['Close'].ewm(span=20, adjust=False).mean()
    df['EMA_50'] = df['Close'].ewm(span=50, adjust=False).mean()
    df['EMA_200'] = df['Close'].ewm(span=200, adjust=False).mean()

    # Drop rows with NaN values resulting from EMA calculation (first 199 rows for EMA_200)
    df.dropna(inplace=True)

    # --- Crossover Detection ---

    # 20/50 EMA Crossover
    df['20_50_diff'] = df['EMA_20'] - df['EMA_50']
    df['20_50_bullish'] = (df['20_50_diff'].shift(1) < 0) & (df['20_50_diff'] > 0)
    df['20_50_bearish'] = (df['20_50_diff'].shift(1) > 0) & (df['20_50_diff'] < 0)

    # 50/200 EMA Crossover
    df['50_200_diff'] = df['EMA_50'] - df['EMA_200']
    df['50_200_golden'] = (df['50_200_diff'].shift(1) < 0) & (df['50_200_diff'] > 0)
    df['50_200_death'] = (df['50_200_diff'].shift(1) > 0) & (df['50_200_diff'] < 0)

    # --- Consolidate and Output Results ---
    all_crossovers = []

    # Collect 20/50 Bullish Crossovers
    bullish_20_50_crosses_df = df[df['20_50_bullish']]
    if not bullish_20_50_crosses_df.empty:
        for date, row in bullish_20_50_crosses_df.iterrows():
            all_crossovers.append({
                'date': date.date(),
                'type': 'BULLISH CROSS (20/50)',
                'price': row['Close'].item()
            })

    # Collect 20/50 Bearish Crossovers
    bearish_20_50_crosses_df = df[df['20_50_bearish']]
    if not bearish_20_50_crosses_df.empty:
        for date, row in bearish_20_50_crosses_df.iterrows():
            all_crossovers.append({
                'date': date.date(),
                'type': 'BEARISH CROSS (20/50)',
                'price': row['Close'].item()
            })

    # Collect 50/200 Golden Crosses
    golden_crosses_df = df[df['50_200_golden']]
    if not golden_crosses_df.empty:
        for date, row in golden_crosses_df.iterrows():
            all_crossovers.append({
                'date': date.date(),
                'type': 'GOLDEN CROSS (50/200)',
                'price': row['Close'].item()
            })

    # Collect 50/200 Death Crosses
    death_crosses_df = df[df['50_200_death']]
    if not death_crosses_df.empty:
        for date, row in death_crosses_df.iterrows():
            all_crossovers.append({
                'date': date.date(),
                'type': 'DEATH CROSS (50/200)',
                'price': row['Close'].item()
            })

    # Sort all collected crossovers by date (most recent to oldest)
    all_crossovers.sort(key=lambda x: x['date'], reverse=True)

    print("\n--- All Recent Crossovers (Most Recent First) ---")

    if all_crossovers:
        for crossover in all_crossovers:
            print(f"{crossover['date']} | {crossover['type']} | Price: ${crossover['price']:,.2f}")
    else:
        print("No crossovers found in the historical data.")

except Exception as e:
    print(f"An error occurred during data fetching or crossover detection: {e}")
    exit() # Exit if primary data fetching or EMA calculation fails

# Convert the list of crossovers to a DataFrame
crossover_df = pd.DataFrame(all_crossovers)

# If crossover_df is empty after processing, there's nothing more to do
if crossover_df.empty:
    print("\nNo crossovers found to process for interval analysis. Exiting.")
    exit()

# Rename columns to match your requested format
crossover_df.rename(columns={
    'date': 'Date',
    'type': 'Category',
    'price': 'Price'
}, inplace=True)

# --- Add Numerical Category Column ---
# Define the mapping dictionary for categories to numbers
category_to_num_map = {
    'BULLISH CROSS (20/50)': 1,
    'BEARISH CROSS (20/50)': 2,
    'GOLDEN CROSS (50/200)': 3,
    'DEATH CROSS (50/200)': 4
}
# Create the new 'Category_Num' column in crossover_df
crossover_df['Category_Num'] = crossover_df['Category'].map(category_to_num_map)

# Format price as integer for display (but keep as float internally for BigQuery upload)
display_df = crossover_df[['Date', 'Category', 'Price']].copy()
display_df['Price'] = display_df['Price'].apply(lambda x: f"${int(x):,}")



# Ensure the 'Date' column is in datetime format
crossover_df['Date'] = pd.to_datetime(crossover_df['Date'])

# Create a list to store interval DataFrames
interval_dfs = []

# For each category, calculate the time between consecutive events
for category in crossover_df['Category'].unique():
    # Filter for this category and sort by date
    category_df = crossover_df[crossover_df['Category'] == category].sort_values('Date')

    # Skip categories with only one event
    if len(category_df) <= 1:
        continue  # Skip to the next category

    # Calculate the days between consecutive events
    category_df['Days Between'] = category_df['Date'].diff().dt.days

    # Create a DataFrame for the intervals
    intervals = pd.DataFrame({
        'Category': category,
        'Category_Num': category_df['Category_Num'],
        'Previous Date': category_df['Date'].shift(1),
        'Current Date': category_df['Date'],
        'Days Between': category_df['Days Between']
    }).dropna(subset=['Days Between'])  # Explicitly drop NaNs only from 'Days Between'

    if not intervals.empty:
        interval_dfs.append(intervals)

# Initialize interval_df, summary_stats, and distribution_df as empty DataFrames
# in case no intervals are calculated. This prevents NameError later.
interval_df = pd.DataFrame()
summary_stats = pd.DataFrame()
distribution_df = pd.DataFrame()

# Combine all intervals if we have any
if interval_dfs:
    interval_df = pd.concat(interval_dfs, ignore_index=True)

    # Debugging: Print columns to ensure 'Days Between' and 'Category_Num' are present
    print("\nColumns available in interval_df:", interval_df.columns.tolist())

    # Format dates for display
    interval_df['Previous Date'] = interval_df['Previous Date'].dt.strftime('%Y-%m-%d')
    interval_df['Current Date'] = interval_df['Current Date'].dt.strftime('%Y-%m-%d')

    # Calculate summary statistics
    summary_stats = interval_df.groupby('Category')['Days Between'].agg(
        ['mean', 'median', 'min', 'max', 'std', 'count']
    ).reset_index()

    # Rename columns for clarity
    summary_stats.columns = [
        'Category',
        'Avg Days Between',
        'Median Days Between',
        'Min Days Between',
        'Max Days Between',
        'Std Dev Days',
        'Interval Count'
    ]

    # Format the statistics
    summary_stats['Avg Days Between'] = summary_stats['Avg Days Between'].round(1)
    summary_stats['Std Dev Days'] = summary_stats['Std Dev Days'].round(1)

    # Display the detailed intervals (including the new Category_Num)
    print("\nTime Between Consecutive Crossovers by Category:")
    print(interval_df[['Category', 'Category_Num', 'Previous Date', 'Current Date', 'Days Between']].to_string())



    # Define common bins for all categories (adjust as needed)
    bins = [0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650, 700, 750, 800, 850, 900, 950, 1000,
            float('inf')]
    labels = [f'{b}-{bins[i + 1] - 1}' for i, b in enumerate(bins[:-2])] + [f'{bins[-2]}+']

    # Create an empty dictionary to store the distribution data for each category
    distribution_data = {}

    for category in interval_df['Category'].unique():
        category_intervals = interval_df[interval_df['Category'] == category]['Days Between'].dropna()

        if not category_intervals.empty:
            # Calculate the histogram counts
            counts, _ = pd.cut(category_intervals, bins=bins, labels=labels, right=False,
                               include_lowest=True).value_counts(sort=False).align(pd.Series(0, index=labels),
                                                                                  fill_value=0)
            distribution_data[category] = counts
        else:
            distribution_data[category] = pd.Series(0, index=labels)

    # Convert the dictionary of distributions to a DataFrame
    distribution_df = pd.DataFrame(distribution_data)

    # Rename the index to 'Days Between Range'
    distribution_df.index.name = 'Days Between Range'

    # --- Sanitize column names for BigQuery ---
    # Create a mapping for invalid characters to underscores, then map problematic strings
    # to simpler, valid BigQuery column names.
    # Example: 'BULLISH CROSS (20/50)' -> 'BULLISH_CROSS_20_50'
    sanitized_columns = {}
    for col in distribution_df.columns:
        sanitized_name = col.replace(' ', '_').replace('(', '').replace(')', '').replace('/', '_').replace('-', '_')
        # Ensure no double underscores or leading/trailing underscores from replacements
        sanitized_name = '_'.join(filter(None, sanitized_name.split('_')))
        sanitized_columns[col] = sanitized_name.upper() # Convert to upper for consistency
    distribution_df.rename(columns=sanitized_columns, inplace=True)





# --- Saving DataFrames to Google BigQuery ---
print("\n--- Saving DataFrames to Google BigQuery ---")

# Only attempt BigQuery save if credentials and project_id were successfully loaded
if credentials and project_id:
    # Removed 'raw_ema_data' from the dictionary of DataFrames to save
    dataframes_to_save = {
        "crossover_summary": crossover_df.copy(),
        "crossover_intervals": interval_df.copy(), # Now interval_df is always a DataFrame (empty if no intervals)
        "crossover_interval_summary": summary_stats.copy(), # Now summary_stats is always a DataFrame
        "crossover_interval_distribution": distribution_df.copy(), # Now distribution_df is always a DataFrame
    }

    for table_name, dataframe in dataframes_to_save.items():
        if not dataframe.empty:
            # Convert datetime objects to date objects for BigQuery DATE type
            for col in dataframe.columns:
                if pd.api.types.is_datetime64_any_dtype(dataframe[col]):
                    dataframe[col] = dataframe[col].dt.date

            destination_table = f"{dataset_id}.{table_name}"
            try:
                print(f"Saving '{table_name}' to BigQuery table '{destination_table}'...")
                pandas_gbq.to_gbq(
                    dataframe,
                    destination_table,
                    project_id=project_id,
                    if_exists='replace',  # Options: 'fail', 'replace', 'append'
                    credentials=credentials
                )
                print(f"Successfully saved '{table_name}' to BigQuery.")
            except Exception as e:
                print(f"Error saving '{table_name}' to BigQuery: {e}")
        else:
            print(f"DataFrame '{table_name}' is empty, skipping BigQuery upload.")
else:
    print("\nSkipping BigQuery saving: Credentials or Project ID could not be loaded successfully earlier. Please check the initial BigQuery Configuration section for errors.")

print("\nBigQuery saving process complete.")
