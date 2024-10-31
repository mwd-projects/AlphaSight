import yfinance as yf
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt


# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect("D:/CodeTemp/MyData/stock_data.db")
cursor = conn.cursor()

# Dictionary to store data for each ticker
data_dict = {}


# Create the stocks table if it doesn't already exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stocks (
        date TEXT,
        ticker TEXT,
        open REAL,
        high REAL,
        low REAL,
        close REAL,
        adj_close REAL,
        volume INTEGER,
        PRIMARY KEY (date, ticker)
    )
''')
conn.commit()

# Define stock tickers to download and fetch stock data
dow_jones_tickers = [
    "AAPL", "MSFT", "JPM", "V", "PG", "JNJ", "UNH", "HD", "GS", "NKE",
    "DIS", "AXP", "IBM", "MRK", "TRV", "CVX", "BA", "WMT", "CAT", "MCD",
    "KO", "XOM", "INTC", "MMM", "WBA", "VZ", "CSCO", "DOW", "AMGN", "HON"
]

# Fetch and insert data for each ticker
for ticker in dow_jones_tickers:
    print(f"Fetching data for {ticker}...")
    
    # Fetch historical data for each ticker
    stock_data = yf.download(ticker, start="2024-01-01", end="2024-10-27")

    # Check if data is returned
    if not stock_data.empty:
        # Reset index and rename columns to match database schema
        stock_data = stock_data.reset_index()
        stock_data.columns = ["Date", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]  # Rename columns
        stock_data['Ticker'] = ticker  # Add ticker column

        # Convert Date column to string format to avoid Timestamp issues
        stock_data["Date"] = stock_data["Date"].astype(str)

        # Insert each row into the SQL table
        for row in stock_data.itertuples(index=False):
            cursor.execute('''
                INSERT OR REPLACE INTO stocks (date, ticker, open, high, low, close, adj_close, volume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (row.Date, row.Ticker, row.Open, row.High, row.Low, row.Close, row.Adj_Close, row.Volume))
        
        conn.commit()
    else:
        print(f"No data found for {ticker}.")

# Query and store Close prices for each ticker in data_dict
for ticker in dow_jones_tickers:
    query = f"SELECT date, close FROM stocks WHERE ticker = '{ticker}' AND date >= '2024-01-01' ORDER BY date"
    data = pd.read_sql(query, conn, parse_dates=['date'])
    data_dict[ticker] = data.set_index('date')['close']




percentage_differences = {}

for ticker in dow_jones_tickers:
    # Query for the first opening price and last closing price
    cursor.execute(f'''
        SELECT open FROM stocks WHERE ticker = '{ticker}' AND date >= '2024-01-01' ORDER BY date ASC LIMIT 1''')
    first_open = cursor.fetchone()[0]  # Get the first opening price

    cursor.execute(f'''
            SELECT close FROM stocks WHERE ticker = '{ticker}' AND date >= '2024-01-01' ORDER BY date DESC LIMIT 1''')
    last_close = cursor.fetchone()[0]  # Get the last closing price

    # Calculate the percentage difference
    if first_open and last_close:
        percentage_diff = ((last_close - first_open) / first_open) * 100
        percentage_differences[ticker] = percentage_diff

query = "SELECT * FROM stocks"
stock_data_df = pd.read_sql(query, conn)

conn.close()


results_df = pd.DataFrame(list(percentage_differences.items()), columns=["Ticker", "Percentage Difference"])
results_df = results_df.sort_values(by="Percentage Difference", ascending=False)

# Display the results
print("Biggest percentage differences between first open and last close for each company:")
print(results_df)

stock_data_df.to_excel("D:/CodeTemp/MyData/full_stock_data.xlsx", index=False)

print("Stock data has been written to 'D:/CodeTemp/MyData/full_stock_data.xlsx'")


# Initialize a larger plot for clarity
plt.figure(figsize=(14, 8))

# Plot each ticker’s Close price data with a label
for ticker, close_prices in data_dict.items():
    plt.plot(close_prices.index, close_prices, label=ticker)

# Customize the plot
plt.title("Closing Prices of Dow Jones 30 Stocks")
plt.xlabel("Date")
plt.ylabel("Price (USD)")
plt.legend(loc='upper left', bbox_to_anchor=(1.05, 1), ncol=2)  # Places legend outside the plot
plt.grid(True)

# Show the plot
plt.tight_layout()
plt.show()

