import yfinance as yf
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt

# Connect to SQLite database (or create it if it doesn't exist)
conn = sqlite3.connect("D:/CodeTemp/MyData/stock_data.db")
cursor = conn.cursor()

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
tickers = ["AAPL", "MSFT", "GOOGL"]
stock_data = yf.download(tickers, start="2023-01-01", end="2023-12-31")

# Unstack and reformat the DataFrame to have columns: Date, Ticker, Open, High, Low, Close, Adj_Close, Volume
stock_data = stock_data.stack(level=1).reset_index()
stock_data.columns = ["Date", "Ticker", "Open", "High", "Low", "Close", "Adj_Close", "Volume"]

# Convert Date column to string format
stock_data["Date"] = stock_data["Date"].astype(str)

# Insert data into the database
for row in stock_data.itertuples(index=False):
    cursor.execute('''
        INSERT OR REPLACE INTO stocks (date, ticker, open, high, low, close, adj_close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (row.Date, row.Ticker, row.Open, row.High, row.Low, row.Close, row.Adj_Close, row.Volume))

# Commit transaction to save changes
conn.commit()

# Query and display data for a specific ticker
ticker = "AAPL"
cursor.execute("SELECT * FROM stocks WHERE ticker = ?", (ticker,))
rows = cursor.fetchall()
for row in rows:
    print(row)


ticker = "AAPL"
query = f"SELECT date, open, close FROM stocks WHERE ticker = '{ticker}' ORDER BY date"
data = pd.read_sql(query, conn, parse_dates=['date'])

# Close the database connection
conn.close()

data = data.sort_values(by='date')

# Plot Open and Close prices
plt.figure(figsize=(12, 6))  # Set the figure size for clarity

# Plot Open prices
plt.plot(data['date'], data['open'], label='Open Price', linestyle='-', marker='', color='blue')

# Plot Close prices
plt.plot(data['date'], data['close'], label='Close Price', linestyle='-', marker='', color='green')

# Customize the plot
plt.title(f"Time Series of {ticker} Stock Prices")
plt.xlabel("Date")
plt.ylabel("Price (USD)")
plt.legend()
plt.grid(True)

# Show the plot
plt.show()
