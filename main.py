import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database configuration
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')

def process_stock_data(file_path):
    # Read the Excel file
    df = pd.read_excel(file_path)
    
    # Excel file has columns: Date, Open, High, Low, Close, Volume
    # Data cleaning
    df['Date'] = pd.to_datetime(df['Date']).dt.date  # Ensure Date is in the correct format
    
    return df

def process_weather_data(file_path):
    # Read the Excel file
    df = pd.read_excel(file_path)
    
    # Excel file has columns: Date, Temperature, Humidity, Precipitation
    # data cleaning and validation
    df['Date'] = pd.to_datetime(df['Date']).dt.date  # Ensure Date is in the correct format
    
    return df

def store_data(df, table_name):
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)
    cur = conn.cursor()

    # Create a list of column names
    columns = list(df.columns)

    # Create the SQL INSERT statement
    insert_stmt = sql.SQL("INSERT INTO {} ({}) VALUES ({}) ON CONFLICT (Date) DO UPDATE SET {}").format(
        sql.Identifier(table_name),
        sql.SQL(', ').join(map(sql.Identifier, columns)),
        sql.SQL(', ').join(sql.Placeholder() * len(columns)),
        sql.SQL(', ').join(sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(col), sql.Identifier(col)) for col in columns if col != 'Date')
    )

    # Convert DataFrame to list of tuples
    data = [tuple(x) for x in df.to_numpy()]

    # Execute the INSERT statement
    cur.executemany(insert_stmt, data)

    conn.commit()
    cur.close()
    conn.close()

def main():
    stock_file_path = 'path/to/stock_data.xlsx'
    weather_file_path = 'path/to/weather_data.xlsx'

    # Process and store stock data
    stock_df = process_stock_data(stock_file_path)
    store_data(stock_df, 'stock_prices')
    print("Stock data has been processed and stored.")

    # Process and store weather data
    weather_df = process_weather_data(weather_file_path)
    store_data(weather_df, 'weather_data')
    print("Weather data has been processed and stored.")

if __name__ == "__main__":
    main()
