# generate_mock_returns.py
import csv
import random
from datetime import datetime, timedelta
import argparse

def generate_mock_returns(start_date, end_date, file_name='returns.csv'):
    """
    Generate a CSV file with mock returns data.

    Args:
    - start_date (str): The start date in 'YYYY-MM-DD' format.
    - end_date (str): The end date in 'YYYY-MM-DD' format.
    - file_name (str): The name of the CSV file to be generated.
    """
    try:
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        
        if start_date > end_date:
            raise ValueError("Start date must be before end date")
        
        current_date = start_date
        data = []

        while current_date <= end_date:
            # Generate a random return value between -0.05 and 0.05
            daily_return = round(random.uniform(-0.05, 0.05), 6)
            data.append((current_date.strftime('%Y-%m-%d'), daily_return))
            current_date += timedelta(days=1)
        
        # Write data to CSV
        with open(file_name, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['date', 'returns'])
            writer.writerows(data)
        
        print(f"Mock returns data generated and saved to {file_name}")
    
    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Generate mock returns data.')
    parser.add_argument('start_date', type=str, help='Start date in YYYY-MM-DD format')
    parser.add_argument('end_date', type=str, help='End date in YYYY-MM-DD format')
    parser.add_argument('--file_name', type=str, default='returns.csv', help='CSV file name (default: returns.csv)')

    args = parser.parse_args()
    
    generate_mock_returns(args.start_date, args.end_date, args.file_name)

# use example in termainal: 'python generate_mock_returns.py 2024-01-01 2025-12-31'