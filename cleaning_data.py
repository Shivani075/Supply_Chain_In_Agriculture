import pandas as pd
import re

# Define the input and output file paths
input_file_path = r'C://Users\Admin//My programs//Major_project//Tera3.csv'  # Replace with your file name
output_file_path = 'Cleaned_Tera3.csv'

try:
    # Load the CSV file with proper encoding
    data = pd.read_csv(input_file_path, encoding='ISO-8859-1')

    # Clean the "Date-and-Time" column to retain only the content within brackets
    if 'Date-and-Time' in data.columns:
        data['Date-and-Time'] = data['Date-and-Time'].apply(
            lambda x: re.search(r'\[.*?\]', str(x)).group() if re.search(r'\[.*?\]', str(x)) else x
        )
    else:
        print("Column 'Date-and-Time' not found in the dataset. Please check your file.")

    # Save the cleaned data to a new CSV file
    data.to_csv(output_file_path, index=False)
    print(f"Cleaned data has been saved to {output_file_path}")

except Exception as e:
    print(f"An error occurred: {e}")
