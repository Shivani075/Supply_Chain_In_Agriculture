# import pandas as pd
# import firebase_admin
# from firebase_admin import credentials, firestore
# from io import StringIO
# import datetime

# # Initialize Firebase (keep this part the same)
# cred = credentials.Certificate('supply-chain-in-agriculture-firebase-adminsdk-zowmd-99626f5c62.json')
# #This initializes the Firebase app with the service account credentials.
# firebase_admin.initialize_app(cred)
# # Initializes a Firestore client, allowing access to Firestore for CRUD operations.
# db = firestore.client()

# # Load and process the CSV file
# csv_file = 'C://Users//Admin//My programs//Major_project//Raja_Puri.csv'

# # Read the file content
# with open(csv_file, 'r') as file:
#     content = file.readlines()

# # Find the start of the actual data
# start_index = next(i for i, line in enumerate(content) if '120011476' in line)
# #Extracts the relevant data from the CSV starting at start_index
# data_content = content[start_index:]

# # Create a list to store the processed data
# #This loop processes each line, splitting it into a timestamp and a list of data values (humidity, temperature, etc.). The processed data is stored in the list processed_data
# processed_data = []

# for line in data_content:
#     parts = line.strip().split('] ', 1)
#     if len(parts) == 2:
#         timestamp, data = parts
#         timestamp = timestamp[1:]  # Remove the leading '['
#         processed_data.append([timestamp] + data.split(','))

# # Create DataFrame
# #A Pandas DataFrame is created using the processed_data list. Each element in the list becomes a row, and the columns are named accordingly 
# df = pd.DataFrame(processed_data, columns=['Timestamp', 'Humidity %', 'Temperature °C', 'Heat Index', 'Empty'])

# # Drop unnecessary columns
# df = df.drop(['Empty'], axis=1)

# # Convert columns to appropriate types
# df['Date_and_Time'] = pd.to_datetime(df['Date_and_Time'], unit='ms')
# df['Humidity %'] = pd.to_numeric(df['Humidity %'], errors='coerce')
# #pd.to_numeric: Converts the columns  to numeric data types. If conversion fails, NaN values are inserted
# df['Temperature °C'] = pd.to_numeric(df['Temperature °C'], errors='coerce')
# df['Heat Index'] = pd.to_numeric(df['Heat Index'], errors='coerce')

# # Print the first few rows and column names to verify
# print(df.head())
# print(df.columns)

# # Function to send each row of the CSV to Firestore and generate alert if needed
# def send_to_firebase(row):
#  #specifies the collection (or table) in Firestore where the data will be stored.
#     collection_ref = db.collection('mango_transport_data_new')
# #collection_ref.document(): Creates a new document in the collection
#     doc_ref = collection_ref.document()
#     data_to_add = {
#         'Timestamp': row['Timestamp'],
# #row['Date_and_Time'].isoformat(): Converts the datetime object into ISO format (e.g., 2024-09-19T12:34:56) before sending it to Firestore
#         'Date and Time': row['Date_and_Time'].isoformat(),
#         'Temperature °C': row['Temperature °C'],
#         'Humidity %': row['Humidity %'],
#         'Heat Index': row['Heat Index']
#     }
#  #Sends the dictionary data_to_add to Firestore, storing the row's data as a new document.
#     doc_ref.set(data_to_add)
#     print(f"Data sent to Firestore: {data_to_add}")
    
#     # Check if temperature exceeds 30°C and generate alert
#     if row['Temperature °C'] > 30:
#         generate_alert(row)

# def generate_alert(row):
#     alerts_ref = db.collection('temperature_alerts')
#     alert_doc = alerts_ref.document()
#     alert_data = {
#         'Timestamp': row['Timestamp'],
#         'Date and Time': row['Date_and_Time'].isoformat(),
#         'Temperature °C': row['Temperature °C'],
#         'Alert Message': f"High temperature detected: {row['Temperature °C']}°C at {row['Date_and_Time']}",
#         'Created At': datetime.datetime.now().isoformat()
#     }
#  # alert_doc.set(alert_data): Sends the alert details (like the timestamp, temperature, and a custom alert message) to Firestore.
#     alert_doc.set(alert_data)
#     print(f"Temperature Alert Generated: {alert_data['Alert Message']}")

# # Iterate over each row in the DataFrame and send it to Firebase
# # df.iterrows(): This loop iterates over each row in the DataFrame, sending each row's data to Firestore by calling send_to_firebase(row)
# for index, row in df.iterrows():
#     send_to_firebase(row)

# print("CSV data transfer to Firebase completed!")

import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import datetime

# Initialize Firebase
cred = credentials.Certificate('supply-chain-in-agricult-5bc11-firebase-adminsdk-fbsvc-10f2ea41c8.json')  # Update with your Firebase credentials path
firebase_admin.initialize_app(cred)
db = firestore.client()

# Load the CSV file
csv_file = 'C://Users//Admin//My programs//Major_project//Cleaned_Tera1.csv'  # Update with the path to your CSV file
df = pd.read_csv(csv_file)

# Rename columns for consistency
df.columns = ['Timestamp', 'Humidity %', 'Temperature °C', 'Heat Index']

# Remove brackets from the Timestamp column
df['Timestamp'] = df['Timestamp'].str.strip('[]')

# Convert data types
df['Timestamp'] = pd.to_datetime(df['Timestamp'], errors='coerce')
df['Humidity %'] = pd.to_numeric(df['Humidity %'], errors='coerce')
df['Temperature °C'] = pd.to_numeric(df['Temperature °C'], errors='coerce')
df['Heat Index'] = pd.to_numeric(df['Heat Index'], errors='coerce')

# Handle invalid timestamps (NaT)
if df['Timestamp'].isna().any():
    print("Invalid timestamps found. Handling them...")
    # Option 1: Fill with a default value (e.g., '2024-05-21')
    df['Timestamp'].fillna(pd.Timestamp('2024-05-21'), inplace=True)

# Verify the conversion
print("Sample data after processing:")
print(df.head())

# Batch writing function
def batch_write_to_firebase(df):
    try:
        batch = db.batch()
        collection_ref = db.collection('mango_mix_new')
        for index, row in df.iterrows():
            doc_ref = collection_ref.document()  # Create a new document
            data_to_add = {
                'Timestamp': row['Timestamp'].isoformat(),
                'Humidity %': row['Humidity %'],
                'Temperature °C': row['Temperature °C'],
                'Heat Index': row['Heat Index'],
                'Mango Name': 'Alphonso'
            }
            batch.set(doc_ref, data_to_add)
            print(f"Queued data for batch: {data_to_add}")

            # Commit batch every 500 writes
            if (index + 1) % 500 == 0:
                batch.commit()
                print(f"Batch of 500 committed at index {index + 1}.")
                batch = db.batch()  # Start a new batch

        # Commit any remaining writes in the batch
        batch.commit()
        print("Final batch committed!")
    except Exception as e:
        print(f"Error during batch write: {e}")

# Use batch writing to send data to Firebase
batch_write_to_firebase(df)

print("CSV data transfer to Firebase completed!")
