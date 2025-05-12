# import firebase_admin
# from firebase_admin import credentials, firestore
# import pandas as pd

# # Path to the CSV file
# file_path = r'C:/Users/Admin/My programs/Major_project/banana_quality.csv'

# # Load the CSV data
# banana_data = pd.read_csv(file_path)

# # Initialize the Firebase app
# cred = credentials.Certificate(r'supply-chain-in-agriculture-firebase-adminsdk-zowmd-99626f5c62.json')  # Replace with your Firebase Admin SDK JSON file
# firebase_admin.initialize_app(cred)

# # Initialize Firestore
# db = firestore.client()

# # Loop through the DataFrame and upload each row to Firestore
# for index, row in banana_data.iterrows():
#     doc_ref = db.collection('banana_quality').document()  # Create a new document
#     doc_ref.set({
#         'Size': row['Size'],
#         'Weight': row['Weight'],
#         'Sweetness': row['Sweetness'],
#         'Softness': row['Softness'],
#         'HarvestTime': row['HarvestTime'],
#         'Ripeness': row['Ripeness'],
#         'Acidity': row['Acidity'],
#         'Quality': row['Quality']
#     })

# print("Data uploaded successfully!")

import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import datetime

# Initialize Firebase
cred = credentials.Certificate('supply-chain-in-agricult-5bc11-firebase-adminsdk-fbsvc-10f2ea41c8.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# Load the CSV file
csv_file = 'C://Users//Admin//My programs//Major_project//Cleaned_banana.csv'
df = pd.read_csv(csv_file)

# Rename columns for consistency
df.columns = ['Timestamp', 'Humidity %', 'Temperature °C', 'Heat Index']

# Clean and process the data
df['Timestamp'] = pd.to_datetime(df['Timestamp'].astype(str).str.strip('[]'), errors='coerce')
numeric_columns = ['Humidity %', 'Temperature °C', 'Heat Index']
for col in numeric_columns:
    df[col] = pd.to_numeric(df[col], errors='coerce')

# Fill invalid timestamps with a default value
df['Timestamp'].fillna(pd.Timestamp('2024-10-18'), inplace=True)

# Batch writing function
def batch_write_to_firebase(df):
    try:
        batch = db.batch()
        collection_ref = db.collection('Banana_data_new')
        for index, row in df.iterrows():
            doc_ref = collection_ref.document()  # Create a new document
            data_to_add = {
                'Timestamp': row['Timestamp'].isoformat(),
                'Humidity %': row['Humidity %'],
                'Temperature °C': row['Temperature °C'],
                'Heat Index': row['Heat Index'],
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

# Alert generation function
def generate_alert(row):
    try:
        alerts_ref = db.collection('temperature_alerts')
        alert_doc = alerts_ref.document()
        alert_data = {
            'Timestamp': row['Timestamp'].isoformat(),
            'Humidity %': row['Humidity %'],
            'Temperature °C': row['Temperature °C'],
            'Alert Message': f"High temperature detected: {row['Temperature °C']}°C at {row['Timestamp']}",
            'Created At': datetime.datetime.now().isoformat(),
        }
        alert_doc.set(alert_data)
        print(f"Temperature Alert Generated: {alert_data['Alert Message']}")
    except Exception as e:
        print(f"Error generating alert: {e}")

# Generate alerts for high-temperature rows
def generate_alerts_for_high_temperatures(df):
    high_temp_rows = df[df['Temperature °C'] > 30]
    for _, row in high_temp_rows.iterrows():
        generate_alert(row)

# Use batch writing to send data to Firebase
batch_write_to_firebase(df)

# Generate alerts
generate_alerts_for_high_temperatures(df)

print("CSV data transfer and alert generation to Firebase completed!")
