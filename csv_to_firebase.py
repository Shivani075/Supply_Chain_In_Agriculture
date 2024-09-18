import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from io import StringIO
import datetime

# Initialize Firebase (keep this part the same)
cred = credentials.Certificate('supply-chain-in-agriculture-firebase-adminsdk-zowmd-99626f5c62.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

# Load and process the CSV file
csv_file = 'C:/Users/Admin/My programs/Major_project/teraterm1.csv'

# Read the file content
with open(csv_file, 'r') as file:
    content = file.readlines()

# Find the start of the actual data
start_index = next(i for i, line in enumerate(content) if '120011476' in line)
data_content = content[start_index:]

# Create a list to store the processed data
processed_data = []

for line in data_content:
    parts = line.strip().split('] ', 1)
    if len(parts) == 2:
        timestamp, data = parts
        timestamp = timestamp[1:]  # Remove the leading '['
        processed_data.append([timestamp] + data.split(','))

# Create DataFrame
df = pd.DataFrame(processed_data, columns=['Timestamp', 'Date_and_Time', 'Humidity %', 'Temperature °C', 'Heat Index', 'Empty'])

# Drop unnecessary columns
df = df.drop(['Empty'], axis=1)

# Convert columns to appropriate types
df['Date_and_Time'] = pd.to_datetime(df['Date_and_Time'], unit='ms')
df['Humidity %'] = pd.to_numeric(df['Humidity %'], errors='coerce')
df['Temperature °C'] = pd.to_numeric(df['Temperature °C'], errors='coerce')
df['Heat Index'] = pd.to_numeric(df['Heat Index'], errors='coerce')

# Print the first few rows and column names to verify
print(df.head())
print(df.columns)

# Function to send each row of the CSV to Firestore and generate alert if needed
def send_to_firebase(row):
    collection_ref = db.collection('mango_transport_data')
    doc_ref = collection_ref.document()
    data_to_add = {
        'Timestamp': row['Timestamp'],
        'Date and Time': row['Date_and_Time'].isoformat(),
        'Temperature °C': row['Temperature °C'],
        'Humidity %': row['Humidity %'],
        'Heat Index': row['Heat Index']
    }
    doc_ref.set(data_to_add)
    print(f"Data sent to Firestore: {data_to_add}")
    
    # Check if temperature exceeds 30°C and generate alert
    if row['Temperature °C'] > 30:
        generate_alert(row)

def generate_alert(row):
    alerts_ref = db.collection('temperature_alerts')
    alert_doc = alerts_ref.document()
    alert_data = {
        'Timestamp': row['Timestamp'],
        'Date and Time': row['Date_and_Time'].isoformat(),
        'Temperature °C': row['Temperature °C'],
        'Alert Message': f"High temperature detected: {row['Temperature °C']}°C at {row['Date_and_Time']}",
        'Created At': datetime.datetime.now().isoformat()
    }
    alert_doc.set(alert_data)
    print(f"Temperature Alert Generated: {alert_data['Alert Message']}")

# Iterate over each row in the DataFrame and send it to Firebase
for index, row in df.iterrows():
    send_to_firebase(row)

print("CSV data transfer to Firebase completed!")