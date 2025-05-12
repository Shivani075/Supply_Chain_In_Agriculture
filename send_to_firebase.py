import serial
import serial.tools.list_ports
import firebase_admin
from firebase_admin import credentials, firestore
import time

# Initialize Firebase Admin SDK
cred = credentials.Certificate('C:\\Users\\Admin\\My programs\\Major_project\\supply-chain-in-agricult-5bc11-firebase-adminsdk-fbsvc-10f2ea41c8.json')
firebase_admin.initialize_app(cred)
db = firestore.client()

def find_arduino_port():
    ports = list(serial.tools.list_ports.comports())
    for p in ports:
        print(f"Found port: {p.device} - {p.description}")
        if "USB Serial Device" in p.description or "Arduino" in p.description:
            return p.device
    return None

def connect_serial(port=None, baud_rate=9600, timeout=1, max_attempts=5):
    if port is None:
        port = find_arduino_port()
        if port is None:
            raise Exception("Serial port not found. Please check the connection.")

    for attempt in range(max_attempts):
        try:
            ser = serial.Serial(port, baud_rate, timeout=timeout)
            print(f"Successfully connected to {port}")
            return ser
        except serial.SerialException as e:
            print(f"Attempt {attempt + 1}/{max_attempts}: Failed to connect to {port}. Error: {e}")
            if attempt < max_attempts - 1:
                print("Retrying in 5 seconds...")
                time.sleep(5)
    raise Exception(f"Failed to connect to {port} after {max_attempts} attempts")

def send_data_to_firebase(data):
    try:
        # Assuming the data format is: timestamp, humidity, temperature, heat_index
        timestamp, humidity, temperature, heat_index = data[:4]
        
        data_to_send = {
            'timestamp': int(timestamp),
            'humidity': float(humidity),
            'temperature': float(temperature),
            'heat_index': float(heat_index),
            'firebase_timestamp': firestore.SERVER_TIMESTAMP
        }
        db.collection('real_time_data').add(data_to_send)
        print(f"Data sent to Firebase: Temp: {temperature}°C, Humidity: {humidity}%, Heat Index: {heat_index}°C")
    except Exception as e:
        print("Failed to send data:", e)

def main():
    try:
        ser = connect_serial()
        
        while True:
            try:
                if ser.in_waiting > 0:
                    line = ser.readline().decode('utf-8').strip()
                    print("Raw data received:", line)
                    data = line.split(',')
                    if len(data) >= 4:  # We expect at least 4 values
                        print("Processed data:", data[:4])  # Only print the first 4 values
                        send_data_to_firebase(data)
                    else:
                        print("Received incomplete data:", data)
            except serial.SerialException as e:
                print(f"Serial connection lost: {e}")
                print("Attempting to reconnect...")
                ser = connect_serial()
            except Exception as e:
                print("Error:", e)
    except KeyboardInterrupt:
        print("Program terminated by user")
    finally:
        if 'ser' in locals() and ser.is_open:
            ser.close()
            print("Serial port closed")

if __name__ == "__main__":
    main()