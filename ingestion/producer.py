import fastf1
import json
import time
from kafka import KafkaProducer

fastf1.Cache.enable_cache("cache")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_lap_data():
    session = fastf1.get_session(2026, 1, 'FP3')  # Bahrain 2026 Race
    session.load()

    laps = session.laps[['Driver', 'LapNumber', 'LapTime', 
                         'Sector1Time', 'Sector2Time', 'Sector3Time', 
                         'Compound']]

    laps = laps.dropna()

    return laps.to_dict(orient='records')

def stream_data():
    laps = fetch_lap_data()

    for lap in laps:
        # Convert timedelta to string
        lap['LapTime'] = str(lap['LapTime'])
        lap['Sector1Time'] = str(lap['Sector1Time'])
        lap['Sector2Time'] = str(lap['Sector2Time'])
        lap['Sector3Time'] = str(lap['Sector3Time'])

        producer.send("f1_lap_times", lap)
        print("Sent:", lap['Driver'], lap['LapNumber'], lap['LapTime'])
        time.sleep(1)

if __name__ == "__main__":
    stream_data()