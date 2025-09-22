import json
import random
import time
from pathlib import Path
from datetime import datetime, timezone

class NetvoxR718x:
    def __init__(self, 
                 sensor_id: str, 
                 fill_level_percent: int = 0,
                 temperature_c: float = 22.0,
                 battery_v: float = 3.6,
                 fill_threshold: int = 85,
                 lat: float | None = None,
                 lng: float | None = None):
        self.sensor_id = sensor_id
        self.fill_level_percent = int(fill_level_percent)
        self.temperature_c = float(temperature_c)
        self.battery_v = float(battery_v)
        self.fill_threshold = int(fill_threshold)
        self.lat = lat
        self.lng = lng
        self.timestamp = datetime.now(timezone.utc)

    def to_dict(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "timestamp": self.timestamp.isoformat(),
            "lat": self.lat,
            "lng": self.lng,
            "fill_level_percent": self.fill_level_percent,
            "temperature_c": self.temperature_c,
            "battery_v": self.battery_v,
            "fill_threshold": self.fill_threshold
        }
        
    def print_json(self) -> None:
        print(json.dumps(self.to_dict(), indent=2))

    def simulate_changes(self, fill_change: int | None = None, 
                         temp_change: float | None = None, 
                         battery_change: float | None = None):
        if fill_change is None:
            fill_change = random.randint(0, 2)
        if temp_change is None:
            temp_change = random.uniform(-0.1, 0.2)
        if battery_change is None:
            battery_change = random.uniform(-0.001, -0.002)
        
        self.fill_level_percent = max(0, min(100, self.fill_level_percent + int(fill_change)))
        self.temperature_c = round(self.temperature_c + temp_change, 1)
        self.battery_v = round(max(0.0, self.battery_v + battery_change), 3)
        self.timestamp = datetime.now(timezone.utc)


#TESTING OBJECT
if __name__ == "__main__":
    s = NetvoxR718x(
        sensor_id="R718X-001",
        fill_level_percent=50,
        temperature_c=25.0,
        battery_v=3.18,
        fill_threshold=85,
        lat=-37.7932, 
        lng=144.8990
    )
    
    s.print_json()

    while True:
        try:
            s.simulate_changes()
            s.print_json()
            time.sleep(30)
        except KeyboardInterrupt:
            print("Simulation stopped.")
            break