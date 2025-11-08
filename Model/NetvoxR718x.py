import json
import random
import math
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional

class NetvoxR718x:
    def __init__(self, 
                 sensor_id: str, 
                 fill_level_percent: int = 0,
                 temperature_c: float = 22.0,
                 battery_v: float = 3.6,
                 fill_threshold: int = 85,
                 #lat: float | None = None, #take out
                 #lng: float | None = None, #take out
                 enable_traffic = True,
                 
                 #DIURNAL TEMPERATURE VARIATION
                 temp_mean: float = 15.0, # Average daily temperature
                 temp_amplitude: float = 7.0, # Peak deviation from mean
                 temp_noise: float = 0.2, # Random noise factor
                 temp_peak_hour: int = 15, # Hour of peak temperature
                 tz_name: str = "Australia/Melbourne", #Timezone name

                 #Fill sensitivity
                 fill_sentivity: int = 3,
                 **kwargs):
        
        self.sensor_id = sensor_id
        self.fill_level_percent = int(fill_level_percent)
        self.temperature_c = float(temperature_c)
        self.battery_v = float(battery_v)
        self.fill_threshold = int(fill_threshold)
        # self.lat = lat #take out
        # self.lng = lng #take out

        #Current time reading
        self.timestamp = datetime.now(timezone.utc)

        #Event timestamps
        self.last_emptied: Optional[datetime] | None = None
        self.last_overflow: Optional[datetime] | None = None

        # Enable traffic-based fill simulation
        self.enable_traffic = enable_traffic

        #Diurnal temperature parameters
        self.temp_mean = temp_mean
        self.temp_amplitude = temp_amplitude
        self.temp_noise = temp_noise
        self.temp_peak_hour = temp_peak_hour
        self._tz = ZoneInfo(tz_name)

        #Simulate overflow
        self.overflow: bool = False
        self.overflow_count: int = 0

        #Bin personailities
        self.fill_sentivity = max(1, min(10, int(fill_sentivity))) # tweak fill sensitivity 1-10
        



    def to_dict(self) -> dict:
        return {
            "sensor_id": self.sensor_id,
            "timestamp": self.timestamp,
            "fill_level_percent": int(round(self.fill_level_percent)),
            "temperature_c": round(self.temperature_c, 1),
            "battery_v": round(self.battery_v, 3),
            "fill_threshold": self.fill_threshold,
            "last_emptied": self.last_emptied,
            "overflow": self.overflow,
            "overflow_count": self.overflow_count,
            "last_overflow": self.last_overflow
        }
        


    def print_json(self) -> None:
        print(json.dumps(self.to_dict(), indent=2))



    def simulate_changes(self, dt_minutes: int = 15):
        base_rate_per_hour = 3.0 # Base fill rate per hour
        avg_fill_change_per_hour = base_rate_per_hour * self.fill_sentivity
        noise_per_hour = 0.1 # Random noise factor per hour

        traffic = self.fill_traffic() if self.enable_traffic else 1.0
        delta_per_hour = (avg_fill_change_per_hour * traffic) + random.uniform(-noise_per_hour, noise_per_hour)
        delta = max(0.0, delta_per_hour * (dt_minutes / 60.0))

        #Work on proposed fill level first
        proposed = float(self.fill_level_percent) + float(delta)
        now = datetime.now(timezone.utc)

        if proposed >= 100.0:
            if not self.overflow:
                self.overflow = True
                self.overflow_count += 1
                self.last_overflow = now
            self.fill_level_percent = 100.0
        else:
            self.fill_level_percent = max(0.0, proposed)


        #Battery drain scaled by time
        battery_drain_per_hour = 5e-06
        tx_drain_per_report = 3e-07
        dv = battery_drain_per_hour * (dt_minutes / 60.0) + tx_drain_per_report * 1
        self.battery_v = round(max(2.8, self.battery_v -dv), 3)
        
        self.timestamp = now



    def empty_event(self, residue_percent: int = 0):
        self.fill_level_percent = max(0, residue_percent)
        self.last_emptied = datetime.now(timezone.utc)
        if getattr(self, 'overflow', False):
            self.overflow = False



    def attempt_empty_event(self, base_threshold: int = 85, empty_chance = 0.005) -> bool:
        if self.fill_level_percent >= base_threshold:
            overfill_factor = min((self.fill_level_percent - base_threshold) / 20, 1.0)
            prob = empty_chance + (overfill_factor * (1 - empty_chance))

            if random.random() < prob:
                self.fill_level_percent = 0
                self.last_emptied = datetime.now()
                self.overflow = False
            else:
                if self.fill_level_percent > 100:
                    self.fill_level_percent = min(self.fill_level_percent + random.uniform(0,5), 120)
                    self.overflow = True
        

    def update_temperature(self):
        """Simulate temperature based on time of day."""
        now = datetime.now(self._tz)
        hour = now.hour + now.minute / 60.0 + now.second / 3600.0
        angle = 2 * math.pi * (hour - self.temp_peak_hour) / 24.0 + math.pi / 2
        base = self.temp_mean + self.temp_amplitude * math.sin(angle)

        jitter = random.uniform(-self.temp_noise, self.temp_noise)
        self.temperature_c = round(base + jitter, 2)

    def fill_traffic(self) -> float:
        # Simulate fills based on peak traffic hours
        if not self.enable_traffic:
            return 1.0
        hour = datetime.now(self._tz).hour
        profile = [
            0.6, 0.55, 0.5, 0.5, 0.55, 0.6, # midnight to 5am
            0.75, 0.9, 1.05, 1.15, 1.25, 1.35, # 6am to 11am
            1.4, 1.4, 1.3, 1.15, 1.0, 0.9, # noon to 5pm
            0.8, 0.75, 0.7, 0.65, 0.6, 0.6 # 6pm to 11pm
            ]
            
        return profile[hour]


"""
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
            
"""