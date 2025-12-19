import ephem
import requests
import datetime
import math

class SkyEngine:
    def __init__(self):
        # Gold Coast Coordinates
        self.lat = '-28.0167'
        self.lon = '153.4000'
        self.observer = ephem.Observer()
        self.observer.lat = self.lat
        self.observer.lon = self.lon
        self.observer.elevation = 10 

    def _get_holiday_mode(self):
        """Returns the active holiday mode based on Gold Coast date."""
        now = datetime.datetime.now()
        month = now.month
        day = now.day
        year = now.year

        # --- HOLIDAY CALENDAR ---
        
        # 1. New Year's Eve / Day
        if (month == 12 and day == 31) or (month == 1 and day == 1): return "nye"

        # 2. Australia Day
        if month == 1 and day == 26: return "australia_day"

        # 3. Valentine's Day
        if month == 2 and day == 14: return "valentines"

        # 4. St Patrick's Day
        if month == 3 and day == 17: return "st_patricks"

        # 5. Halloween
        if month == 10 and day == 31: return "halloween"

        # 6. Christmas Season (Dec 1 - Dec 26) - Extended for maximum festive vibes
        if month == 12 and 1 <= day <= 26: return "christmas"

        # 7. Gold Coast Show (Last Friday in August)
        if month == 8:
            last_day_aug = datetime.date(year, 8, 31)
            offset = (last_day_aug.weekday() - 4) % 7
            if day == (31 - offset): return "gc_show"

        # 8. Thanksgiving (4th Thurs Nov)
        if month == 11:
            first_nov = datetime.date(year, 11, 1)
            offset = (3 - first_nov.weekday()) % 7 
            if day == (1 + offset + 21): return "thanksgiving"

        return "normal"

    def get_environment_data(self, lat=None, lon=None):
        # Use User Location if provided, else default to Gold Coast
        target_lat = lat if lat else self.lat
        target_lon = lon if lon else self.lon

        # Update Observer
        self.observer.lat = str(target_lat)
        self.observer.lon = str(target_lon)
        self.observer.date = datetime.datetime.now(datetime.timezone.utc)

        # Astronomy
        sun = ephem.Sun(self.observer)
        moon = ephem.Moon(self.observer)
        is_day = sun.alt > (-6 * ephem.degree)
        
        # Moon Phase Icon
        moon_phase = moon.phase
        if moon_phase < 5: moon_icon = 0
        elif moon_phase < 45: moon_icon = 1
        elif moon_phase < 55: moon_icon = 2
        elif moon_phase < 95: moon_icon = 3
        else: moon_icon = 4

        # Weather API
        weather_desc = "Clear"
        temp = 25
        cloud_cover = 0
        
        try:
            url = f"https://api.open-meteo.com/v1/forecast?latitude={target_lat}&longitude={target_lon}&current=temperature_2m,weather_code,cloud_cover&timezone=auto"
            r = requests.get(url, timeout=1)
            if r.status_code == 200:
                d = r.json().get('current', {})
                temp = d.get('temperature_2m', 25)
                cloud_cover = d.get('cloud_cover', 0)
                w_code = d.get('weather_code', 0)
                w_map = {0:"Clear", 1:"Mainly Clear", 2:"Partly Cloudy", 3:"Overcast", 45:"Fog", 51:"Drizzle", 61:"Rain", 80:"Showers", 95:"Storms"}
                weather_desc = w_map.get(w_code, "Clear")
        except: pass

        # Star Visibility
        moon_factor = 1.0 - (moon_phase / 200.0)
        cloud_factor = 1.0 - (cloud_cover / 100.0)
        star_visibility = max(0, moon_factor * cloud_factor)

        return {
            "is_day": bool(is_day),
            "temp": round(temp),
            "condition": weather_desc,
            "moon_phase": round(moon_phase),
            "moon_icon_code": moon_icon,
            "star_visibility": round(star_visibility, 2),
            "holiday": self._get_holiday_mode()
        }