import base64
import datetime
import sqlite3
import time
import geopandas as gpd
import requests


class FlightTracker:
    def __init__(self, boundary_file, db_file, auth_user, auth_pass):
        self.gdf = gpd.read_file(boundary_file)
        self.bbox = self.gdf.total_bounds
        self.conn = sqlite3.connect(db_file)
        self.c = self.conn.cursor()
        self.auth = (auth_user, auth_pass)
        self.prev_in_boundary = {}  # Dictionary to keep track of previously inside-boundary aircraft

    def get_aircraft_data(self):
        url = f"https://opensky-network.org/api/states/all?lamin={self.bbox[1]}&lomin={self.bbox[0]}&lamax={self.bbox[3]}&lomax={self.bbox[2]}"
        headers = {'Authorization': 'Basic ' + base64.b64encode(f"{self.auth[0]}:{self.auth[1]}".encode()).decode()}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve data: {response.text}")
            return
        try:
            data = response.json()
            if data is not None:
                for aircraft in data["states"]:
                    if aircraft[5] is not None and aircraft[6] is not None:
                        icao24 = aircraft[0]
                        callsign = aircraft[1]
                        lat = aircraft[6]
                        lon = aircraft[5]
                        altitude = aircraft[7]
                        point = gpd.points_from_xy([lon], [lat])
                        point_in_gdf = self.gdf.contains(point[0])
                        in_boundary = 1 if point_in_gdf.any() else 0  # Assign 1 if inside, 0 if outside
                        log_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        table_name = f"aircraft_{icao24}_"
                        self.c.execute(f"CREATE TABLE IF NOT EXISTS {table_name} "
                                       "(id INTEGER PRIMARY KEY AUTOINCREMENT, "
                                       "callsign TEXT, "
                                       "time TEXT, "
                                       "latitude REAL, "
                                       "longitude REAL, "
                                       "altitude REAL, "
                                       "icao24 TEXT, "
                                       "in_boundary INTEGER)")  # Add in_boundary column to table
                        self.c.execute(
                            f"INSERT INTO {table_name} (callsign, time, latitude, longitude, altitude, icao24, in_boundary) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?)", (callsign, log_time, lat, lon, altitude, icao24, in_boundary))
                        print(f"New data entered for: {icao24} | {callsign} at {log_time}")
                        self.conn.commit()
                        if icao24 in self.prev_in_boundary and in_boundary != self.prev_in_boundary[icao24]:
                            if in_boundary:
                                print(f"Entered shapefile boundary: {icao24} | {callsign} at {log_time}")
                            else:
                                print(f"Left shapefile boundary: {icao24} | {callsign} at {log_time}")
                        self.prev_in_boundary[icao24] = in_boundary
            elif "error" in data:
                print(f"Error: {data['error']}")
            else:
                print("No data returned from API")
        except Exception as e:
            print(f"Failed to retrieve data: {str(e)}")
    def run(self, interval=120):
        while True:
            self.get_aircraft_data()
            time.sleep(interval)


if __name__ == "__main__":
    tracker = FlightTracker("gadm41_UKR_shp/gadm41_UKR_0.shp", "aircraft_positions.db", "OPENSKYUSERNAME", "OPENSKYPASSWORD")
    tracker.run()
