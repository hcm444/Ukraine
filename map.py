import sqlite3
import geopandas as gpd
import matplotlib.pyplot as plt


class FlightMap:
    def __init__(self, db_filename, shapefile):
        self.conn = sqlite3.connect(db_filename)
        self.gdf = gpd.read_file(shapefile)
        self.fig, self.ax = plt.subplots(figsize=(10, 10))

    def plot_routes(self):
        self.gdf.plot(ax=self.ax, edgecolor='black', facecolor='none')
        c = self.conn.cursor()
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'aircraft_%'")
        tables = c.fetchall()
        for table in tables:
            table_name = table[0]
            c.execute(f"SELECT latitude, longitude FROM {table_name}")
            rows = c.fetchall()
            if len(rows) > 1:
                lats, lons = zip(*rows)
                self.ax.plot(lons, lats, label=table_name)
        self.ax.legend(loc='upper left')
        self.ax.set_title('Flight Routes')

    def save(self, filename, dpi=300):
        plt.savefig(filename, dpi=dpi)


flight_map = FlightMap('aircraft_positions.db', 'gadm41_UKR_shp/gadm41_UKR_0.shp')
flight_map.plot_routes()
flight_map.save('map.png')
