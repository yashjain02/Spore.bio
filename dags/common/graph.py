import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

def fetch_data_from_database(engine):

    sql_query = """
        SELECT "barcode", "total_number_of_bacteria_measured_in_lab", "number_of_bacteria_pixels"
        FROM images_table
    """

    df = pd.read_sql_query(sql_query, engine)
    return df

def plot_bacteria_vs_surface(data):
    bacteria_per_membrane = data.groupby('membrane')['total_number_of_bacteria_measured_in_lab'].sum()
    surface_occupied_by_bacteria = data['number_of_bacteria_pixels'].mean()

    plt.figure(figsize=(10, 6))
    plt.scatter(bacteria_per_membrane, surface_occupied_by_bacteria, color='blue')
    plt.title('Number of Bacteria per Membrane vs Average Surface Occupied by Bacteria')
    plt.xlabel('Number of Bacteria per Membrane')
    plt.ylabel('Average Surface Occupied by Bacteria in pixels')
    plt.grid(True)
    plt.show()

engine = create_engine('postgresql://airflow:airflow@127.0.0.1:5432/spore')

data = fetch_data_from_database(engine)

plot_bacteria_vs_surface(data)
