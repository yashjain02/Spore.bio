import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

def fetch_data_from_database(engine):

    sql_query = """
        SELECT "membrane", "total_number_of_bacteria_measured_in_lab", "number_of_bacteria_pixels"
        FROM spore.membrane_image_camera
    """

    df = pd.read_sql_query(sql_query, engine)
    return df

def plot_bacteria_vs_surface(data):
    # we can also apply sum() windows function here aswell
    # Group by membrane and calculate the sum of number_of_bacteria_pixels and total_of_number_of_bacteria_measured_in_lab
    grouped_data = data.groupby('membrane').sum()
    # Calculate the average surface occupied by bacteria for each membrane
    grouped_data['average_surface'] = grouped_data['number_of_bacteria_pixels'] / grouped_data['total_number_of_bacteria_measured_in_lab']
    # Plot the data
    plt.figure(figsize=(10, 6))
    plt.scatter(grouped_data['average_surface'], grouped_data['number_of_bacteria_pixels'])
    plt.title('Number of Bacteria per Membrane vs. Average Surface Occupied by Bacteria')
    plt.xlabel('Average Surface Occupied by Bacteria')
    plt.ylabel('Number of Bacteria per Membrane')
    plt.grid(True)
    plt.show()


engine = create_engine('postgresql://airflow:airflow@127.0.0.1:5432/spore')

data = fetch_data_from_database(engine)

plot_bacteria_vs_surface(data)
