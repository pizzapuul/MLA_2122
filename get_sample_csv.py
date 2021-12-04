
import pandas as pd

def sample_csv(data):
    n = 1000  # every 1000th line = 0.1% of the lines
    df = pd.read_csv(data, header=0, skiprows=lambda i: i % n != 0)
    df.to_csv('sample_data.csv', encoding='utf-8', index=False)

    return None

