import pandas as pd

def sample_csv(path):
    n = 100  # every 1000th line = 0.1% of the lines
    df = pd.read_csv(path, header=0, skiprows=lambda i: i % n != 0)
    df.to_csv('sample_data_2.csv', encoding='utf-8', index=False)

    return None

sample_csv('C:/Users/BIE/Desktop/Python/MLA/MLA_2122/data/01_211203_TUDA_data.csv')