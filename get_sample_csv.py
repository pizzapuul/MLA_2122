import pandas as pd

def sample_csv(path):
    n = 1000  # every 1000th line = 0.1% of the lines
    df = pd.read_csv(path, header=0, skiprows=lambda i: i % n != 0)
    df.to_csv('sample_data.csv', encoding='utf-8', index=False)

    return None

#sample_csv('C:/Users/BIE/Desktop/Python/MLA/MLA_2122/data/***.csv')