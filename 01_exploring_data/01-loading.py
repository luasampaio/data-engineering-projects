# Load data from a CSV file into a DataFrame
import pandas as pd

df = pd.read_csv('_datasets/jobs.csv', low_memory=False) # low_memory=False para evitar warnings

print(df.head(3))  # Exibe as primeiras linhas do DataFrame


print(df.info())  # Exibe informações sobre o DataFrame
print(df.shape)  # Exibe o número de linhas e colunas do DataFrame


print(df.describe())  # Exibe estatísticas descritivas do DataFrame

