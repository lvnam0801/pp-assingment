import pandas as pd
import numpy as np
df = pd.read_csv('./2-speed-disk.csv')
print(df.mean(axis=0))