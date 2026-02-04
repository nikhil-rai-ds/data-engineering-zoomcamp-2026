import sys 
import pandas as pd 

df = pd.DataFrame({'days':[1,2], 'number':[87,92],'month':[8,9] } )
month = sys.argv[1]
df['month'] = sys.argv[1]
df.to_parquet(f'output_{month}.parquet')
print(df)