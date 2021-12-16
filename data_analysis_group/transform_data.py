
import pandas as pd
import numpy as np
import datetime

list_df = []

date_start = '2020-01-01'

dateparse = lambda x: datetime.datetime.strptime(x, '%Y-%m-%d')

for number_dataset in [0,1,2]:
  path = './sales_model_{number_dataset}.csv'.format(number_dataset=number_dataset)
  df_current = pd.read_csv(path,parse_dates=['date'], date_parser=dateparse)
  list_df.append(df_current[df_current['date']>=date_start])

df_full = pd.concat(list_df,ignore_index=True)

# Перевод веса в килограммы
df_full['quantity'] = df_full['quantity']/1000

df_full['proceeds'] = df_full['quantity']*df_full['price']

df_full['week_number'] = df_full['date'].apply(lambda x: x.strftime("%V"))

df_group = df_full.groupby(by=['model','week_number','retailer','kind_cheese'],as_index=False).agg({'quantity': ['sum'], 
                                                                                     'proceeds': ['sum','size']})

df_group.columns = ['model', 'week_number','retailer','kind_cheese','total_quantity','total_proceeds','total_amount']

df_group['total_quantity'] = df_group['total_quantity'].apply(lambda x: round(x,2))
df_group['total_proceeds'] = df_group['total_proceeds'].apply(lambda x: round(x,2))

df_group.to_csv('df_group.csv',index=False,sep=';',decimal=',')