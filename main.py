import pandas as pd
from databases.connection import connector_mysql, create_table, create_sale

mydb = connector_mysql()
create_table()
mycursor = mydb.cursor()
data = pd.read_csv('sales_data_sample.csv', sep=";", encoding="latin1")
data = data.fillna(0)

# FORMAT DATE TO ORDERDATE
data['ORDERDATECLEAN'] = pd.to_datetime(data['ORDERDATE'])

# ASSIGN FIELD YEAR TO CSV
data['YEAR'] = data['ORDERDATECLEAN'].dt.strftime('%Y')

# ASSIGN FIELD MONTH TO CSV
data['MONTH'] = data['ORDERDATECLEAN'].dt.strftime('%m')

# ASSIGN FIELD SALES TO CSV
data['SALES'] = data['QUANTITYORDERED'] * data['PRICEEACH']


for index, row in data.iterrows():
    create_sale(row)
