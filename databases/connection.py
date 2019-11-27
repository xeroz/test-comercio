import mysql.connector
import os
from dotenv import load_dotenv
load_dotenv('.env')


def connector_mysql():
    mydb = mysql.connector.connect(
        user=os.getenv('user'),
        passwd=os.getenv('password'),
        database=os.getenv('database')
    )
    return mydb


def create_table():
    mydb = connector_mysql()
    mycursor = mydb.cursor()

    mycursor.execute("SHOW TABLES")

    for x in mycursor:
        if x[0] != 'sales':
            mycursor.execute("CREATE TABLE sales (ordernumber int, sales double, orderdate date, year int, month int, status varchar(100), customername varchar(100))")


def create_sale(row):
    mydb = connector_mysql()
    mycursor = mydb.cursor()

    order_date = row['ORDERDATECLEAN'].date()

    sql = "INSERT INTO sales (ordernumber, sales, orderdate, year, month, status, customername) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (row['ORDERNUMBER'], row['SALES'], order_date, row['YEAR'], row['MONTH'], row['STATUS'], row['CUSTOMERNAME'])

    mycursor.execute(sql, val)

    mydb.commit()

    print(mycursor.rowcount, "record inserted.")

