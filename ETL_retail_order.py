#import libraries
get_ipython().system('pip install kaggle')
import kaggle
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

#Let's download dataset from Kaggle API.
get_ipython().system('kaggle datasets download ankitbansal06/retail-orders -f orders.csv')

#Let's extract and transform dataset using pandas.
df = pd.read_csv('orders.csv', na_values = ['Not Available', 'unknown'])
df.head(10)
df['Ship Mode'].unique()

#rename columns names ..make them lower case and replace space with underscore
df.columns=df.columns.str.lower()
df.columns=df.columns.str.replace(' ','_')
df.head(5)

#derive new columns discount , sale price and profit
df['discount']=df['list_price']*df['discount_percent']*.01
df['sale_price']= df['list_price']-df['discount']
df['profit']=df['sale_price']-df['cost_price']
df.head()


#Let's make sure all datatypes are correct.
df.dtypes
df["order_date"] = pd.to_datetime(df["order_date"], format = "%Y-%m-%d")

df.dtypes

# Dropping columns which are not required.
df.drop(columns = ['list_price', 'cost_price', 'discount_percent'], inplace = True)
df


# Let's load our dataset into MySQL database for further ad-hoc analysis.
# Connect to MySQL server without specifying a database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="******"
)

cursor = conn.cursor()

# Create the database if it doesn't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS retail_orders")

cursor.close()
conn.close()

print("Database 'retail_orders' is ready.")

# creating engine
engine = create_engine("mysql+mysqlconnector://root:******@localhost:3306/retail_orders")

# Trying to connect now
conn = engine.connect()
print("Connection to 'retail_orders' successful.")

#dumping data from our pandas dataframe into MySQL database
df.to_sql(
    name='df_orders',
    con=conn,     
    index=False,
    if_exists='append'
)


