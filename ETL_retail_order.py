#!/usr/bin/env python
# coding: utf-8

# In[1]:


#import libraries
get_ipython().system('pip install kaggle')
import kaggle


# In[2]:


get_ipython().system('kaggle datasets download ankitbansal06/retail-orders -f orders.csv')


# In[4]:


get_ipython().system('kaggle datasets download -d ankitbansal06/retail-orders --unzip --force')


# In[19]:


import pandas as pd
df = pd.read_csv('orders.csv', na_values = ['Not Available', 'unknown'])


# In[20]:


df.head(10)


# In[21]:


df['Ship Mode'].unique()


# In[22]:


#rename columns names ..make them lower case and replace space with underscore
df.columns=df.columns.str.lower()
df.columns=df.columns.str.replace(' ','_')
df.head(5)


# In[23]:


#derive new columns discount , sale price and profit
df['discount']=df['list_price']*df['discount_percent']*.01
df['sale_price']= df['list_price']-df['discount']
df['profit']=df['sale_price']-df['cost_price']
df.head()


# In[24]:


df.dtypes


# In[25]:


df["order_date"] = pd.to_datetime(df["order_date"], format = "%Y-%m-%d")


# In[26]:


df.dtypes


# In[27]:


# Dropping columns which are not required.


# In[28]:


df.drop(columns = ['list_price', 'cost_price', 'discount_percent'], inplace = True)


# In[30]:


df.shape


# In[31]:


df


# In[ ]:


#load the data into sql server using replace option
#import sqlalchemy as sal
#engine = sal.create_engine('mssql://ANKIT\SQLEXPRESS/master?driver=ODBC+DRIVER+17+FOR+SQL+SERVER')
#conn=engine.connect()


# In[35]:


import mysql.connector

# Connect to MySQL server without specifying a database
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="yourpassword"
)

cursor = conn.cursor()

# Create the database if it doesn't exist
cursor.execute("CREATE DATABASE IF NOT EXISTS retail_orders")

cursor.close()
conn.close()

print("Database 'retail_orders' is ready.")


# In[36]:


from sqlalchemy import create_engine

# creating engine
engine = create_engine("mysql+mysqlconnector://root:yourpassword@localhost:3306/retail_orders")

# Trying to connect now
conn = engine.connect()
print("Connection to 'retail_orders' successful.")


# In[37]:


#dumping data from our pandas dataframe into MySQL database

df.to_sql(
    name='df_orders',
    con=conn,     
    index=False,
    if_exists='append'
)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




