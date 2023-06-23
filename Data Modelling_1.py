#!/usr/bin/env python
# coding: utf-8

# In[1]:


import psycopg2
import pandas as pd


# In[2]:


def create_database():
    conn = psycopg2.connect("host = localhost dbname=postgres user=postgres password=Anshi@18")
    conn.set_session(autocommit = True)
    cur = conn.cursor()
    
    cur.execute("Drop database if exists accounts")
    cur.execute("Create database accounts")
    
    conn.close()
    conn = psycopg2.connect("host = localhost dbname=accounts user=postgres password=Anshi@18")
    cur = conn.cursor()
    return cur, conn


# In[3]:


def drop_table(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


# In[4]:


def create_table(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


# In[5]:


AccountsCountry = pd.read_csv("data/Wealth-AccountsCountry.csv")


# In[6]:


AccountsCountry_clean = AccountsCountry[['Code','Short Name','Table Name','Long Name','Currency Unit']]


# In[7]:


AccountsCountry_clean.head()


# In[8]:


AccountsData = pd.read_csv("data/Wealth-AccountData.csv")


# In[9]:


AccountsData_clean = AccountsData[['Country Name', 'Country Code', 'Series Name', 'Series Code','1995 [YR1995]','2000 [YR2000]','2005 [YR2005]','2010 [YR2010]','2014 [YR2014]']]


# In[10]:


AccountsData_clean.head()


# In[11]:


AccountsSeries = pd.read_csv("data/Wealth-AccountSeries.csv")


# In[12]:


AccountsSeries_clean = AccountsSeries[['Code','Topic','Indicator Name','Long definition']]


# In[13]:


AccountsSeries_clean.head()


# In[15]:


cur, conn = create_database()


# In[16]:


accountcountry_table_create = ("""Create table if not exists accountsCountry(
                               Country_Code varchar,
                               Short_Name varchar,
                               Table_Name varchar,
                               Long_Name varchar,
                               Currency_Unit varchar)""")


# In[17]:


cur.execute(accountcountry_table_create)
conn.commit()


# In[18]:


accountData_table_create = ("""Create table if not exists accountsData(
                               Country_Name varchar, 
                               Country_Code varchar,
                               Series_Name varchar, 
                               Series_Code varchar,
                               year_1995 varchar,
                               year_2000 varchar,
                               year_2005 varchar,
                               year_2010 varchar,
                               year_2014 varchar)""")


# In[19]:


cur.execute(accountData_table_create)
conn.commit()


# In[20]:


accountSeries_table_create = ("""Create table if not exists accountsSeries(
                              Country_Code varchar,
                              Topic varchar,
                              Indicator_Name varchar,
                              Long_definition varchar)""")


# In[21]:


cur.execute(accountSeries_table_create)
conn.commit()


# In[22]:


accountcountry_table_insert = ("""Insert into accountsCountry(
                               Country_Code,
                               Short_Name,
                               Table_Name,
                               Long_Name,
                               Currency_Unit)
                               values(%s,%s,%s,%s,%s)""")


# In[23]:


for i,row in AccountsCountry_clean.iterrows():
    cur.execute(accountcountry_table_insert , list(row))


# In[24]:


conn.commit()


# In[25]:


accountData_table_insert = ("""Insert into accountsData(
                               Country_Name, 
                               Country_Code,
                               Series_Name, 
                               Series_Code,
                               year_1995,
                               year_2000,
                               year_2005,
                               year_2010,
                               year_2014)
                               values(%s,%s,%s,%s,%s,%s,%s,%s,%s)""")


# In[26]:


for i,row in AccountsData_clean.iterrows():
    cur.execute(accountData_table_insert , list(row))


# In[29]:


accountSeries_table_insert = ("""Insert into accountsSeries(
                              Country_Code,
                              Topic,
                              Indicator_Name,
                              Long_definition)
                              values(%s,%s,%s,%s)""")


# In[31]:


conn.commit()


# In[32]:


for i,row in AccountsSeries_clean.iterrows():
    cur.execute(accountSeries_table_insert , list(row))


# In[33]:


cur.close()
conn.close()


# In[ ]:




