


import psycopg2
import pandas as pd


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



def drop_table(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()



def create_table(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()



AccountsCountry = pd.read_csv("data/Wealth-AccountsCountry.csv")



AccountsCountry_clean = AccountsCountry[['Code','Short Name','Table Name','Long Name','Currency Unit']]



AccountsCountry_clean.head()



AccountsData = pd.read_csv("data/Wealth-AccountData.csv")



AccountsData_clean = AccountsData[['Country Name', 'Country Code', 'Series Name', 'Series Code','1995 [YR1995]','2000 [YR2000]','2005 [YR2005]','2010 [YR2010]','2014 [YR2014]']]



AccountsData_clean.head()



AccountsSeries = pd.read_csv("data/Wealth-AccountSeries.csv")



AccountsSeries_clean = AccountsSeries[['Code','Topic','Indicator Name','Long definition']]



AccountsSeries_clean.head()



cur, conn = create_database()



accountcountry_table_create = ("""Create table if not exists accountsCountry(
                               Country_Code varchar,
                               Short_Name varchar,
                               Table_Name varchar,
                               Long_Name varchar,
                               Currency_Unit varchar)""")



cur.execute(accountcountry_table_create)
conn.commit()



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



cur.execute(accountData_table_create)
conn.commit()



accountSeries_table_create = ("""Create table if not exists accountsSeries(
                              Country_Code varchar,
                              Topic varchar,
                              Indicator_Name varchar,
                              Long_definition varchar)""")



cur.execute(accountSeries_table_create)
conn.commit()



accountcountry_table_insert = ("""Insert into accountsCountry(
                               Country_Code,
                               Short_Name,
                               Table_Name,
                               Long_Name,
                               Currency_Unit)
                               values(%s,%s,%s,%s,%s)""")



for i,row in AccountsCountry_clean.iterrows():
    cur.execute(accountcountry_table_insert , list(row))



conn.commit()



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



for i,row in AccountsData_clean.iterrows():
    cur.execute(accountData_table_insert , list(row))



accountSeries_table_insert = ("""Insert into accountsSeries(
                              Country_Code,
                              Topic,
                              Indicator_Name,
                              Long_definition)
                              values(%s,%s,%s,%s)""")



conn.commit()



for i,row in AccountsSeries_clean.iterrows():
    cur.execute(accountSeries_table_insert , list(row))



cur.close()
conn.close()






