import pandas as pd
import mysql.connector
from sqlalchemy import types,create_engine

csv_data=pd.read_csv("DISTRO-2357.csv",usecols=['Track ID','Performer Legal/Birth Name','Performer Role'])
track=tuple(csv_data['Track ID'])
#print(track)
#engine=create_engine('mysql+mysqlconnector://DBcreds',echo=False)
engine=create_engine('mysql+mysqlconnector://DBcreds',echo=False)
data=pd.read_sql_query("select birth_name,unique_track_id,performer_type from performer where unique_track_id in "+str(track),engine)
csv_new=csv_data.sort_values(by=['Track ID'])

data_new=data.sort_values(by=['unique_track_id'])


'''
csv_new=pd.read_csv("vgsales.csv",usecols=['Rank','Name','Year','Publisher'],sep=",")
engine=create_engine('mysql+mysqlconnector://root:root@127.0.0.1:3306/flaskapp',echo=False)
data_new=pd.read_sql_query("select * from vg ;",engine)
'''

def compare(csv_new_row,data_new_row):
    if csv_new_row==data_new_row:
        print(csv_new_row)
    else:
        print(f"value in csv :-{csv_new_row} value in database :-{data_new_row}")


def database(csv_index,csv_row):
    for index,row in data_new.iterrows():
        data_new_index=index
        if data_new_index==csv_index:
            data_new_row=row["birth_name"]
            print(f"comparing {data_new_index+1} row ")
            compare(csv_new_row,data_new_row)


def csv():
    for index,row in csv_new.iterrows():
        csv_new_index=index
        csv_new_row=row["Performer Legal/Birth Name"]
        database(csv_new_index,csv_new_row)
        

csv()
