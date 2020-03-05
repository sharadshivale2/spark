import pandas as pd
import mysql.connector
import numpy as np
from sqlalchemy import types,create_engine


csv_data=pd.read_csv("DISTRO-2357.csv",usecols=['Track ID','Performer Legal/Birth Name','Performer Role'])
track=tuple(csv_data['Track ID'])
csv_new=csv_data.sort_values(by=['Track ID'])






def connection():
    engine=create_engine('mysql+mysqlconnector://asingh:*ABnR*Me*B9LbetYaM@qa-ows-track.cluster-cb22xqmk0y0q.us-east-1.rds.amazonaws.com:3306/ows_track',echo=False)
    data_new=pd.read_sql_query("select birth_name,unique_track_id,performer_type from performer where unique_track_id in "+str(track),engine)
    return data_new

def compare(csv_row,database_row):
    if csv_row==database_row:
        print(csv_row)
    else:
        print(f"value in csv :- {csv_row} value in database :- {database_row}")


def database(csv_index,csv_row):
    for index,row in database_data.iterrows():
        database_index=index
        if database_index==csv_index:
            database_row=row["birth_name"]
            print(f"comparing {database_index+1} row ")
            print(database_row)
            compare(csv_row,database_row)


def csv():
    data=connection()
    database_data=data.sort_values(by=['unique_track_id'])
    temp=0
    for index,row in csv_data.iterrows():
        #trackid=row["Track ID"]
        #new=database_data.loc[database_data['unique_track_id']==trackid]
        #for index1,row1 in new.iterrows():
            new_name=row1["birth_name"]
              
            csv_row=row["Performer Legal/Birth Name"]
            #print(type(new_name))
            if csv_row==new_name:
                pass
            else:
                temp+=1
                print(f"not same at {trackid}")
                print(f"faulty values {temp}")
        #database(csv_index,csv_row)
        

csv()


