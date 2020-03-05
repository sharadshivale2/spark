import pandas as pd
from sqlalchemy import types,create_engine


performers=int(input("enter no of performers"))
csv=pd.read_excel("E:\performers data\DISTRO-2357\Eksik Bilgiler_Toygar Ali Işıklı.xlsx",header=0,encoding='latin1')

#csv['Performer Type'].replace({"Primary Performer":"primary","Featured Performer":"featured","Non Featured Performer":"non-featured"},inplace=True)
#csv['Performer Role'].replace({"Wind Instruments - Saxaphone":"Wind Instruments - Saxophone"},inplace=True)
csv_data=csv.sort_values(by=['Track ID'])


new_columns=[]
for item in csv_data.columns:
    counter=0
    newitem=item
    while newitem in new_columns:
        counter+=1
        newitem="{}_{}".format(item,counter)
    new_columns.append(newitem)
csv_data.columns=new_columns

csv_data.columns=csv_data.columns.str.replace(" ","_")
csv_data.columns=csv_data.columns.str.replace(".","_")


track=csv_data['Track_ID']
print(len(track))

###both qa and prod creds  are passed comment out as per needs#### 
#engine=create_engine('mysql+mysqlconnector://asingh:*ABnR*Me*B9LbetYaM@qa-ows-track.cluster-cb22xqmk0y0q.us-east-1.rds.amazonaws.com/ows_track',echo=False)
engine=create_engine('mysql+mysqlconnector://ugala:VDBFvd4cbsZCqV9Z@prod-ows.cb22xqmk0y0q.us-east-1.rds.amazonaws.com:3306/ows_track',echo=False)
#data_role=pd.read_sql_query('SELECT performer_role, performer_role_id FROM performer_role',engine)
database_data=pd.read_sql_query("select birth_name,performer_role_id,unique_track_id,performer_type from performer where unique_track_id in "+str(tuple(track)),engine)
#database_data['birth_name'].replace({"Toygar Ali I??kl?":"Toygar Ali Işıklı"},inplace=True)

missmatch_tracks=list()
empty_tracks=list()
for  i in track:
    csv_list=list()
    database_list=list()
    new_csv=csv_data.loc[csv_data['Track_ID']==i]
    new_db=database_data.loc[database_data['unique_track_id']==i]
    for j in range(0,performers):
        csv_performer=list()
        if j==0:
            #performer_type=new_csv['Performer_Type'].values[0]
            performer_name=new_csv['Performer_Legal/Birth_Name'].values[0]
            #performer_role=new_csv['Performer_Role'].values[0]
            if isinstance(performer_name,str):
                performer_name=performer_name.strip()
                #csv_performer.append(performer_type)
                csv_performer.append(performer_name)
                #csv_performer.append(performer_role)
                csv_list.append(csv_performer)
        else:
            #new_csv['Performer_Type_'+str(j)].replace({"Primary Performer":"primary","Featured Performer":"featured","Non Featured Performer":"non-featured"},inplace=True)
            #new_csv['Performer_Role_'+str(j)].replace({"Wind Instruments - Saxaphone":"Wind Instruments - Saxophone"},inplace=True)
            #performer_type=new_csv['Performer_Type_'+str(j)].values[0]
            performer_name=new_csv['Performer_Legal/Birth_Name_'+str(j)].values[0]
            #performer_role=new_csv['Performer_Role_'+str(j)].values[0]           
            if isinstance(performer_name,str):
                performer_name=performer_name.strip()
                if (pd.notnull(performer_name) and performer_name != ''):
                    if performer_name not in csv_performer:
                        #csv_performer.append(performer_type)
                        csv_performer.append(performer_name)
                        #csv_performer.append(performer_role)
                        csv_list.append(csv_performer)
                    else:
                        pass
            
    for index,row in new_db.iterrows():
        if new_db.empty==False:
            database_performer=list()        
            #temp=row['performer_role_id']
            #new_role_db=data_role.loc[data_role['performer_role_id']==temp]
            #db_performer_role=new_role_db['performer_role'].values[0]       
            #db_performer_type=row['performer_type']        
            db_performer_name=row['birth_name']        
            if isinstance(db_performer_name,str):
                db_performer_name=db_performer_name.strip()
                #database_performer.append(db_performer_type)
                database_performer.append(db_performer_name)
                #database_performer.append(db_performer_role)
                if database_performer !='':
                    database_list.append(database_performer)
                else:
                    pass
    count=0
    if database_list !=[]:
        for ind in csv_list:
            for db in database_list:
                if ind[0]==db[0] and ind[1]==db[1] and ind[2]==db[2]:
                    count=1
                    break
                else:
                    count=-1
            if count ==-1:
                    break
    elif database_list==[]:
        empty_tracks.append(i)
    if count==1:
        pass  
    elif count==-1:
        missmatch_tracks.append(i)

print(f"missmatched tracks:- \n")
print(len(missmatch_tracks))
if missmatch_tracks!=[]:
    print(missmatch_tracks[0])
    print(missmatch_tracks[-1])
    df=pd.DataFrame()
    df['missmatched_tracks']= missmatch_tracks[0:]
    df.to_excel('result_missmatch.xlsx',index=False)
print("****************************************")
print("empty-tracks:- \n")
print(len(empty_tracks))
#print(empty_tracks)

if empty_tracks!=[]:
    df1=pd.DataFrame()
    df1['empty_tracks']= empty_tracks[0:]
    df1.to_excel('result_empty.xlsx',index=False)
print("done")



