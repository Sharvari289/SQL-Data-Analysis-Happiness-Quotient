from sql_commands.connect import connect_sql
import requests as r
import sys
import pandas as pd
import random
import re
firebase_link="https://happiness-sql-76db1-default-rtdb.firebaseio.com/"
conn=connect_sql()
cursor=conn.cursor(buffered=True)

def put(file_name,path,k):
    k=int(k)
    name=file_name.split('/')[-1]
    cursor.execute('Select p_id from part;')
    partition_id_list=cursor.fetchall()
    #print(partition_id_list)
    cursor.execute('Select * from namenode where name="'+path+'";')
    parent_node=cursor.fetchall()

    if len(parent_node)==0:
        return 'error: No such path exists!'
    else:
        parent_node=parent_node[0][0]
        if file_name.split('.')[-1]=='csv' or file_name.split('.')[-1]=='json':
            data=pd.read_csv(file_name)
            num=random.randint(0, 1000000)
            num_list=[]
            content_list=[]
            size=int(len(data)/k)
            col=data.columns
            for i in range(0,k):
                while(num in partition_id_list):
                    num=random.randint(0, 999999)      
                partition_id_list.append(num)
                num_list.append(num)
                temp=data[i*size:(i+1)*size]
                #temp['CarName']=temp['CarName'].apply(lambda x:'-'.join(x.split(' ')))
                s=temp.to_string()
                s=re.sub(' +', ' ', s)
                s=s.replace(' ',',')
                s=s.replace('\n','$$$,')
                content_list.append(s)


            if path[-1]=='/':
                path=path[:-1]
            total_path=path+'/'+name
            cursor.execute("INSERT INTO namenode (type, name) Values('FILE','"+total_path+"');")
            conn.commit()
            cursor.execute('Select max(node_id) from namenode;')
            last_node=cursor.fetchall()[0][0]
            #print("INSERT INTO struct Values("+str(parent_node)+","+str(last_node+1)+");")
            cursor.execute("INSERT INTO struct Values("+str(parent_node)+","+str(last_node)+");")
            #print(content_list)
            #print(num_list)
            for i in range(0,len(num_list)):
                cursor.execute("CREATE TABLE "+str(last_node)+'_'+str(num_list[i])+" (content varchar(600) not null);")
                value = '{"'+str(last_node)+'_'+str(num_list[i])+'":'+'"'+content_list[i]+'"}'  
                print(r.patch(firebase_link+'.json',data=value).json()) 
                link=firebase_link+str(last_node)+'_'+str(num_list[i])+'.json'
                cursor.execute("INSERT INTO "+str(last_node)+'_'+str(num_list[i])+" Values('"+link+"');")
                cursor.execute("INSERT INTO part Values("+str(last_node)+","+str(num_list[i])+","+str(i+1)+");")
            
            conn.commit()
        return "{} has been uploaded!".format(name)