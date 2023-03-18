from sql_commands.connect import connect_sql
import sys
import random
import requests as r
import pandas as pd
import io
conn=connect_sql()
cursor=conn.cursor(buffered=True)


def readPartition(path,k):
    k=int(k)
    cursor.execute('Select * from namenode where name="'+path+'";')
    node=cursor.fetchall()

    if len(node)==0:
        return 'error: No such path exists!'
    else:
        node=node[0][0]
        #print(node)
        #print('Select p_id from part where node_id='+str(node)+' and p_no='+str(k)+';')
        cursor.execute("Select p_id from part where node_id="+str(node)+" and p_no="+str(k)+";")
        p_id=cursor.fetchall()
        print(p_id)
        #print(len(p_id))
        if len(p_id)==0:
            return 'error: No such partition exists!'
        else:
            p_id=p_id[0][0]
            table_name=str(node)+"_"+str(p_id)
            cursor.execute("Select content from "+table_name+";")
            link=cursor.fetchall()[0][0]
            content=r.get(link).json()
            content=content.replace('$$$','\n')
            y=pd.read_csv(io.StringIO(content),sep=",",on_bad_lines='skip')
            y=y.drop('Unnamed: 0',axis=1)
            y=y.reset_index().drop('index',axis=1)
            return y