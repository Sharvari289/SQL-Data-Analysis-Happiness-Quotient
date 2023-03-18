from sql_commands.connect import connect_sql
import sys
import random
import pandas as pd 
import requests as r
import io
conn=connect_sql()
cursor=conn.cursor(buffered=True)


def cat(path):

    cursor.execute('Select * from namenode where name="'+path+'";')
    node=cursor.fetchall()


    if len(node)==0:
        return 'error: No such path exists!'
    else:
        node=node[0][0]

        cursor.execute('Select p_id,p_no from part where node_id='+str(node)+';')
        values=cursor.fetchall()
        values.sort(key = lambda x: x[1])
        content=""
        for i in values:
            table_id=str(node)+'_'+str(i[0])
            cursor.execute('Select content from '+table_id+';')
            link=cursor.fetchall()[0][0]
            content=content+r.get(link).json()+'\n'
        content=content.replace('$$$','\n')
        y=pd.read_csv(io.StringIO(content),sep=",",on_bad_lines='skip')
        y=y.drop('Unnamed: 0',axis=1)
        y=y.reset_index().drop('index',axis=1)
        return y.drop_duplicates(keep=False)
