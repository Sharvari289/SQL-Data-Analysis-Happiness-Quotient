
from sql_commands.connect import connect_sql
#from check_path import checker
import sys

conn=connect_sql()
cursor=conn.cursor()

def id(path):
    path_ls=list(filter(None,path.split("/")))
    path_ls.insert(0,'/')
    if len(path.split("/")[:-1])==1 and path.split("/")[:-1][0]=='':
        cursor.execute("Select node_id from namenode where name='/';")
    else:
        cursor.execute("Select node_id from namenode where name='"+"/".join(path.split("/")[:-1])+"';")
    parent=cursor.fetchall()[0][0]

    cursor.execute("Select node_id from namenode where name='"+path+"';")
    child=cursor.fetchall()[0][0]
    
    #print(parent,child)
    cursor.execute("Insert into struct values ("+str(parent)+","+str(child)+");")
    conn.commit()
    
        