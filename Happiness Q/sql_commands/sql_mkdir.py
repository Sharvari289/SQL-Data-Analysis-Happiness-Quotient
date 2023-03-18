from sql_commands.connect import connect_sql
#from check_path import checker
import sys
from sql_commands.mkdir_select import id


conn=connect_sql()
cursor=conn.cursor()

def mkdir(path):
    cursor.execute("Select * from namenode where name='"+path+"';")
    status=cursor.fetchall()
    #print(len(status))
    if len(status)>0:
        return  "error: mkdir: cannot create directory '{}': Directory already exists".format(path)
    parent_path='/'.join(path.split('/')[:-1])  
    if parent_path=="":
        parent_path='/'
    #print(parent_path)
    cursor.execute("Select * from namenode where name='"+parent_path+"';")
    status=cursor.fetchall()
    #print(status)
    if len(status)>0:
        status=1
    else:
        #print('in')
        return "error: mkdir: cannot create directory '{}': No such file or directory".format(path)
    path_ls=list(filter(None,path.split("/")))
    path_ls.insert(0,'/')
    #print("/".join(path.split("/")[:-1]))
    #if len(path_ls[-1].split('.'))>1:
        #type_='FILE'
    #else:
    type_='DIRECTORY'
    if status==1:
        cursor.execute("INSERT INTO namenode (type, name) Values ('"+type_+"','"+path+"');")
        conn.commit()
        id(path)
    return "{} has been created!".format(path)
    