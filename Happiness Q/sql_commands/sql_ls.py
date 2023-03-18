from sql_commands.connect import connect_sql
import sys


conn=connect_sql()
cursor=conn.cursor(buffered=True)



def ls(path):
    cursor.execute('Select * from namenode where name="'+path+'";')
    value=cursor.fetchall()

    if len(value)==0:
        return "error: No such path exists!"
    else:
        if value[0][1]!='DIRECTORY':
            return "No such folder exists!"
        else:
            node=value[0][0]
            cursor.execute('Select child from struct where parent="'+str(node)+'";')
            child_list=cursor.fetchall()
            child_list=[x[0] for x in child_list]
            child=[]
            for i in child_list:
                cursor.execute('Select name from namenode where node_id="'+str(i)+'";')
                child_list=cursor.fetchall()
                name=child_list[0][0].split('/')[-1]
                child.append(name)
            return child