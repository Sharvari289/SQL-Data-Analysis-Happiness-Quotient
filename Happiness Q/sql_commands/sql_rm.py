from sql_commands.connect import connect_sql
import sys
import random

conn=connect_sql()
cursor=conn.cursor()

def rm(path):
    name=path.split('/')[-1]
    cursor.execute('Select * from namenode where name="'+path+'";')
    node=cursor.fetchall()

    print(node)
    if len(node)==0:
        return 'error: No such path exists!'

    else:
        node=node[0][0]
        cursor.execute('Select p_id from part where node_id='+str(node)+';')
        p_id=cursor.fetchall()
        for i in p_id:
            table_name=str(node)+'_'+str(i[0])
            #print('Drop table '+table_name+';')
            cursor.execute('Drop table '+table_name+';')
            print('done')
        cursor.execute('DELETE from part where node_id='+str(node)+';')
        cursor.execute('DELETE from struct where child='+str(node)+';')
        cursor.execute('DELETE from namenode where node_id='+str(node)+';')
        conn.commit()

    return "{} has been deleted!".format(name)