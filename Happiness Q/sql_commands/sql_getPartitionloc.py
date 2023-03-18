from sql_commands.connect import connect_sql
import sys
import random

conn=connect_sql()
cursor=conn.cursor(buffered=True)


def getPartitionloc(path):
    cursor.execute('Select * from namenode where name="'+path+'";')
    node=cursor.fetchall()


    if len(node)==0:
        return 'error: No such path exists!'
    else:
        node=node[0][0]
        #print(node)
        #print('Select p_id from part where node_id='+str(node)+';')
        cursor.execute('Select p_id from part where node_id='+str(node)+';')
        p_id=cursor.fetchall()
        p_id=[str(node)+'_'+str(i[0]) for i in p_id]
        return p_id