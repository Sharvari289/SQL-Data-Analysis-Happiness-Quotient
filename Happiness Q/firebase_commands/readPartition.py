import requests as r
import pandas as pd
import random
import io
from firebase_commands.getPartitionloc import getPartitionloc
namenode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/'
structure_link='https://happinessq-fc061-default-rtdb.firebaseio.com/structure/root/'
datanode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/datanode/'
partition_id_link ='https://happinessq-fc061-default-rtdb.firebaseio.com/'


def readPartition(file_path,partition_number):
  partition_number=int(partition_number)
  partition_tables=getPartitionloc(file_path)
  if type(partition_tables)==str:
    return partition_tables
  if partition_number>len(partition_tables):
    return "Partition Number does not exist"
  p_table=partition_tables[partition_number-1]
  print(p_table)
  content=r.get('https://happinessq-fc061-default-rtdb.firebaseio.com/'+p_table+'/.json').json()
  content=content.replace('$$$','\n')
  y=pd.read_csv(io.StringIO(content),sep=",",on_bad_lines='skip')
  y=y.drop('Unnamed: 0',axis=1)
  y=y.reset_index().drop('index',axis=1)
  return y