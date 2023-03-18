import requests as r
import pandas as pd
import random
import io
from firebase_commands.getPartitionloc import getPartitionloc
namenode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/'
structure_link='https://happinessq-fc061-default-rtdb.firebaseio.com/structure/root/'
datanode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/datanode/'
partition_id_link ='https://happinessq-fc061-default-rtdb.firebaseio.com/'

def cat(file_path):
  total_content=""
  partition_tables=getPartitionloc(file_path)
  if type(partition_tables)==str:
    return partition_tables
  for i in partition_tables:
    total_content=total_content+r.get('https://happinessq-fc061-default-rtdb.firebaseio.com/'+i+'/.json').json()+'\n'
  #print(total_content.split(','))
  
  content=total_content.replace('$$$','\n')
  y=pd.read_csv(io.StringIO(content),sep=",",on_bad_lines='skip')
  y=y.drop('Unnamed: 0',axis=1)
  y=y.reset_index().drop('index',axis=1)
  return y.drop_duplicates(keep=False)