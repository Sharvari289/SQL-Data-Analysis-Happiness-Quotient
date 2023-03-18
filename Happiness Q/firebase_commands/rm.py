import requests as r
import pandas as pd
import random
import io
from firebase_commands.getPartitionloc import getPartitionloc
namenode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/'
structure_link='https://happinessq-fc061-default-rtdb.firebaseio.com/structure/root/'
datanode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/datanode/'
partition_id_link ='https://happinessq-fc061-default-rtdb.firebaseio.com/'



def rm(file_path):
  partition_tables=getPartitionloc(file_path)
  if type(partition_tables)==str:
    return partition_tables
  else:
    name=file_path.split('/')[-1].replace('.','%')
    file_path=file_path.replace('.','%')
    if file_path[0]=='/':
      file_path=file_path[1:]
    print(partition_tables)

    for i in partition_tables:
      r.delete('https://happinessq-fc061-default-rtdb.firebaseio.com/'+i+'/.json').json()
    r.delete(namenode_link+name+'/.json').json()
    if len(r.get(structure_link+file_path+'/.json').json())==1:
      r.put(structure_link+'/'.join(file_path.split('/')[:-1])+'/.json',data='""')
    else:
      r.delete(structure_link+file_path.replace('.','%')+'/.json').json()
    return "{} Deleted!".format(name).replace('%','.')