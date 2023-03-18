import requests as r
import pandas as pd
import random
import io
import numpy as np
from itertools import chain
explain={}
namenode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/'
structure_link='https://happinessq-fc061-default-rtdb.firebaseio.com/structure/root/'
datanode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/datanode/'
partition_id_link ='https://happinessq-fc061-default-rtdb.firebaseio.com/'

def getPartitionloc(file_path):
  if file_path[0]=="/":
    file_path=file_path[1:]
  check_path='/'.join(file_path.split('/')[:-1])
  name=file_path.split('/')[-1].replace('.','%')
  if r.get(structure_link+check_path+'/'+name+'/.json').json()==None:
    return "'{}': No such file".format(file_path)
  else:
    values=r.get(namenode_link+name+'/.json').json()
    inode=values['inode']
    part=values['part']
    loc_list=[]
    for i in part:
      loc_list.append(str(inode)+'_'+str(i))
    return loc_list

def cat(file_path,country,subject):
  map_ls=[]
  partition_tables=getPartitionloc(file_path)
  explain.clear()
  if type(partition_tables)==str:
    return partition_tables
  for i in partition_tables:
    total_content=r.get('https://happinessq-fc061-default-rtdb.firebaseio.com/'+i+'/.json').json()+'\n'
    content=total_content.replace('$$$','\n')
    y=pd.read_csv(io.StringIO(content),sep=",",on_bad_lines='skip')
    y=y.drop('Unnamed: 0',axis=1)
    y=y.reset_index().drop('index',axis=1)
    map_ls.append(map_min_month_country_subject(partition=y,country=country,subject=subject))
  
    
  return map_ls



def map_min_month_country_subject(partition,country,subject):
    key_list=[]
    partition=partition[(partition['LOCATION']==country) & (partition['SUBJECT']==subject) ]
    
    bot_df=partition.sort_values(by=['Value'],ascending=True)
    
    
    if bot_df.shape[0]==0:
        return []
    key_list.append([bot_df.iloc[0]['Value'],bot_df.iloc[0]['MONTH']])


    if 'map' in explain:
      explain['map'].append([key_list,1])
    else:
      explain['map']=[[key_list,1]]
    return [key_list,1]


def reduce_min_month_country_subject(map_list):
    min_val=10**20
    
    for val in map_list:
        
        if len(val)==0:
            continue
        for innerval in val[0]:
            if innerval[1]<min_val:
                min_val=innerval[1]
                min_month=innerval[0]

    explain['reducer']=[min_month,min_val]
    return [min_month,min_val]


def min_main(file_path,country,subject):
    map_ls=cat('shar/shar1/unemployment-2020.csv',country,subject)
    month_map={1:'January',2:'February',3:'March',4:'April',5:'May',6:'June',7:'July',8:'August',9:'September',10:'October',11:'November',12:'December'}
    v,m=reduce_min_month_country_subject(map_ls)
    m=month_map[m]
    return explain,pd.DataFrame([[v,m]],columns=['Minimum Unemployent Value','Month for minimum value for gender {} for country {}'.format(subject,country)])

