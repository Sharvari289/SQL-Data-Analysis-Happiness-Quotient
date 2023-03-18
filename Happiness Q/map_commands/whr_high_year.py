import requests as r
import pandas as pd
import random
import io
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

def cat(file_path,whr_col,country):
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
    #map_ls.append(map_lowest_10_whr(partition=y,year=2019,whr_col='Happiness_Score'))
    map_ls.append(map_highest_score_year(y,whr_col,country))
    
  return map_ls

def map_highest_score_year(partition,whr_col,country):
    key_list=[]
    partition=partition[(partition['Country']==country)]
    low_df=partition.sort_values(by=[whr_col],ascending=True)
    if low_df.shape[0]==0:
        return []

    if 'map' in explain:
      explain['map'].append([low_df.iloc[0]['Year'],low_df.iloc[0][whr_col]])
    else:
      explain['map']=[[low_df.iloc[0]['Year'],low_df.iloc[0][whr_col]]]
    return [low_df.iloc[0]['Year'],low_df.iloc[0][whr_col]]

def reduce_highest_score_year(map_ls):
    max_val=0
    
    for val in map_ls:
        if len(val)==0:
            continue
        if val[1]>max_val:
            max_val=val[1]
            max_year=val[0]
    explain['reducer']=[max_year,max_val]
    return [max_year,max_val]

def high_year_main(file_path,whr_col,country):
    map_ls=cat('users/pop2/pop3/whr.csv',whr_col,country)
    #dict_ls=combiner_lowest_10_whr(map_ls)
    return explain,pd.DataFrame([reduce_highest_score_year(map_ls)],columns=['Maximum year','Maximum value for {} for country {}'.format(whr_col,country)])
