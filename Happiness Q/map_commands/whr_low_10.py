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

def cat(file_path,year,whr_col):
  map_ls=[]
  partition_tables=getPartitionloc(file_path)
  if type(partition_tables)==str:
    return partition_tables
  explain.clear()
  for i in partition_tables:
    total_content=r.get('https://happinessq-fc061-default-rtdb.firebaseio.com/'+i+'/.json').json()+'\n'
    content=total_content.replace('$$$','\n')
    y=pd.read_csv(io.StringIO(content),sep=",",on_bad_lines='skip')
    y=y.drop('Unnamed: 0',axis=1)
    y=y.reset_index().drop('index',axis=1)
    #map_ls.append(map_lowest_10_whr(partition=y,year=2019,whr_col='Happiness_Score'))
    map_ls.append(map_lowest_10_whr(y,year,whr_col))
    
  return map_ls

def map_lowest_10_whr(partition,year,whr_col):
    key_list=[]
    partition=partition[partition['Year']==year]
    low_df=partition.sort_values(by=[whr_col])
    if low_df.shape[0]==0:
        return []
    for i in range(10):
        key_list.append([low_df.iloc[i]['Country'],low_df.iloc[i][whr_col]])
    if 'map' in explain:
      explain['map'].append([key_list,1])
    else:
      explain['map']=[[key_list,1]]
    
    return [key_list,1]

def combiner_lowest_10_whr(map_list):
    dict_lowest={}
    
    for val in map_list:
        #print(val)
        if len(val)==0:
            continue
        for inner_val in val[0]:
            #print(inner_val)
            country_ls=inner_val[0]
            whr_ls=inner_val[1]
            dict_lowest[country_ls]=whr_ls
            #print(dict_lowest)
    explain['combiner']=dict_lowest
    return dict_lowest

def reduce_lowest_10_whr(dict_lowest):
    dict_lowest={k: v for k, v in sorted(dict_lowest.items(), key=lambda item: item[1])}
    explain['reducer']=list(dict_lowest.keys())[:10]
    return list(dict_lowest.keys())[:10]


def low_10_main(file_path,year,whr_col):
    map_ls=cat('users/pop2/pop3/whr.csv',year,whr_col)
    dict_ls=combiner_lowest_10_whr(map_ls)
    return explain,pd.DataFrame(reduce_lowest_10_whr(dict_ls),columns=['Lowest 10 countries for {} for year {}'.format(whr_col,year)])