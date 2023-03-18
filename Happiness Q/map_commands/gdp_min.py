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

def cat(file_path,year,gdp_opt):
  map_list=[]
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
    #top_10_gdp(partition=y,year=2020,gdp_opt='GDP_per_capita_USD')
    map_list.append(map_min_gdp(partition=y,year=year,gdp_opt=gdp_opt))
    #map_list.append(map_max_gdp(partition=y,year=2019,gdp_opt='GDP_USD'))
    
  return map_list

def map_min_gdp(partition,year,gdp_opt):
    key_list=[]
    partition=partition[partition['year']==year]
    key_list.append(partition.sort_values(by=[gdp_opt]).iloc[0]['Country_Name'])
    key_list.append(partition.sort_values(by=[gdp_opt]).iloc[0][gdp_opt])
    value=1
    if 'map' in explain:
      explain['map'].append([key_list,value])
    else:
      explain['map']=[[key_list,value]]
    return [key_list,value]
            
def reduce_min_gdp(map_list):
    min_val=10**20
    for val in map_list:
        gdp=val[0][1]
        if gdp<min_val:
            min_val=gdp
            min_country=val[0][0]
    explain['reducer']=[min_country,min_val]
    return min_country,min_val   


def min_main(file_path,year,gdp_option):

    map_list=cat('/GDP_updated.csv',year,gdp_option)
    return explain,pd.DataFrame([reduce_min_gdp(map_list)],columns=['Country with minimum {}'.format(gdp_option),'Minimum value for {}'.format(year)])
