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

def cat(file_path,year,gdp_option):
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
    map_list.append(map_highest_10_gdp(partition=y,year=year,gdp_opt=gdp_option))
    #map_list.append(map_max_gdp(partition=y,year=2019,gdp_opt='GDP_USD'))
    
  return map_list


def map_highest_10_gdp(partition,year,gdp_opt):
    key_list=[]
    partition=partition[partition['year']==year]
    top_df=partition.sort_values(by=[gdp_opt],ascending=False)
    for i in range(10):
        key_list.append([top_df.iloc[i]['Country_Name'],top_df.iloc[i][gdp_opt]])
    if 'map' in explain:
      explain['map'].append([key_list,1])
    else:
      explain['map']=[[key_list,1]]
    return [key_list,1]

def combiner_highest_10_gdp(map_list):
    dict_lowest={}
    
    for val in map_list:
        #print(val)
        for inner_val in val[0]:
            #print(inner_val)
            country_ls=inner_val[0]
            gdp_ls=inner_val[1]
            dict_lowest[country_ls]=gdp_ls
            #print(dict_lowest)
    explain['combiner']=[dict_lowest,1]
    return [dict_lowest,1]

def reduce_highest_10_gdp(dict_highest):
    dict_highest={k: v for k, v in sorted(dict_highest.items(), key=lambda item: item[1],reverse=True)}
    explain['reducer']=list(dict_highest.keys())[:10]
    return list(dict_highest.keys())[:10]


def max_main(file_path,year,gdp_option):
    map_list=cat('/GDP_updated.csv',year,gdp_option)
    combined_res=combiner_highest_10_gdp(map_list)
    return explain,pd.DataFrame(reduce_highest_10_gdp(combined_res[0]),columns=['Country with maximum {} for year {}'.format(gdp_option,year)])
    #print(reduce_max_gdp(map_list))