import requests as r
#import pandas
#import random
namenode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/'
structure_link='https://happinessq-fc061-default-rtdb.firebaseio.com/structure/root/'
datanode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/datanode/'

def mkdir(directory_path):
  check_path='/'.join(directory_path.split('/')[:-1])
  name=directory_path.split('/')[-1]
  if r.get(structure_link+check_path+'.json').json()==None:
    return "error: mkdir: cannot create directory '{}': No such file or directory".format(directory_path)
  else:
    d_value='{"'+directory_path.split('/')[-1]+'":""}'
    if len(r.get(structure_link+check_path+'.json').json())>0:
      r.patch(structure_link+check_path+'.json',data=d_value).json()

    else:
      r.put(structure_link+check_path+'.json',data=d_value).json()
    last_node=int(list(r.get('https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/.json?orderBy="inode"&print=pretty&limitToLast=1').json().values())[0]['inode'])
    r.patch(namenode_link+name+'.json',data='{"inode":'+str(last_node+1)+',"type":"DIRECTORY"}').json()
    return "New folder '{}' has been created".format(name)