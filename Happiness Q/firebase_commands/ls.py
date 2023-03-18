import requests as r
#import pandas
#import random
namenode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/'
structure_link='https://happinessq-fc061-default-rtdb.firebaseio.com/structure/root/'
datanode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/datanode/'


def ls(directory_path):
  #print(r.get(structure_link+directory_path+'.json').json())
  if r.get(structure_link+directory_path+'.json').json()==None:
    return 'error: ls: cannot access {}: No such file or directory'.format(directory_path)
  else:
    if len(r.get(structure_link+directory_path+'.json').json())==0:
        return "The folder structure is empty"
    else:
      ans=r.get(structure_link+directory_path+'.json').json().keys()
      ans=[i.replace('%','.') if '%' in i else i for i in ans]
      return ans