import requests as r
import pandas as pd
import re
import random
namenode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/'
structure_link='https://happinessq-fc061-default-rtdb.firebaseio.com/structure/root/'
datanode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/datanode/'
partition_id_link ='https://happinessq-fc061-default-rtdb.firebaseio.com/'

def put(file_name,file_path,k):
  partition_id_list=list(r.get('https://happinessq-fc061-default-rtdb.firebaseio.com/'+'.json').json().keys())
  partition_id_list.remove('Namenode')
  partition_id_list.remove('structure')
  partition_id_list=[int(i.split('_')[-1]) for i in partition_id_list]
  k=int(k)
  t=file_name
  if file_name.split('.')[-1]=='csv' or file_name.split('.')[-1]=='json':
    file_name=file_name.split('/')[-1]
    file_name='%'.join(file_name.split('.'))
    if file_path=='/':
      file_path=""
    if r.get(structure_link+file_path+'.json').json()==None:
      return "error: put: cannot put {}: No such file or directory".format(file_path)
    else:
      d_value='{"'+file_name+'":""}'
      print(d_value)
      if len(r.get(structure_link+file_path+'.json').json())>0:
        print(structure_link+file_path+'.json')
        print(r.patch(structure_link+file_path+'.json',data=d_value).json())
      else:
        print(r.put(structure_link+file_path+'.json',data=d_value).json())
      data=pd.read_csv(t)
      num=random.randint(0, 1000000)
      num_list=[]
      content_list=[]
      size=int(len(data)/k)
      col=data.columns
      for i in range(0,k):
        while(num in partition_id_list):
          num=random.randint(0, 999999)      
        partition_id_list.append(num)
        num_list.append(num)
        temp=data[i*size:(i+1)*size]
        #temp['CarName']=temp['CarName'].apply(lambda x:'-'.join(x.split(' ')))
        s=temp.to_string()
        s=re.sub(' +', ' ', s)
        s=s.replace(' ',',')
        s=s.replace('\n','$$$,')
        content_list.append(s)
      last_node=int(list(r.get('https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/.json?orderBy="inode"&print=pretty&limitToLast=1').json().values())[0]['inode'])
      for i in range(0,len(num_list)):  
          text=''.join(content_list[i]).replace('\n','')
          value = '{"'+str(last_node+1)+'_'+str(num_list[i])+'":'+'"'+text+'"}'
          print(r.patch('https://happinessq-fc061-default-rtdb.firebaseio.com/.json',data=value).json())    
      print(namenode_link+file_name+'.json')   
      print(r.patch(namenode_link+file_name+'.json',data='{"inode":'+str(last_node+1)+',"type":"FILE","part":'+str(num_list)+'}').json())
      return "{} file has been created!.".format(file_name.replace('%','.'))

  else:
    return 'error: Invalid File format'
      