from sql_commands.connect import connect_sql
#from check_path import checker
from collections import defaultdict

conn=connect_sql()
cursor=conn.cursor()


def getname(n):
    cursor.execute('Select name from namenode where node_id='+str(n)+';')
    v=cursor.fetchall()[0][0].split('/')[-1]
    if v=='':
        v="root"
    return v

def for_folder(name,data):

    if data==None:
        return {
                "text":name,
                "state":{
                    "opened":True}
            } 
    if data!="":
        if '.' not in name:
            return {
                "text":name,
                "state":{
                    "opened":True},
                "children":[
                    for_folder(i,data[i]) for i in data.keys()
                    
                ]
            }
        else:
            return {
                "text":name.replace('%','.'),
                "icon" : "jstree-file",
                "children":[
                        for_folder(i,data[i]) for i in data.keys()
                ]
            }
    else:
        if '.' not in name:
            return {
                "text":name,
                "state":{
                    "opened":True}
            }
        else:
            return {
                "text":name.replace('%','.'),
                "icon" : "jstree-file",
            }
        
def struct():
    conn.commit()
    cursor.execute('Select * from struct;')
    v=cursor.fetchall()
    #print(v)
    data = v
    result = defaultdict(dict)
    children = set()
    for parent, child in data:
        result[getname(parent)][getname(child)] = result[getname(child)]
        children.add(getname(child))
    for child in children:
        del result[child]
    dp=dict(result)
    if len(list(dp.keys()))==0:
        return {
                "text":"root",
                "state":{
                    "opened":True}
            } 
    else:
        tree=for_folder("root",dp['root'])

    return tree






    