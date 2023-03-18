import requests as r
#import pandas
#import random
namenode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/Namenode/'
structure_link='https://happinessq-fc061-default-rtdb.firebaseio.com/structure/root/'
datanode_link='https://happinessq-fc061-default-rtdb.firebaseio.com/datanode/'

def for_folder(name,data):

    if data==None:
        return {
                "text":name,
                "state":{
                    "opened":True}
            } 
    if data!="":
        if '%' not in name:
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
        if '%' not in name:
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

    tree={"root":r.get(structure_link+'.json').json()}
    print(tree)
    tree=for_folder("root",tree["root"])

    return tree

