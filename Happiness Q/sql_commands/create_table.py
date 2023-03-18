from connect import connect_sql

conn=connect_sql()
cursor=conn.cursor()

cursor.execute("CREATE TABLE namenode (node_id int auto_increment, type varchar(100) not null, name varchar(500) UNIQUE not null, primary key(node_id), CHECK (type in ('FILE','DIRECTORY')));")
cursor.execute("CREATE TABLE struct (parent int, child int, primary key(parent,child), foreign key (parent) references namenode(node_id),foreign key (child) references namenode(node_id));")
cursor.execute("Insert into namenode (type,name) values ('DIRECTORY','/');")
cursor.execute("CREATE TABLE Part (node_id INT,p_id int primary key,p_no int, foreign key (node_id) references namenode(node_id));")

conn.commit()