import mysql.connector

def connect_sql():
    conn = mysql.connector.connect(
        host="localhost",
        database="dsci551_trial",
        user="root",
        password="Vansh0816" )

    
    return conn
    