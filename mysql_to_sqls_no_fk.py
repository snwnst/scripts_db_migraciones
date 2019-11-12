
from tqdm import tqdm
import mysql.connector
import psycopg2
import datetime

def create_table(table_name):
    cratetablestr = 'CREATE TABLE {} ('.format(table_name)
    mysql.execute("DESCRIBE {}.{}".format(scheme,table_name))
    columnsinfo = mysql.fetchall()
    constranint = ''
    for columinfo in columnsinfo:
        if 'int' in str(columinfo[1]):
            cratetablestr = '{} {} INT {},'.format(cratetablestr,columinfo[0],columinfo[2])
        else:
            if 'datetime' in str(columinfo[1]):
                cratetablestr = '{} {} timestamp {},'.format(cratetablestr,columinfo[0],columinfo[2])
            else:
                cratetablestr = '{} {} {} {},' .format(cratetablestr,columinfo[0],columinfo[1].upper(),columinfo[2])   
        if 'PRI' in str(columinfo[3]):
            constranint =  '{} {},'.format(constranint,str(columinfo[0]))
    if constranint != '':
        constranint = '{}@'.format(constranint)
        constranint = constranint.replace(",@", "")
        constranint = 'CONSTRAINT PK_{} PRIMARY KEY ({})'.format(table_name,constranint)
        cratetablestr = '{} {}'.format(cratetablestr,constranint)
    cratetablestr = cratetablestr.replace("NO", "NOT NULL")
    cratetablestr = cratetablestr.replace("YES", "")
    cratetablestr = cratetablestr.replace("TINYTEXT", "VARCHAR(255)")
    cratetablestr = cratetablestr.replace("DOUBLE", "FLOAT")
    cratetablestr = '{});'.format(cratetablestr)
    msqls.execute(cratetablestr)
    msqlsconnection.commit()

def insert_data(table_name):
    mysql.execute("DESCRIBE {}.{}".format(scheme,table_name))
    columnames = mysql.fetchall()
    query = 'INSERT INTO {} ('.format(table_name)
    for columname in columnames:
        query = query+columname[0]+','
    query = query+')'
    query = query.replace(",)", ")")
    mysql.execute("SELECT * FROM {}.{}".format(scheme,table_name))
    rows = mysql.fetchall()
    persent_rows_table = tqdm(rows)
    for row in persent_rows_table:
        if 'datetime.date' in str(row):
            for idx, subrow in enumerate(row):
                if isinstance(subrow, datetime.date) == True:
                    lst = list(row)
                    lst[idx] = str(subrow)
                    row = tuple(lst)
        msqls.execute(query+" VALUES "+str(row).replace("None", "NULL"))
        msqlsconnection.commit()
        persent_rows_table.set_description("MIGRANDO %s" % table_name)

newscheme = 'dbo'
scheme = 'terracota'

mysqlconnection = mysql.connector.connect(host='46.105.122.133',database='terracota',user='nmartinez',password='Terra2019')
mysql = mysqlconnection.cursor()

msqlsconnection = psycopg2.connect("host=167.172.216.50 dbname=TERRASAM user=root password=@H1lcotadmin")
msqls = msqlsconnection.cursor() 

mysql.execute("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema='{}'".format(scheme))
tablenames = mysql.fetchall()

array_table_names = []

for tablename in tablenames:
    create_table(tablename[0])
    insert_data(tablename[0])