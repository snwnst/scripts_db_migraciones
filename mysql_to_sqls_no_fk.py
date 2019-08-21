import mysql.connector
import pyodbc 
import datetime

MysqlHost = ''
MysqlDatabase = ''
MysqlUser = ''
MysqlPassword = ''

SqlsHost = ''
SqlsDatabase = ''
SqlsUser = ''
SqlsPassword = ''

scheme = ''

mysqlconnection = mysql.connector.connect(host=MysqlHost,database=MysqlDatabase,user=MysqlUser,password=MysqlPassword)
mysql = mysqlconnection.cursor()

msqlsconnection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER={};DATABASE={};UID={};PWD={}'.format(SqlsHost,SqlsDatabase,SqlsUser,SqlsPassword))
msqls = msqlsconnection.cursor() 

mysql.execute("SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema='{}'".format(scheme))
tablenames = mysql.fetchall()

hora_inicio = datetime.datetime.now()
for tablename in tablenames:
    datarows = 0
    hora_inicio_tabla = datetime.datetime.now()
    print('CREANDO TABLA {}...'.format(tablename[0]))
    cratetablestr = 'CREATE TABLE {}.{} ('.format(scheme,tablename[0])
    mysql.execute("DESCRIBE {}."+str(scheme,tablename[0]))
    columnsinfo = mysql.fetchall()
    constranint = ''
    for columinfo in columnsinfo:
        if 'int' in str(columinfo[1]):
            cratetablestr = '{} {} INT {},'.format(cratetablestr,columinfo[0],columinfo[2])
        else:
            cratetablestr = '{} {} {} {},' .format(cratetablestr,columinfo[0],columinfo[1].upper(),columinfo[2])   
        if 'PRI' in str(columinfo[3]):
            constranint =  '{} {},'.format(constranint,str(columinfo[0]))
    if constranint != '':
        constranint = '{}@'.format(constranint)
        constranint = constranint.replace(",@", "")
        constranint = 'CONSTRAINT PK_{} PRIMARY KEY ({})'.format(str(tablename[0]),constranint)
        cratetablestr = '{} {}'.format(cratetablestr,constranint)
    cratetablestr = cratetablestr.replace("NO", "NOT NULL")
    cratetablestr = cratetablestr.replace("YES", "")
    cratetablestr = cratetablestr.replace("TINYTEXT", "VARCHAR(255)")
    cratetablestr = cratetablestr.replace("DOUBLE", "FLOAT")
    cratetablestr = '{});'.format(cratetablestr)
    msqls.execute(cratetablestr)
    msqlsconnection.commit()
    mysql.execute("DESCRIBE {}.{}".format(scheme,tablename[0]))
    columnames = mysql.fetchall()
    print('INSERTANDO DATA EN {}.{}...'.format(scheme,tablename[0]))
    query = 'INSERT INTO {}.{} ('.format(scheme,tablename[0])
    for columname in columnames:
        query = query+columname[0]+','
    query = query+')'
    query = query.replace(",)", ")")
    mysql.execute("SELECT * FROM {}.{}".format(scheme,tablename[0]))
    rows = mysql.fetchall()
    for row in rows:
        if 'datetime.date' in str(row):
            for idx, subrow in enumerate(row):
                if isinstance(subrow, datetime.date) == True:
                    lst = list(row)
                    lst[idx] = str(subrow)
                    row = tuple(lst)
        msqls.execute(query+" VALUES "+str(row).replace("None", "NULL"))
        msqlsconnection.commit()
        datarows = datarows + 1
    hora_fin = datetime.datetime.now()
    print(str(datarows) + ' CELDAS INSERTADAS EN {}.{}'.format(scheme,tablename))
    print('TIEMPO TRANSCURRIDO: '+str(hora_fin - hora_inicio_tabla))
print('MIGRACION COMPLETA. TIEMPO TRANSCURRIDO: '+str(hora_fin - hora_inicio))
