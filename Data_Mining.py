import time
start_time = time.time()
#Connecting to teradata
import teradata
udaExec = teradata.UdaExec (appName="HelloWorld", version="1.0",logConsole=False)
session = udaExec.connect(method="odbc", system="Enter System IP Address",username="Enter Username", password="Enter Password");
 

import pandas as pd
import time
df=pd.read_csv('\\data.csv')


#Concat values for parameter
df['INPUT1'] = df[['DatabaseName', 'TableName']].apply(lambda x: '.'.join(x), axis=1)
df['INPUT'] = df[['VIEWNAME', 'TableName']].apply(lambda x: '.'.join(x), axis=1)



import pandas as pd
df1=pd.read_csv('\\search.csv')
df1 = df1.applymap(str)
print("Starting with Search...Live Mining trough all the tables in database...")


for row in df1.itertuples(index=True, name='Pandas'):
    string=getattr(row, "search")
    dfx = pd.DataFrame(columns=['TableName','ColumnName','Comment', 'Database Schema'])
    print("Starting search for",string)
    print("********"+"DatabaseName.TableName"+"*****************"+"ColumnName","*****************"+"String Present or not"+"*************")
    print("****************************************************************************************************************")
    start_time1 = time.time()
    #Starting of search loop
    for i, z, y in zip (df.INPUT, df.DatabaseName, df.TableName):
        ip=i
        #print(i, z, y)
        try:
            column = ("sel distinct trim(ColumnName) as ColumnName from database_metadata.columns where  trim(DATABASENAME) LIKE '{0}' ".format(z) + "AND trim(TABLENAME) like '{0}' ".format(y)+ "AND trim(COLUMNTYPE) NOT IN ('Enter Datatypes here like Timestamp,Measure values etc which are not required');")
            dfc = pd.read_sql(column,session)
            count=("SELECT count(*) as c FROM {0};".format(ip))
            df2 = pd.read_sql(count,session)
            if (int(df2['c'].iloc[0])==0):
                print(z,ip,"              N/A","                   Contains no data")
                dfx.loc[-1] = [z,ip, 'N/A', "Contains no data", z]
                dfx.index = dfx.index + 1
            else:
                for ip1 in dfc.ColumnName:
                    countc=("SELECT count(*) as count_num FROM {0} ".format(ip)+"WHERE CAST({0} as VARCHAR(2000)) ".format(ip1)+" like '{0}'; ".format(string))
                    df3 = pd.read_sql(countc,session)
                    if (int(df3['count_num'].iloc[0]) > 0): 
                        print (z,ip,"           ",ip1,"                   Contains given substring ")
                        dfx.loc[-1] = [ip, ip1, "Contains given substring", z]
                        dfx.index = dfx.index + 1
                        dfx.sort_index(inplace=True)
                    else: 
                        print (z,ip,"           ", ip1,"                   Doesn't contains given substring")
                        dfx.loc[-1] = [ip,ip1, "Doesn't contains given substring", z]
                        dfx.index = dfx.index + 1
                        dfx.sort_index(inplace=True)
        except:
            print(z,ip,"              N/A","                   Doesnt exist in DB/Error in Metadata/Access not there for the user")
            dfx.loc[-1] = [ip, 'N/A', "Doesnt exist in DB/Error in Metadata/Access not there for the user", z]
            dfx.index = dfx.index + 1
            dfx.sort_index(inplace=True)

    export_csv = dfx.to_csv (r'\\{0}.csv'.format(string), index = None, header=True)
    del(dfx)

    print("Time taken for mining: %s seconds" % (time.time() - start_time1))
print("***************************************************************************************")
print(" Total Time taken for mining: %s seconds" % (time.time() - start_time))
