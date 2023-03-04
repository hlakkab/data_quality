import os
import mysql.connector
import pandas as pd
from tabulate import tabulate
import mytests
from dataqualitychecks import check_for_nulls
from dataqualitychecks import check_for_min_max
from dataqualitychecks import check_for_valid_values
from dataqualitychecks import check_for_duplicates
from dataqualitychecks import run_data_quality_check


conn = mysql.connector.connect(host="localhost", user="root", password="", database="data_processing")
"""
    cursor = connection.cursor()
query = "select * from dimcustomer limit 5 "
cursor.execute(query)
result =cursor.fetchall()
df =pd.DataFrame(result)
print(df)
"""

results = []
tests = {key:value for key,value in mytests.__dict__.items() if key.startswith('test')}
print(tests)
for testname,test in tests.items():
    test['conn'] = conn
    results.append(run_data_quality_check(**test))
    print(test)
    print('hmiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiiim9')

#print(results)
df=pd.DataFrame(results)
df.index+=1
df.columns = ['Test Name', 'Table','Column','Test Passed']
print(tabulate(df,headers='keys',tablefmt='psql'))
#End of data quality checks
conn.close()
print("Disconnected from data warehouse")
