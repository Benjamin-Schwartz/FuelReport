import string
import pandas as pd
import openpyxl
import sys
import pyodbc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from pandasql import sqldf
import numpy as np

mode = "AtHome"

def connect():
    DRIVER_NAME = 'SQL Server'
    SERVER_NAME = 'SQLDATA'
    DATABASE_NAME = 'Rehab'

    
    #Connecting paramaters
    connection_string = f"""
            DRIVER={{{DRIVER_NAME}}};
            SERVER={SERVER_NAME};
            DATABASE={DATABASE_NAME};
            Trusted_Connection = yes;
            uid = WNSM\ben.schwartz;
            pwd = Dish929292Intern;
        """
   
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
     
    engine = create_engine(connection_url)
      
    return engine
        

report = pd.DataFrame(columns = ['Branch', 'Area', 'Region', 
                            'Lease_Id', 'Year', 'Make', 
                            'Model', 'VIN', 'Plate_Number',
                            'Full_Name', 'Employee_Number', 'Role', 'Vehicle_Status',
                            'Azuga_Device','Blackout' , 'Covered', 
                            'UL mileage','Azuga Mileage', 'Miles',
                            'Tech Miles','Idle Miles','Idle Minutes', 
                            'Idle %', 'MPG', 'PPG',
                            'Fuel Spend','Weekend mileage', 'off-hour mileage', 
                            'Derive eligible?', 'Derive completed', 'Tech Activity'])


if mode != "AtHome":
    Branch_Area = pd.read_sql("""SELECT Branch_ID,
                           Region,
                           ActualRegion
                            FROM  rehab.A_BranchOffices_rtbl
                           """, connect())
else:
    Branch_Area = pd.read_csv("branch.csv")



UL_PowerBi = pd.read_excel('Ul_PowerBI.xlsx')
Vehicle_Report = pd.read_csv('VehiclesReport.csv')
Report_ul = pd.read_csv('Report.csv')
blackout = pd.read_csv("Report-Blackout.csv")
export = pd.read_csv("Export.csv")
Azuga_trips = pd.read_csv("TRIPS_REPORT-1821259c2f8.xlsx.csv",encoding = "ISO-8859-1", engine='python' )



page1 = pd.read_csv('Page1.csv' ,encoding = "ISO-8859-1", engine='python' )

#Delete leading 0's in Branch
report['Branch'] = UL_PowerBi['Branch'].str.replace('^\D*0*', "", regex = True)

#Look at report_before_join.csv

#Look at branch.csv These are the two we are joining
#Branch_Area.to_csv("branch.csv", index = False)



report['Lease_Id'] = UL_PowerBi['Lease Id']
report['Year'] = UL_PowerBi['Year']
report['Make'] = UL_PowerBi['Make'].str.lower()
report['Model'] = UL_PowerBi['Model']
report['VIN'] = UL_PowerBi['VIN']
report['Plate_Number'] = UL_PowerBi['Plate Number']
report['Full_Name'] = UL_PowerBi['Full Name']
report['Employee_Number'] = UL_PowerBi['Employee Id']
report['Vehicle_Status'] = UL_PowerBi['Vehicle Status']


Car_Brands = ['honda',
'chevrolet',
'ford',
'mercedes-Benz',
'jeep',
'bmw',
'porsche',
'subaru',
'nissan',
'cadillac',
'volkswagen',
'lexus',
'ram',
'buick',
'toyota',
'dodge',
'gmc',
'chrysler'
]



report = sqldf("""
                  SELECT 
                  Branch_Area.Branch_ID,
                  Branch_Area.Region,
                  Branch_Area.ActualRegion,
                  report.Lease_ID,
                  report.Year,
                  report.Make,
                  report.Model,
                  report.VIN,
                  report.Plate_Number,
                  report.Full_Name,
                  report.Employee_Number,
                  report.Vehicle_Status
                  FROM Branch_Area
                  LEFT JOIN report
                  ON  Branch_Area.Branch_ID = report.Branch 
""")


report = report[(report['Make']).isin(Car_Brands)]
report = report[(report['Vehicle_Status'] == "Active") |
                (report['Vehicle_Status'] == "Service Only")]


#Getting Role from page1 Table
report = pd.merge(report.astype(str), page1[['Job Title','Employee Number']].astype(str), how = 'left', left_on = "Employee_Number", right_on = "Employee Number" ).drop('Employee_Number', axis = 1)
report = pd.merge(report.astype(str), Vehicle_Report[['Device Serial Number', 'VIN']].astype(str), how = 'left', on = "VIN" )

report['Device Serial Number'] = report['Device Serial Number'].astype(str).apply(lambda x: x.replace('.0',''))
report = pd.merge(report.astype(str), blackout[['Blackout since', 'Device S/N']].astype(str), how='left', left_on="Device Serial Number", right_on="Device S/N").drop('Device S/N', axis=1)


cover_filter = []

for i in range (len(report['Blackout since'])):
    if ((report['Blackout since'][i] != None and report['Blackout since'][i] != "Not Activated") and report['Device Serial Number'][i] != "nan"):
        cover_filter.append(1)
    else:
        cover_filter.append(0)



report['Covered'] = cover_filter



report = pd.merge(report.astype(str), Report_ul[['Miles Driven', 'VIN']].astype(str), how='left', on="VIN")


report['Miles Driven'] = report['Miles Driven'].str.replace(',','')
report['Miles Driven'] = report['Miles Driven'].fillna(0)
report['Miles Driven'] = report['Miles Driven'].astype(str).apply(lambda x: x.replace('.0','')).astype(int)

#columns_to_group = report.columns.difference(['Miles Driven'])

report = report.groupby(['Branch_ID', 'Region', 'ActualRegion', 'Lease_Id', 
                         'Year', 'Make',  'Model',  'VIN',  
                        'Plate_Number',  'Full_Name', 'Employee Number', 'Job Title',
                      'Device Serial Number', 'Blackout since',  'Covered']).sum().reset_index()


report = pd.merge(report.astype(str), Azuga_trips[['TOTAL DISTANCE TRAVELED (MILES)', 'VIN']].astype(str), how='left', on="VIN")

report.rename(columns = {'TOTAL DISTANCE TRAVELED (MILES)':'Azuga Mileage'}, inplace = True)
report['Azuga Mileage'] = report['Azuga Mileage'].str.replace(',','')
report['Azuga Mileage'] = report['Azuga Mileage'].fillna(0)
report['Azuga Mileage'] = report['Azuga Mileage'].astype(float)

#columns_to_group = report.columns.difference(['Miles Driven'])

report = report.groupby(['Branch_ID', 'Region', 'ActualRegion', 'Lease_Id', 
                         'Year', 'Make',  'Model',  'VIN',  
                        'Plate_Number',  'Full_Name', 'Employee Number', 'Job Title',
                      'Device Serial Number', 'Blackout since',  'Covered' ,'Miles Driven'])['Azuga Mileage'].sum().reset_index()
#print(report)


#Column for miles we are concerned with
miles = []
for i in range (len(report['Covered'])):
    if (report['Covered'][i] == "0"):
        miles.append(report['Miles Driven'][i])
    else:
        miles.append(report['Azuga Mileage'][i])

report['Miles'] = miles



report = pd.merge(report.astype(str), Azuga_trips[['TOTAL IDLE TIME', 'VIN']].astype(str), how='left', on="VIN")
time = pd.DatetimeIndex(report['TOTAL IDLE TIME'])
time = time.hour * 60 + time.minute + (time.second / 60)


report['TOTAL IDLE TIME'] = time

report = report.groupby(['Branch_ID', 'Region', 'ActualRegion', 'Lease_Id', 
                         'Year', 'Make',  'Model',  'VIN',  
                        'Plate_Number',  'Full_Name', 'Employee Number', 'Job Title',
                      'Device Serial Number', 'Blackout since',  'Covered' ,'Miles Driven', 'Azuga Mileage', 'Miles'])['TOTAL IDLE TIME'].sum().reset_index()



report = pd.merge(report.astype(str), Azuga_trips[['TOTAL DRIVE TIME', 'VIN']].astype(str), how='left', on="VIN")
time = pd.DatetimeIndex(report['TOTAL DRIVE TIME'])
time = time.hour * 60 + time.minute + (time.second / 60)


report['TOTAL DRIVE TIME'] = time

report = report.groupby(['Branch_ID', 'Region', 'ActualRegion', 'Lease_Id', 
                         'Year', 'Make',  'Model',  'VIN',  
                        'Plate_Number',  'Full_Name', 'Employee Number', 'Job Title',
                      'Device Serial Number', 'Blackout since',  'Covered' ,'Miles Driven', 'Azuga Mileage', 'Miles', 'TOTAL IDLE TIME' ])['TOTAL DRIVE TIME'].sum().reset_index()


report['% Idle'] = report['TOTAL IDLE TIME'] .astype(float) / report['TOTAL DRIVE TIME'].astype(float) * 100




report = pd.merge(report.astype(str), Azuga_trips[['FUEL USED (GALLONS)', 'VIN']].astype(str), how='left', on="VIN")

report['FUEL USED (GALLONS)'] = report['FUEL USED (GALLONS)'].fillna(0)
report['FUEL USED (GALLONS)'] = report['FUEL USED (GALLONS)'].astype(float)


report = report.groupby(['Branch_ID', 'Region', 'ActualRegion', 'Lease_Id', 
                         'Year', 'Make',  'Model',  'VIN',  
                        'Plate_Number',  'Full_Name', 'Employee Number', 'Job Title',
                      'Device Serial Number', 'Blackout since',  'Covered','Miles Driven', 'Azuga Mileage', 'Miles', 'TOTAL IDLE TIME'])['FUEL USED (GALLONS)'].sum().reset_index()

report['MPG'] = report['Azuga Mileage'].astype(float) / report['FUEL USED (GALLONS)'].astype(float)


report = pd.merge(report.astype(str), Azuga_trips[['VOC ($)', 'VIN']].astype(str), how='left', on="VIN")


report['VOC ($)'] = report['VOC ($)'].fillna(0)
report['VOC ($)'] = report['VOC ($)'].astype(float)


report = report.groupby(['Branch_ID', 'Region', 'ActualRegion', 'Lease_Id', 
                         'Year', 'Make',  'Model',  'VIN',  
                        'Plate_Number',  'Full_Name', 'Employee Number', 'Job Title',
                      'Device Serial Number', 'Blackout since',  'Covered','Miles Driven', 'Azuga Mileage', 'Miles', 'TOTAL IDLE TIME', 'FUEL USED (GALLONS)'])['VOC ($)'].sum().reset_index()



#Getting eval @ home %
report = pd.merge(report.astype(str), export[['Eval @ Home %', 'Branch_ID']].astype(str), how = 'left',  on = "Branch_ID" )

print(report)

report.to_csv("test2.csv", index = False)