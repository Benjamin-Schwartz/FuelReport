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
        



#Initial database these are all of the columns that we are concerned about
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


#Mode variable is so I can work at home on my PC without needing to pull from database will remove when program is in development mode
if mode != "AtHome":
    Branch_Area = pd.read_sql("""SELECT Branch_ID,
                           Region,
                           ActualRegio
                            FROM  rehab.A_BranchOffices_rtbl
                           """, connect())
else:
    Branch_Area = pd.read_csv("../files/branch.csv")




#Reading from multiple csvs creating dataframes to manipulate later
UL_PowerBi = pd.read_excel('../files/Ul_PowerBI.xlsx')
Vehicle_Report = pd.read_csv('../files/VehiclesReport.csv')
Report_ul = pd.read_csv('../files/Report.csv')
blackout = pd.read_csv("../files/Report-Blackout.csv")
export = pd.read_csv("../files/Export.csv")
Azuga_trips = pd.read_csv("../files/test.csv",encoding = "ISO-8859-1", engine='python' )
page1 = pd.read_csv('../files/Page1.csv' ,encoding = "ISO-8859-1", engine='python' )
NSM_table = pd.read_csv('../files/National Seating Mobility - NSM.csv')


#Pulling all initial data and populating columns with information from Union Leasing PowerBi
report['Branch'] = UL_PowerBi['Branch'].str.replace('^\D*0*', "", regex = True)
report['Lease_Id'] = UL_PowerBi['Lease Id']
report['Year'] = UL_PowerBi['Year']
report['Make'] = UL_PowerBi['Make'].str.lower()
report['Model'] = UL_PowerBi['Model']
report['VIN'] = UL_PowerBi['VIN']
report['Plate_Number'] = UL_PowerBi['Plate Number']
report['Full_Name'] = UL_PowerBi['Full Name']
report['Employee_Number'] = UL_PowerBi['Employee Id']
report['Vehicle_Status'] = UL_PowerBi['Vehicle Status']


#Need Charlie to look at these and see 
#These are all of the relevant car Brands if there is anything else we don't care about it
#Could be something like a trailer and not a vehicle this report only concerns vehicles that use gas
Car_Brands = ['honda',
'chevrolet',
'ford',
'mercedes-benz',
'subaru',
'nissan',
'volkswagen',
'ram',
'buick',
'toyota',
'dodge',
'gmc',
'chrysler'
]


#Creates an initial table called report
#IT pulls the Branch_ID, Area, and Region from NSM SQL Database 
#Also other information from 
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


#Removing all rows that don't have vehicles that is contained in the carBrand list
report = report[(report['Make']).isin(Car_Brands)]

report = report[(report['Vehicle_Status'] == "Active") |
                (report['Vehicle_Status'] == "Service Only")]


#Getting Role from page1 Table
report = pd.merge(report.astype(str), page1[['Job Title','Employee Number']].astype(str), how = 'left', left_on = "Employee_Number", right_on = "Employee Number" ).drop('Employee_Number', axis = 1)

#Geting Azuga Device number Called Device Serial Number in the table we're pulling from
report = pd.merge(report.astype(str), Vehicle_Report[['Device Serial Number', 'VIN']].astype(str), how = 'left', on = "VIN" )


#Getting rid of decimals in the Device Serial Number they are not suppose to be there they are generated from the previous Merge
report['Device Serial Number'] = report['Device Serial Number'].astype(str).apply(lambda x: x.replace('.0',''))

#Getting Blackout Data based off the Device Serial Number
report = pd.merge(report.astype(str), blackout[['Blackout since', 'Device S/N']].astype(str), how='left', left_on="Device Serial Number", right_on="Device S/N").drop('Device S/N', axis=1)


#This list will contain which rows will have a 1 or 0 in the Covered column
cover_filter = []

#If blackout is Activated and it has a Serial Number then it is Covered otherwise it is Not
for i in range (len(report['Blackout since'])):
    if ((str(report['Blackout since'][i]) == "nan") and report['Device Serial Number'][i] != "nan"):
        cover_filter.append(1)
    else:
        cover_filter.append(0)

#Append this Covered Dataframe to the report
report['Covered'] = cover_filter



#Getting all of the Miles driven for each individual trip
#This is unclean data It will create duplicates because each VIN has multiple trips associated with it.
#Data also will contain commas and null valeus for trips with no miles
report = pd.merge(report.astype(str), Report_ul[['Miles Driven', 'VIN']].astype(str), how='left', on="VIN")
#Cleaning data getting rid of commas and converting the data to numbers for arithmetic
report['Miles Driven'] = report['Miles Driven'].str.replace(',','')
report['Miles Driven'] = report['Miles Driven'].fillna(0)
report['Miles Driven'] = report['Miles Driven'].astype(str).apply(lambda x: x.replace('.0','')).astype(int)

#All columns besides Miles Driven
columns_to_group = list(report.columns.difference(['Miles Driven']))

#Get rid of Duplicate rows and get the sum for all Miles Driven
#Example
#VIN NAME MILES
#123   BOB    1000
#234  ALICE   500
#123   BOB    1000

#This turns into

#VIN  NAME MILES
#123  BOB  2000
#234 ALICE 500
report = report.groupby(columns_to_group).sum().reset_index()

#Azuga Distance
#Azuga Idle Time
#Azuga Trip Time
#Getting the miles driven from Azuga Data
# Azuga_trips.rename(columns={'vehicleName': 'VIN'}, inplace=True)
Azuga_trips['idleTime'] = Azuga_trips['idleTime'].str.split(' ').str[0]

report = pd.merge(report.astype(str), Azuga_trips[['idleTime', 'VIN']].astype(str), how='left', on = 'VIN')
report.rename(columns = {'idleTime':'Azuga Mileage'}, inplace = True)
report['Azuga Mileage'] = report['Azuga Mileage'].fillna(0)
report['Azuga Mileage'] = report['Azuga Mileage'].astype(float)

columns_to_group = report.columns.difference(['Azuga Mileage'])
#Remove Duplicates and sum up Azuga Mileage same logic as above see Example above
report = report.groupby(list(columns_to_group))['Azuga Mileage'].sum().reset_index()


Azuga_trips['idleTime'] = Azuga_trips['idleTime'].str.split(
    ' ').str[0]

report = pd.merge(report.astype(str), Azuga_trips[[
                  'idleTime', 'VIN']].astype(str), how='left', on='VIN')
report['idleTime'] = report['idleTime'].fillna(0)
report['idleTime'] = report['idleTime'].astype(float)

columns_to_group = report.columns.difference(['idleTime'])
#Remove Duplicates and sum up idleTime same logic as above see Example above
report = report.groupby(list(columns_to_group))[
    'idleTime'].sum().reset_index()


Azuga_trips['tripTime'] = Azuga_trips['tripTime'].str.split(
        ' ').str[0]

report = pd.merge(report.astype(str), Azuga_trips[[
                  'tripTime', 'VIN']].astype(str), how='left', on='VIN')
report['tripTime'] = report['tripTime'].fillna(0)
report['tripTime'] = report['tripTime'].astype(float)

columns_to_group = report.columns.difference(['tripTime'])
#Remove Duplicates and sum up tripTime same logic as above see Example above
report = report.groupby(list(columns_to_group))[
    'tripTime'].sum().reset_index()

#Column for miles we are concerned with
#If not covered get UL Miles otherwise get Azuga Mileage
miles = []
for i in range (len(report['Covered'])):
    if (report['Covered'][i] == "0"):
        miles.append(report['Miles Driven'][i])
    else:
        miles.append(report['Azuga Mileage'][i])

#Add these miles to the table
report['Miles'] = miles




#Getting Fuel spend from UL_Report
Report_ul['Total Amount'] = Report_ul['Total Amount'].str.replace('$', '', regex=False)
report = pd.merge(report.astype(
    str), Report_ul[['Total Amount', 'VIN']].astype(str), how='left', on="VIN")
report['Total Amount'] = report['Total Amount'].fillna(0)

report['Total Amount'] = report['Total Amount'].astype(float)
#Removing Duplicate rows and summing VOC
columns_to_group = list(report.columns.difference(['Total Amount']))
report = report.groupby(columns_to_group)[
    'Total Amount'].sum().reset_index()

#Getting PPG based on the VIn for each vehicle
Report_ul['Price Per Unit'] = Report_ul['Price Per Unit'].str.replace('$','', regex= False)
report = pd.merge(report.astype(str), Report_ul[['Price Per Unit', 'VIN']].astype(str), how='left', on="VIN")
report['Price Per Unit'] = report['Price Per Unit'].fillna(0)

report['Price Per Unit'] = report['Price Per Unit'].astype(float)
#Removing Duplicate rows and summing VOC
columns_to_group = list(report.columns.difference(['Price Per Unit']))
report = report.groupby(columns_to_group)['Price Per Unit'].mean().reset_index()

#Getting eval @ home %
report = pd.merge(report.astype(str), export[['Eval @ Home %', 'Branch_ID']].astype(str), how = 'left',  on = "Branch_ID" )
report['Eval @ Home %'] = report['Eval @ Home %'].fillna('')


eligible = []
completed = []
5
nsm_df = NSM_table['Status']

for i in range (len(report['VIN'])):
    if (report['VIN'][i] in (list(NSM_table['VIN']))):
        eligible.append('1')
        if(NSM_table['Status'][NSM_table.loc[NSM_table['VIN'] == report['VIN'][i]]['Status'].index[0]] == 'DERIVE OPTIMIZED'):
            completed.append('1')
        else:
            completed.append('0')

    else:
        completed.append('0')
        eligible.append('0')
     



report['Derive completed?'] = completed
report['Derive eligible?'] = eligible

print(report['tripTime'])

report['idle %'] = report['tripTime'].astype(float) / report['idleTime'].astype(float)
# report['PPG'] = report['VOC ($)'].astype(float) / (report['Miles Driven'].astype(float) / report['MPG'].astype(float))

Gallons = (report['Total Amount'].astype(float)) / (report['Price Per Unit'].astype(float))
report['MPG'] = (report['Miles Driven'].astype(float) / Gallons)
report = report.loc[:, ["Branch_ID","Region","ActualRegion","Lease_Id",
 "Year", "Make", "Model", "VIN", "Plate_Number", "Full_Name", 
 "Employee Number", "Job Title", "Device Serial Number", "Blackout since", 
 "Covered", "Miles Driven", "Azuga Mileage", "Miles", "idleTime", 'idle %', 'MPG', 'Price Per Unit', 'Total Amount', 'Derive eligible?', "Derive completed?", "Eval @ Home %"]]



report = report.rename(columns = {'Branch_ID':'Branch', 
                        'Region':'Area',
                        'ActualRegion':'Region',
                        'Lease_Id': 'Lease Id',
                        'Plate_Number': 'Plate Number',
                        'Full_Name': 'Full Name',
                        'Employee Number': 'Employee Id',
                        'Job Title': 'Role',
                        'Device Serial Number': 'Azuga Device',
                        'Blackout since': 'Black Out',
                        'Miles Driven': 'UL mileage',
                        'idleTime': 'Idle Minutes',
                        'Price Per Unit': 'PPG',
                        'Total Amount': 'Fuel Spend'})

report['PPG'] = report['PPG'].str.replace(
    '0', '', regex=False)
report = report.replace(np.nan, '', regex=True)
report = report.replace(['nan'], '', regex=True)
report.to_csv("../Reports/FuelReport.csv", index = False)