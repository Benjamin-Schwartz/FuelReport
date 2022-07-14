import pandas as pd
import openpyxl
import sys
import pyodbc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import URL
from sqlalchemy import create_engine
from pandasql import sqldf

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
                            'Full_Name', 'Employee_Number', 'Role',
                            'Azuga Device','Black Out' , 'Covered', 
                            'UL mileage','Azuga Mileage', 'Miles',
                            'Tech Miles','Idle Miles','Idle Minutes', 
                            'Idle %', 'MPG', 'PPG',
                            'Fuel Spend','Weekend mileage', 'off-hour mileage', 
                            'Derive eligible?', 'Derive completed', 'Tech Activity'])


Branch_Area = pd.read_sql("""SELECT Branch_ID,
                           Region,
                           ActualRegion
                            FROM  rehab.A_BranchOffices_rtbl
                           """, connect())



UL_PowerBi = pd.read_excel('Ul_PowerBI.xlsx')
Vehicle_Report = pd.read_csv('VehicleReport.csv')
blackout = pd.read_csv("Report-Blackout.csv")

page1 = pd.read_csv('FuelReportPage1.csv' ,encoding = "ISO-8859-1", engine='python' )

#Delete leading 0's in Branch
report['Branch'] = UL_PowerBi['Branch'].str.replace('^\D*0*', "", regex = True)

#Look at report_before_join.csv
report.to_csv("report_before_join.csv", index = False) 

#Look at branch.csv These are the two we are joining
Branch_Area.to_csv("branch.csv", index = False)



report['Lease_Id'] = UL_PowerBi['Lease Id']
report['Year'] = UL_PowerBi['Year']
report['Make'] = UL_PowerBi['Make']
report['Model'] = UL_PowerBi['Model']
report['VIN'] = UL_PowerBi['VIN']
report['Plate_Number'] = UL_PowerBi['Plate Number']
report['Full_Name'] = UL_PowerBi['Full Name']
report['Employee_Number'] = UL_PowerBi['Employee Id']


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
                  report.Employee_Number
                  FROM Branch_Area
                  LEFT JOIN report
                  ON  Branch_Area.Branch_ID = report.Branch 
""")

#Getting Role from page1 Table
role = pd.merge(report.astype(str), page1.astype(str), how = 'inner', left_on = "Employee_Number", right_on = "Employee Number" )
report['Role'] = role["Job Title"]

#Getting Azuga Device
Azuga_device = pd.merge(report.astype(str), Vehicle_Report.astype(str), how = 'inner', on = "VIN" )
Azuga_device['Device Serial Number'] = Azuga_device['Device Serial Number'].str.replace("s/\.\d*//;", "", regex = True) #Get rid of everything after .
report['Azuga Device'] = Azuga_device["Device Serial Number"]


# blackout_df = pd.merge(report.astype(str), blackout.astype(str), how = 'inner', left_on = "Azuga Device" ,right_on = "Device S/N")
# print(blackout_df)

# report['Black Out'] = blackout_df["Blackout since"]

report.to_csv("test.csv", index = False)
