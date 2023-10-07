"""
The reporting tool will try to create a transaction report for the given list of clients in the given period. 

PROGRAM STEPS:
1) Encrypt the database access PWD into sDecryptedText
2) Prompt user for their AD-ENT ID, transaction period, list of Client_IDs
3) Connect to Teradata database (EIWP)

4) [Codelines 121 - 217] Prepare SQL strings that will be executed in Teradata DB EIWP:
    4a)strAllAccounts: all accounts that link to the clients
    4b)strPriOwners: only the accounts that the clients are primary owners
    4c)strAlltran_count: count the number of observations without pulling all details
    4d)strAlltran_details: pull all transaction details 

5) Execute 4a), 4b), and 4c) to get the number of transaction count

6a)If number of transactions is <= 500K, then will prompt the number of transactions and ask if user still want to output
        YES -> execute 4d) and output result to excel
        NO  -> Exit 
6b)If number of transactions is >  500K, will prompt user this is too many obs and will not output the result to excel

7) DROP the tables created in Teradata database
END OF PROGRAM

OUTPUTS:
Excel file for the detail transactions (this depends on STEP 6).
Log file: log each step of the executions. Used for trouble shot.

FINAL NOTE: 
Use pyinstaller to compile this program in to an executable application for users,
So they dont need to install python to run the application. 
Required Teradata ODBC driver.
"""

import numpy as np
import pandas as pd
import pyodbc
import logging
import time
from easygui import *
import sys

# Create Encrypted String for login password to Teradata database

#from cryptography.fernet import Fernet
#cipher_key = Fernet.generate_key()
#print (cipher_key)
#cipher_key = b'38S4uhD0qyyC_NqPVaoXFWjIQX2-ijfxpKZhBbJVaqg='
#cipher = Fernet(cipher_key)
#text = b'a password'
#encrypted_text = cipher.encrypt(text)
#print (encrypted_text)
#decrypted_text = cipher.decrypt(encrypted_text)
#print(decrypted_text)

# Read password

from cryptography.fernet import Fernet
cipher_key = b'38S4uhD0qyyC_NqPVaoXFWjIQX2-ijfxpKZhBbJVaqg='
encryptedText = 'This is an encrypted text for a password used to log into Teradata'
bEncryptedText = encryptedText.encode('utf-8')
cipher = Fernet(cipher_key)
sDecryptedText = cipher.decrypt(bEncryptedText).decode("utf-8")
#print (sDecryptedText)


#Prompt: pop up window set up
msg = "Enter the required information"
title = "ALLTRAN Details for multiple Clients"
fieldNames = ["AD-ENT ID", "Start Date (yyyy-mm-dd)","End Date(yyyy-mm-dd)","WCIS IDs (separated by commas)"]  
fieldValues = []  # we start with blanks for the values
fieldValues = multenterbox(msg, title, fieldNames)

# make sure that none of the fields was left blank
while 1:
    if fieldValues == None: break # if user cancels the operation from above multenterbox
    errmsg = ""
    for i in range(len(fieldNames)):
      if fieldValues[i].strip() == "":
        errmsg = errmsg + ('"%s" is a required field.\n\n' % fieldNames[i])
    if errmsg == "": break # no problems found
    fieldValues = multenterbox(errmsg, title, fieldNames, fieldValues)
#print( "Values entered were:", fieldValues)

# Get all the values from the dialog box
strADENT = fieldValues[0].strip()                         
strStartDate = "'"+fieldValues[1].replace(" ", "")+"'"    
strEndDate = "'"+fieldValues[2].replace(" ", "")+"'"
strWCIS = fieldValues[3]
#print(strWCIS)

#Filtered empty WCIS ID. ex: user input "xx, ,yy,") 
strAcctNr = "(" + str(list(filter(None, strWCIS.replace(" ","").split(",")))).strip('[]') + ")"  
#print(strAcctNr)


#Create log file
strLogFileName = "WFCRC RIM - ALLTRAN Details - " + strADENT + " - " + time.strftime("%Y%m%d-%H%M%S")
logging.basicConfig(filename=strLogFileName+".log"
                   ,level=logging.INFO
                   ,format="%(asctime)s:%(levelname)s:%(message)s")

logging.info("Log file created\n")
logging.info("Values entered were: "+str(fieldValues))
logging.info("User ID: "+strADENT)
logging.info("Start Date: "+strStartDate)
logging.info("End Date: "+strEndDate)
logging.info("WCIS IDs from input: "+strWCIS)
logging.info("WCIS IDs used in generating the report: "+strAcctNr+"\n")  #for double check if there is any different from input


# Connect to Teradata - EIWP
#print("Connecting to EIWP")
logging.info("Connecting to EIWP")
dcEIW = pyodbc.connect('DSN=EIWP-LDAP;UID=u123456'+';PWD='+sDecryptedText)   
cursor = dcEIW.cursor()
dcEIW.autocommit = True
#print("Connection Successful")
logging.info("EIW Connection Successful\n")


#Preparing string variables for SQL codes
logging.info("Preparing SQL codes")
#STEP 4a) SQL: Get all accounts related to the customers:
# "..." represents omitted SQL code
strAllAccounts = '''
CREATE TABLE adwp_work1.all_accounts_'''+ strADENT + ''' AS ( 
    SELECT ...
    FROM ...
    WHERE ...
      AND dil.Efct_Dt  <= ''' +  strEndDate +  '''  
      AND  dil.Expr_DT >= ''' +  strStartDate +  ''' 
) WITH DATA PRIMARY INDEX(...)        
'''
logging.info("Prepared SQL code, strAllAccounts")

#STEP 4b) SQL: Get primary accounts owners and their accounts:
# "..." represents omitted SQL code
strPriOwners = '''
CREATE TABLE adwp_work1.pri_owner_accounts_'''+ strADENT + ''' AS (
    SELECT ...
    FROM pri_accounts 
    INNER JOIN adwp_work1.all_accounts_'''+ strADENT + ''' AS a2
        ON ...
    ) WITH DATA PRIMARY INDEX(...)
'''
logging.info("Prepared SQL code, strPriOwners")

#STEP 4c) SQL: Count ALLTRAN number of records:
# "..." represents omitted SQL code
strAlltran_count = '''
WITH item_details AS
(
    SELECT ...
    FROM (sel * from ADWP_WORK1.pri_owner_accounts_'''+ strADENT + ''') AS CA 
    INNER JOIN (SELECT ... FROM ... WHERE ...) AS a
            ON ...
    INNER JOIN ADWDM_FIU_V1.DLY_TRAN_ITEM AS i 
            ON ...
    WHERE a.tran_dt BETWEEN ''' +  strStartDate +  ''' AND ''' +  strEndDate +  '''
)

SELECT SUM(rowCount) AS rowCount_total 
FROM 
( 
    SELECT COUNT(*) AS rowCount
    FROM  item_details i 
    WHERE ...

    UNION ALL 
    
    SELECT COUNT(*) AS rowCount
    FROM (sel * from ADWP_WORK1.pri_owner_accounts_'''+ strADENT + ''') AS CA 
    INNER JOIN ADWDM_FIU_V1.DLY_TRAN_ACCT AS a 
        ON ...
    WHERE 
        a.tran_dt BETWEEN ''' +  strStartDate +  ''' AND ''' +  strEndDate +  '''
        AND COALESCE(a.TRAN_TYPE_NM, 'NA') NOT IN (...) 
        AND i.unit_id IS NULL
) details
'''
logging.info("Prepared SQL code, strAlltran_count")

#STEP 4d)SQL: pull all requested transactions:
strAlltran_details = '''
WITH item_details AS
(
    SELECT ...
    FROM (SELECT * from ADWP_WORK1.pri_owner_accounts_'''+ strADENT + ''') AS CA 
    INNER JOIN (SELECT ... FROM ... WHERE ...) AS a
            ON ...
    INNER JOIN ADWDM_FIU_V1.DLY_TRAN_ITEM AS i 
            ON ...
    WHERE a.tran_dt BETWEEN ''' +  strStartDate +  ''' AND ''' +  strEndDate +  '''
)

SELECT details.* 
FROM 
( 
    SELECT  *
    FROM  item_details AS i 
    WHERE ...

    UNION ALL
    
    SELECT ...
    FROM (sel * from ADWP_WORK1.pri_owner_accounts_'''+ strADENT + ''') AS CA 
    INNER JOIN ADWDM_FIU_V1.DLY_TRAN_ACCT AS a 
        ON ...
    WHERE 
        a.tran_dt BETWEEN ''' +  strStartDate +  ''' AND ''' +  strEndDate +  '''
        AND COALESCE(a.TRAN_TYPE_NM, 'NA') NOT IN (...) 
        AND i.unit_id IS NULL 
    
) AS details
ORDER BY ...
'''  
logging.info("Prepared SQL code, strAlltran_details\n")


logging.info("Start retrieving info from EIW database")
try: 
    #STEP a, b, c for rowCount
    logging.info("Getting all accounts associated with the clients -- STEP a")
    cursor.execute(strAllAccounts)
    logging.info("STEP a completed")

    logging.info("Identifying primary owners accounts -- STEP b")
    cursor.execute(strPriOwners)
    logging.info("STEP b completed")

    logging.info("Counting number of records from ALLTRAN -- STEP c")
    dfAlltran_count = pd.read_sql(strAlltran_count, dcEIW)                  
    logging.info("STEP c completed\n\n")

    rowCount = dfAlltran_count.iat[0,0]
    logging.info("Total number of records: "+str(rowCount)+"\n\n")


    #Determine if need to pull all details
    #if number of record <= 500K, prompt user if they want all the records to excel
    #else number of record > 500K, exit without pulling all the records

    titleAlltran = "Alltran result"
    if rowCount <= 500000:
        msgAlltran = "Total number of records: " + str(rowCount) + "\n\nDo you want to output to excel?"
        if boolbox(msgAlltran, titleAlltran, ["Yes", "No"]):
            logging.info("User chooses to get Alltran details")

            #Pull all records and write to excel
            logging.info("Getting Alltran details")
            dfAlltran = pd.read_sql(strAlltran_details, dcEIW)
            logging.info("Retrieved Alltran details")

            logging.info("Preparing to write Alltran details to excel file")
            strFileName = "WFCRC RIM - ALLTRAN Details - " + strADENT + " - " + time.strftime("%Y%m%d-%H%M%S")
            #print(strFileName)
            writer = pd.ExcelWriter(strFileName + '.xlsx', engine='xlsxwriter', date_format = 'dd-mm-yy')
            dfAlltran.to_excel(writer,sheet_name = 'Alltran Details', index=False)
            writer.save()
            logging.info("Alltran details written to excel file\n")

        else:
            #print("Exit without writing to excel")
            logging.info("User chooses not to get Alltran details\n")

    #rowCount over limit, not write to excel
    else:
        msgAlltran2 = "Total number of records: " + str(rowCount) + "\n\nDetails not output to excel, since exceed 500K cap."
        msgbox(msgAlltran2, titleAlltran)
        #print(msgAlltran2)
        logging.info("Alltran details not output to excel, since exceed 500K cap\n")

except:
    logging.error('--- AN ERROR OCCURRED! ---')
    e = sys.exc_info()[0]
    logging.error("<p>Error: %s</p>" % e +"\n")

    titleError = "--- AN ERROR OCCURRED! ---"
    msgError = "An error occurred.\n\nTry enter shorter periods, or fewer WCIS IDs, and double check the values you enter."
    msgbox(msgError, titleError)

finally:
    try:
        #Drop all the tables if exist in the DB
        cursor.execute("drop table ADWP_WORK1.ALL_ACCOUNTS_"+strADENT)
        logging.info("Table ADWP_WORK1.ALL_ACCOUNTS_"+strADENT+" dropped successfully")

        cursor.execute("drop table ADWP_WORK1.PRI_OWNER_ACCOUNTS_"+strADENT)
        logging.info("Table ADWP_WORK1.PRI_OWNER_ACCOUNTS_"+strADENT+" dropped successfully\n")
    except:
        logging.info("Table ADWP_WORK1.ALL_ACCOUNTS_" + strADENT + " not created")
        logging.info("Table ADWP_WORK1.PRI_OWNER_ACCOUNTS_"+strADENT+" not created\n")

    cursor.close()
    dcEIW.close()

    logging.info("End of program")
    logging.shutdown()