############################################################
#             AUTHOR: Gerrott Faulkingham                  #
#            DATE CREATED: June 15th, 2016                 #
############################################################

#                        Objective                         #

#Create a link to the Quandl API to import data into a local MySQL database containing
#different historical stock data


#                      Prerequisites                       #

#1.   Libraries "urllib2", "csv", "MySQLdb", "os" all installed 
#2.   MySQL installed and configured to match parameters in the code
#3.   API signed up for on Quandl.com and "API_key" variable updated with data (it is free to sign up!)
#4.   "csvfile" variable updated to a specific path to save csv datafiles to
#5.   "stock_list" and "stock_dict" updated to pull required stock data
#6.   "mydb" variable updated to match user database login credentials
#7.   "sql3" code updated to match path of "csvfile" save location


#                        Notes                             #

#1.   The Quandl API allows a user to request data up to 50 times per day for anonymous users (users without an "API_key")
#        to take advantage of this: run this program without signing up for a Quandl API and set the "API_key" variable 
#        to ''. Make sure the "stock_list" is less than 50. 

from urllib2 import Request, urlopen, URLError
import csv
import MySQLdb
import os

#List of stocks to be pulled from the Quandl API. This is not all stocks available on the stock exchange! 
#If additional stocks need to be pulled, please check the list @ https://www.quandl.com/data/WIKI

stock_list = ['AAPL', 'AMZN', 'AON', 'AXP', 'BAC', 'BSX', 'CAT', 'CBS', 'CL', 'CMG', 'COST', 'DIS', 'DPS', 'EA', 'F', 'FDX', 'GOOG', \
'GRMN', 'HAS', 'HD', 'HOG', 'HPQ', 'HSY', 'INTC', 'JNJ', 'KMB', 'LMT', 'LOW', 'M', 'MA', 'MAT', 'MCD', 'MDLZ', 'MSFT', 'NFLX', \
'PEP', 'PG', 'SBUX', 'SPLS', 'TAP', 'TGT', 'TJX', 'WFM', 'WMT', 'YHOO', 'YUM', 'UPS'] 

#Dictionary to look up the Ticker with the actual Description. Needs to be updated if a new stock is added to the "stock list"

stock_dict = {'AAPL':'Apple', 'AMZN':'Amazon', 'AON':'Aon', 'AXP':'American Express', 'BAC':'Bank of America', \
'BSX':'Boston Scientific Corp', 'CAT':'Caterpillar', 'CBS':'CBS Corp', 'CL':'Colgate-Palmolive', \
'CMG':'Chipotle Mexican Grill', 'COST':'Costco', 'DIS':'Disney', 'DPS':'Dr Pepper Snapple Group', 'EA':'Electronic Arts', \
'F':'Ford Motor Co', 'FDX':'Fedex', 'GOOG':'Alphabet Inc', 'GRMN':'Garmin', 'HAS':'Hasbro', 'HD':'Home Depot Inc', 'HOG':'Harley-Davidson Inc', \
'HPQ':'Hewlett-Packard Co', 'HSY':'Hershey Company', 'INTC':'Intel Corp', 'JNJ':'Johnson & Johnson', 'KMB':'Kimberly-Clark Corp', \
'LMT':'Lockheed Martin Corp', 'LOW':'Lowes Cos', 'M':'Macy\'s Inc', 'MA':'Mastercard', 'MAT':'Mattel Inc', 'MCD':'McDonald\'s Corp', \
'MDLZ':'Mondelez International Inc', 'MSFT':'Microsoft Corperation', 'NFLX':'Netflix Inc', 'PEP':'Pepsico Inc', 'PG':'Procter & Gamble Co', \
 'SBUX':'Starbux Corp', 'SPLS':'Staples Inc', 'TAP':'Molson Coors Brewing Co', 'TGT':'Target Corp', 'TJX':'TJX Companies Inc', \
 'WFM':'Whole Foods Market Inc', 'WMT':'Wal-mart Stores', 'YHOO':'Yahoo! Inc', 'YUM':'Yum! Brands Inc', 'UPS':'United Parcel Service Inc'}


#declare variables for API request
API_string = 'https://www.quandl.com/api/v3/datasets/'
API_key = '?api_key=QH_iGpEfd7DjPJRiFmwY'
Database = 'WIKI/'
File_format = '.csv'

#Initialize the counter to display progress to the user
Ticker_counter = 1

#iterate through all stocks in the ticker list
for Dataset in stock_list:
    request = Request(API_string + Database + Dataset + File_format + API_key)

#attempt to read API request into variable
    try:
	    response = urlopen(request)
	    stock = response.read()
    except URLError, e:
	    print 'Some unknown error', e
	
#connect to DB
    mydb = MySQLdb.connect(host='localhost',
	    user='gman',
	    passwd='UPDATE',
	    db='stock_data')
    cursor = mydb.cursor()


#delcare save path of CSV
    csvfile = "C:\Users\Gerrott\Data_import\\" + Dataset + "_stock_data"

#write all data into an "in" File
    with open(csvfile + '_in.csv', 'wb') as output:
        writer = csv.writer(output, delimiter = ',', lineterminator='\n')
        writer.writerow([stock])

#Remove the starting Quotation mark (") from the created file		
	f = open(csvfile + '_in.csv', 'rb')
    lines = f.readlines()
    f.close()
    f = open(csvfile + '_in.csv', 'wb')
    c = 0
    for line in lines:
        if c == 0:
		    f.write(line[1:])
        else: f.write(line)
        c += 1
    f.close()

#Remove some whitespace and closing Quotation mark (") from the file    
    with open(csvfile + '_in.csv', 'rb+') as f:
        f.seek(0,2)                 # end of file
        size=f.tell()               # the size...
        f.truncate(size-3)          # truncate at that size - how ever many characters

#Create an output file with the Ticker, Description, and other data from the API		
    with open(csvfile + '_in.csv', 'rb') as csvinput:
        with open(csvfile + '.csv', 'wb') as csvoutput:
            writer = csv.writer(csvoutput)
            for row in csv.reader(csvinput):
                writer.writerow([Dataset] + [stock_dict[Dataset]] + row)
				
    os.remove(csvfile + '_in.csv')

#declare SQL statements

#sql1 and sql2 are to be used only once, if an existing table needs to be dropped and recreated

#Drop table if exists
    sql1 = '''
DROP TABLE IF EXISTS %s_stock;
''' % (Dataset)

#Create table again
    sql2= '''
CREATE TABLE stock_data (
,Ticker text 
,Description text
,Date DATE
,Open float
,High float
,Low float
,Close float
,Volume float
,Ex_Dividend float
,Split_Ratio float
,Adj_Open float
,Adj_High float
,Adj_Low float
,Adj_Close float
,Adj_Volume float
,Change_val float AS (Open - Close)
);
'''

#Load the data from the specific ticker file into MySQL
    sql3 = '''
LOAD DATA LOCAL INFILE 'C:\\\Users\\\Gerrott\\\Data_import\\\\%s_stock_data.csv' 
REPLACE
INTO TABLE stock_data
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\\r\\n'
IGNORE 1 LINES;
''' % (Dataset)

#execute mysql
    #cursor.execute(sql1)
    #cursor.execute(sql2)
    cursor.execute(sql3)
	  
		  
#close the connection to the database and print progress to the user
    mydb.commit()
    cursor.close()
    print "Done with %s data. (%s of %s)" %(Dataset, Ticker_counter, len(stock_list))
    Ticker_counter += 1
    if Ticker_counter == len(stock_list) + 1:
        print "\n \n Database update complete!"