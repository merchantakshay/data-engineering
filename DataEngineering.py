
# coding: utf-8

# In[ ]:


from bs4 import BeautifulSoup
import urllib2
import pandas as pd
import time
import datetime
from collections import OrderedDict
import re
import json
import mysql.connector
import httplib


# In[ ]:


#extract aggregate data 
#currency -- coin names used in url
#df_to_join -- coin index to be joined with historical data 
def extractcurrency(r):
    data = json.load(urllib2.urlopen(r))
    
    #df contains current data for all currencies
    df = pd.DataFrame(data)
    df.insert(0, 'row_no', range(1, 1 + len(df)))
    currency = df['id'].tolist()
    currency = [j.encode("utf-8") for j in currency]
    
    df_to_join = df[['id','row_no']]
    
    return(df, currency, df_to_join)


# In[ ]:


#extract currency name for url using json
r = 'https://api.coinmarketcap.com/v1/ticker/?limit=0'

df, currency, df_to_join = extractcurrency(r)


# In[ ]:


#correct dates for url
#checks pattern, start/end date after 2013-04-28 & start date before end date
def correctdate(start_date, end_date):
    pattern = re.compile('[2][0][1][0-9]-[0-1][0-9]-[0-3][0-9]')   
    if not re.match(pattern, start_date):
        raise ValueError('Invalid format for start_date: ' + start_date + '. Should be: yyyy-mm-dd')
    if not re.match(pattern, end_date):
        raise ValueError('Invalid format for end_date: ' + end_date + '. Should be: yyyy-mm-dd.')
    
    s_year, s_month, s_day = map(int, start_date.split('-'))
    e_year, e_month, e_day = map(int, end_date.split('-'))
    s_month, s_day, e_month, e_day = ["%02d" % x for x in [s_month, s_day, e_month, e_day]]
    s_date = ''.join([str(j) for j in [s_year, s_month, s_day]])
    e_date   = ''.join([str(j) for j in [e_year, e_month, e_day]])
    
    if not (int(s_date) >= 20130428):
        raise ValueError('Coinmarketcap does not contain data before 2013-04-28')
    if not (int(e_date) >= 20130428):
        raise ValueError('Coinmarketcap does not contain data before 2013-04-28')
    if not (int(s_date) < int(e_date)):
        raise ValueError('End date cannot be before Start date')
    
    return(s_date, e_date)


# In[ ]:


#enter dates
start_date = '2013-04-28'
end_date = time.strftime("%Y-%m-%d")

startdate, enddate = correctdate(start_date, end_date)


# In[ ]:


#check if urls work
#catches errors so code does not stop executing -- most will have a 404 error
def correcturl(crypto, startdate, enddate):
    url = 'https://coinmarketcap.com/currencies/' + crypto + '/historical-data/' + '?start=' + startdate + '&end=' + enddate
    try:
        webpage = urllib2.urlopen(url)
        wurl.append(crypto)
    except urllib2.HTTPError as e:
        nwurl.append(crypto)
        err_code.append(e.code)
            
    return(wurl, nwurl, err_code)


# In[ ]:


wurl = [] #stores working currencies
nwurl = [] #stores not working currencies
err_code = [] #stores error codes for not working currencies

for c in currency:
     working, notworking, error_code= correcturl(c, startdate, enddate)
        
notworkingerror = pd.DataFrame(OrderedDict({'notworking': notworking,'error_code': error_code}))


# In[ ]:


#count number of coins not parsed
if len(notworking) > 0:
    print 'Unable to parse data for',len(notworking),'coin/s. Data in notworkingerror'
else:
    pass


# In[ ]:


#read url
#httplib.IncompleteRead -- server side error
def readurl(wurl, startdate, enddate):
    url = 'https://coinmarketcap.com/currencies/' + wurl + '/historical-data/' + '?start=' + startdate + '&end=' + enddate
    try:
        webpage = urllib2.urlopen(url)
        page = webpage.read()
        webpage.close()
    except httplib.IncompleteRead:
        print('Error parsing ' +url)
        
    return(page)


# In[ ]:


#headers for complete dataset -- historical
def historicalheaders(soup):
    headers = soup.tr.get_text()
    headers = headers.encode("utf-8")
    headers = headers.split("\n")
    headers = filter(None, headers)
    headers.insert(len(headers), 'id')
    
    return(headers)


# In[ ]:


#data for complete dataset -- historical
def historicaldata(w, soup):
    cryptocurrency = soup.find_all("tr", class_ = "text-right")
    list_ = map(lambda x: x.get_text(), cryptocurrency) 
    list_ = map(lambda x: x.encode("utf-8"), list_)
    list_ = map(lambda x: x.split("\n"), list_)
    list_ = map(lambda x: filter(None, x), list_)
    map(lambda x: x.insert(any_large_number, w), list_)
    
    return(list_)


# In[ ]:


#init
hist_data = pd.DataFrame()
#any_large_number should be any large number so that the coin name is always inserted at the end of the list
any_large_number = 6000


# In[ ]:


#extract data for all working currencies
#data for each currency is appended into the dataFrame. 
def workingdata(working, startdate, enddate, hist_data):
    for w in working:
        page = readurl(w, startdate, enddate)
        soup = BeautifulSoup(page, "lxml")
        
        list_ = historicaldata(w, soup)
        hist_data = hist_data.append(pd.DataFrame(list_))  
    
    headers = historicalheaders(soup)
    hist_data.columns = headers
    
    return(hist_data)


# In[ ]:


complete = workingdata(working, startdate, enddate, hist_data)


# In[ ]:


#find currencies with no transactions during specified time period
#ideally the only ones in notransaction should be ones with no historical data
def getnotransactions(complete, working):
    
    transactions_present = complete['id'].unique()
    notransaction = list(set(working)-set(transactions_present))
    notransaction = pd.DataFrame({'notransaction': notransaction})
    
    return(notransaction)


# In[ ]:


notransaction = getnotransactions(complete, working)


# In[ ]:


#information on currencies with no tranactions
if len(notransaction) > 0:
    print 'The number of coins with no transactions from ' +start_date+ ' to ' +end_date+ ' is/are:', len(notransaction) 
    print 'Data in notransaction'
else:
    pass


# In[ ]:


#clean snapshot data to prep for move into MySQL 
def cleansnapshot(df):
    columns = df.columns.values
    #clean df 
    for c in columns:
        df[c] = map(str, df[c])
        df[c] = [j.encode("utf-8") for j in df[c]]
        #df[c] = df[c].replace("None", 0.0)
    
    #iterate over each row 
    list_snap = []
    for row in df.itertuples(index = False):
        list_snap.append(row)
        
    return(list_snap)


# In[ ]:


list_snap = cleansnapshot(df)


# In[ ]:


#insert ignore -- errors ignored
def snapshottosql(list_snap):
    #create connection
    conn = mysql.connector.connect(user = 'root', password = 'Akshay1894@', host = 'localhost', database = 'practice')
    mycursor = conn.cursor()
    mycursor.execute("USE practice")
    
    add_data = ("INSERT IGNORE INTO coins VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    
    try:
        #insert data 
        for l in list_snap:
            mycursor.execute(add_data, l)
        #commit
        conn.commit()
        
        print('Snapshot Data moved to database')
        
    except Exception as e:
        
        print(e)
        


# In[ ]:


snapshottosql(list_snap)


# In[ ]:


#clean historical data to prep for move into MySQL
def cleanhistorical(complete, df_to_join):
    
    #join rank from df
    complete = pd.merge(complete, df_to_join, how='left', on=['id'])
    #convert to string
    cols = complete.columns.values
    for c in cols:
        complete[c] = map(str, complete[c])
        #complete[c] = complete[c].replace("-", 0.0)
        
    #to readable date format
    complete['Date'] = pd.to_datetime(complete['Date'], format='%b %d, %Y')
    complete['Date'] = map(str, complete['Date'])
    complete['Date'] = complete['Date'].str.replace(" .*", "")
    
    #remove comma
    complete = complete.replace({',': ''}, regex=True)  

    list_hist = []
    for row in complete.itertuples(index = False):
        list_hist.append(row)
        
    return(list_hist)


# In[ ]:


list_hist = cleanhistorical(complete, df_to_join)


# In[ ]:


#insert ignore -- errors ignored
def historicaltosql(list_hist):
    #create connection
    conn = mysql.connector.connect(user = 'root', password = 'Akshay1894@', host = 'localhost', database = 'practice')
    mycursor = conn.cursor()
    mycursor.execute("USE practice")
    
    add_data = ("INSERT IGNORE INTO historical VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
    
    try:
        #insert data 
        for l in list_hist:
            mycursor.execute(add_data, l)
        #commit
        conn.commit()
        
        print('Historical data moved to database')
    
    except Exception as e:
        
        print(e)
    


# In[ ]:


historicaltosql(list_hist)

