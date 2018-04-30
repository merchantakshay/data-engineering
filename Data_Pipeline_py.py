
# coding: utf-8

# In[ ]:


#tables in sql should be created before running python script


# In[1]:


from bs4 import BeautifulSoup
import urllib2
import pandas as pd
import time
import datetime
from datetime import date
from datetime import timedelta
from collections import OrderedDict
import re
import json
import mysql.connector
import httplib
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# In[2]:


#counter for error log
counter = 0


# In[3]:


#checks database to see if coins table is empty or not
#if empty then auto generated id's are used to populate data
#otherwise data is extracted from the database
#flag is used to differentiate between first run and the next n runs
def check_database():
    
    conn = mysql.connector.connect(user = 'root', password = 'Akshay1894@', host = 'localhost', database = 'practice')
    mycursor = conn.cursor()
    mycursor.execute("USE practice")
    
    mycursor.execute("SELECT COUNT(*) FROM practice.coins")
    number = mycursor.fetchall()
    number = pd.DataFrame(number)
    number = number[0][0]
    
    if number > 0:
        flag = 1
    
    else:
        flag = 0
        
    return(flag)


# In[4]:


#extract aggregate data 
#currency -- coin names used in url
#df_to_join -- coin index to be joined with historical data to give an id for each coin in the historical table
def extractcurrency_new(r):
    
    data = json.load(urllib2.urlopen(r))
    
    #df contains current data for all currencies
    df = pd.DataFrame(data)
    df.insert(0, 'row_no', range(1, 1 + len(df)))
    
    df = df[[ 'row_no', 'id', 'name', 'symbol']]
    
    currency = df['id'].tolist()
    currency = [j.encode("utf-8") for j in currency]
    
    df_to_join = df[['id','row_no']]
    
    return(df, currency, df_to_join)


# In[5]:


#extract existing data from coins table -- to keep id's constant
def extract_database():
    
    conn = mysql.connector.connect(user = 'root', password = 'Akshay1894@', host = 'localhost', database = 'practice')
    mycursor = conn.cursor()
    mycursor.execute("USE practice")
    
    mycursor.execute("SELECT * FROM practice.coins")
    old = mycursor.fetchall()
    
    old = pd.DataFrame(old)
    old.columns = ['row_no', 'id', 'name', 'symbol', 'website', 'source_site']
    old = old[['row_no', 'id', 'name', 'symbol']]
    
    old_name = list(old['id'])
    old_name = map(lambda x: x.encode("utf-8"), old_name)
    
    old_len = len(old) + 1
    
    return(old, old_name, old_len)


# In[6]:


#extract aggregate data and append new currencies to the original data
#currency -- coin names used in url
#df_to_join -- coin index to be joined with historical data 
def extractcurrency_append(r):
    
    data = json.load(urllib2.urlopen(r))
    
    #new contains current data for all currencies
    new = pd.DataFrame(data)
    new = new[['id', 'name', 'symbol']]
    
    old, old_name, old_len = extract_database()
    
    new_name = new['id'].tolist()
    new_name = [j.encode("utf-8") for j in new_name]
    
    difference = list(set(new_name) - set(old_name))
    
    diff = difference
    
    difference = new.loc[new['id'].isin(difference)]
    difference.insert(0, 'row_no', range(old_len, old_len + len(difference)))
    
    df = old.append(difference)
    df['row_no'] = df['row_no'].astype(int)
    
    currency = df['id'].tolist()
    currency = [j.encode("utf-8") for j in currency]
    
    df_to_join = df[['id','row_no']]
    
    return(df, currency, df_to_join, diff)


# In[7]:


#extract currency name for url using json
r = 'https://api.coinmarketcap.com/v1/ticker/?limit=0'

flag = check_database()

if flag > 0:
    df, currency, df_to_join, diff = extractcurrency_append(r)
else:
    df, currency, df_to_join = extractcurrency_new(r)


# In[8]:


#fix column order for data
df= df[['row_no', 'id', 'name', 'symbol']]


# In[9]:


#correct dates for url
#checks pattern, start/end date after 2013-04-28 & start date before end date
#coinmarketcap does a good job maintaining a good date format so we do not have to account for leap years and such
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


# In[10]:


#dates
#flag > 0 means data already exists for previous dates & flag = 0 means program is running for the first time  
if flag > 0:
    yesterday = date.today() - timedelta(1)
    start_date = yesterday.strftime("%Y-%m-%d")
else:
    start_date = '2013-04-28'

end_date = time.strftime("%Y-%m-%d")

startdate, enddate = correctdate(start_date, end_date)


# In[11]:


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


# In[12]:


def create_log():
    
    f = open('log.csv','w')
    f.write(time.strftime("%Y-%m-%d"))


# In[13]:


wurl = [] #stores working currencies
nwurl = [] #stores not working currencies
err_code = [] #stores error codes for not working currencies

for c in currency:
     working, notworking, error_code= correcturl(c, startdate, enddate)
        
notworkingerror = pd.DataFrame(OrderedDict({'notworking': notworking,'error_code': error_code}))


# In[14]:


#count number of coins not parsed
if len(notworking) > 0:
    counter += 1
    create_log()
    with open('log.csv','a') as f:
        f.write('\nUnable to parse data for the following coins:\n')
        (notworkingerror).to_csv(f, header=True)
    f.close()

else:
    pass


# In[15]:


#read url
#httplib.IncompleteRead -- server side error which has not been accounted for. 
def readurl(wurl, startdate, enddate):
    
    url = 'https://coinmarketcap.com/currencies/' + wurl + '/historical-data/' + '?start=' + startdate + '&end=' + enddate
    try:
        webpage = urllib2.urlopen(url)
        page = webpage.read()
        webpage.close()
    except httplib.IncompleteRead:
        print('Error parsing ' +url)
        
    return(page)


# In[16]:


#headers for the historical dataset -- stored in complete
def historicalheaders(soup):
    
    headers = soup.tr.get_text()
    headers = headers.encode("utf-8")
    headers = headers.split("\n")
    headers = filter(None, headers)
    headers.insert(len(headers), 'id')
    
    return(headers)


# In[17]:


#data for the historical dataset -- stored in complete
def historicaldata(w, soup):
    
    cryptocurrency = soup.find_all("tr", class_ = "text-right")
    list_ = map(lambda x: x.get_text(), cryptocurrency) 
    list_ = map(lambda x: x.encode("utf-8"), list_)
    list_ = map(lambda x: x.split("\n"), list_)
    list_ = map(lambda x: filter(None, x), list_)
    map(lambda x: x.insert(any_large_number, w), list_)
    
    return(list_)


# In[18]:


#extract website and source site for all coins
def extractwebsite(w, soup):
    
    #init
    coin_website = []
    source_website = []
        
    list_ = soup.find_all("a", target = "_blank")
    for li in range(len(list_)):
        if list_[li].get_text() == u'Website':
            coin_website.append(list_[li].get('href'))
            
        if list_[li].get_text() == u'Source Code':
            source_website.append(list_[li].get('href'))
            
    if not coin_website:
        coin_website.append('-')
    
    if not source_website:
        source_website.append('-')
    
    return(coin_website, source_website) 


# In[19]:


#init
hist_data = pd.DataFrame()
websites = pd.DataFrame()

#any_large_number should be any large number so that the coin name is always inserted at the end of the list
#this is done to aid with the join on coin_id which is done in python
any_large_number = 6000


# In[20]:


#extract data for all working currencies
#data for each currency is appended into the dataFrame. 
def workingdata(working, startdate, enddate, hist_data, websites):
    
    for w in working:
        page = readurl(w, startdate, enddate)
        soup = BeautifulSoup(page, "lxml")
        
        coin_website, source_website = extractwebsite(w, soup)
        websites = websites.append(pd.DataFrame(OrderedDict(
            {'coin website': coin_website, 
            'source website':source_website, 
            'id':w})))
        
        list_ = historicaldata(w, soup)
        hist_data = hist_data.append(pd.DataFrame(list_))  
    
    headers = historicalheaders(soup)
    hist_data.columns = headers
        
    return(hist_data, websites)


# In[21]:


complete, website = workingdata(working, startdate, enddate, hist_data, websites)


# In[22]:


#insert row_no into historical data
#just a row_no & not used for anything else
complete.insert(0, 'index', range(1, 1 + len(complete)))


# In[23]:


#find currencies with no transactions during specified time period
#ideally the only ones in notransaction should be ones with no historical data
def getnotransactions(complete, working):
    
    transactions_present = complete['id'].unique()
    notransaction = list(set(working)-set(transactions_present))
    notransaction = pd.DataFrame({'notransaction': notransaction})
    
    return(notransaction)


# In[24]:


notransaction = getnotransactions(complete, working)


# In[25]:


#information on currencies with no tranactions
if len(notransaction) > 0:
    if counter==0:
        create_log()
    
    with open('log.csv','a') as f:
        f.write('\nNo historical transactions for coins:\n')
        (notransaction).to_csv(f, header=False)
    f.close()
    counter += 1

else:
    pass


# In[26]:


#pre-process to get final coins(SQL) table
#itertuples is a way to iterate over a dataframe
#itertuples is used to insert data one by one into the database, since moving whole dataframe at once will cause memory errors
def cleansnapshot_append(df, website, diff):
    
    if len(diff) > 0:
        df = pd.merge(df, website, how='left', on=['id'])
        df = df.loc[df['id'].isin(diff)]

        columns = df.columns.values
        #clean df 
        for c in columns:
            df[c] = map(str, df[c])
            df[c] = [j.encode("utf-8") for j in df[c]]

        #iterate over each row 
        list_snap = []
        for row in df.itertuples(index = False):
            list_snap.append(row)
            
    else:
        list_snap = []

    return(list_snap)


# In[27]:


#clean snapshot data to prep for move into MySQL 
def cleansnapshot_new(df, website):
    
    df = pd.merge(df, website, how='left', on=['id'])
    
    columns = df.columns.values
    #clean df 
    for c in columns:
        df[c] = map(str, df[c])
        df[c] = [j.encode("utf-8") for j in df[c]]
    
    #iterate over each row 
    list_snap = []
    for row in df.itertuples(index = False):
        list_snap.append(row)
        
    return(list_snap)


# In[28]:


if flag > 0:
    list_snap = cleansnapshot_append(df, website, diff)

else:
    list_snap = cleansnapshot_new(df, website)


# In[29]:


#insert ignore -- errors ignored
#moves data into coins table(SQL)
def snapshottosql(list_snap, counter):
    
    if not list_snap:
        
        return(counter)
    
    else:
        #create connection
        conn = mysql.connector.connect(user = 'root', password = 'Akshay1894@', host = 'localhost', database = 'practice')
        mycursor = conn.cursor()
        mycursor.execute("USE practice")

        add_data = ("INSERT IGNORE INTO coins VALUES (%s, %s, %s, %s, %s, %s)")

        try:
            #insert data 
            for l in list_snap:
                mycursor.execute(add_data, l)
            #commit
            conn.commit()

        except Exception as e:

            if counter==0:
                create_log()

            with open('log.csv','a') as f:
                f.write('\nData migration for coins table error:\n')
                f.write(str(e))
            f.close()
            counter += 1

            return(counter)      


# In[30]:


counter = snapshottosql(list_snap, counter)


# In[31]:


#clean historical data to prep for move into MySQL
def cleanhistorical(complete, df_to_join):
    
    #join rank from df
    complete = pd.merge(complete, df_to_join, how='left', on=['id'])
    #drop id
    complete = complete.drop(['id'], axis = 1)
    
    #convert to string
    cols = complete.columns.values
    for c in cols:
        complete[c] = map(str, complete[c])
        
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


# In[32]:


list_hist = cleanhistorical(complete, df_to_join)


# In[33]:


#inserts data into the year specific historical table
def insertintotable(year):
    
    return("INSERT IGNORE INTO historical_" +year+ " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")


# In[34]:


#insert ignore -- errors ignored
def historicaltosql(list_hist, counter):
    #create connection
    conn = mysql.connector.connect(user = 'root', password = 'Akshay1894@', host = 'localhost', database = 'practice')
    mycursor = conn.cursor()
    mycursor.execute("USE practice")
    
    try:
        #insert data 
        for l in list_hist:
            year = l.Date[:4]
            add_data = insertintotable(year)
            mycursor.execute(add_data, l)
        #commit
        conn.commit()
        
    except Exception as e:
        
        if counter==0:
            create_log()
        
        with open('log.csv','a') as f:
            f.write('\nData migration for Historical table error:\n')
            f.write(e)
        f.close()
        counter += 1

    return(counter)


# In[35]:


counter = historicaltosql(list_hist, counter)


# In[36]:


#sends email if errors occured
if counter>0:
    
    fromaddr = "johndoedatascientist"
    toaddr = "ddhar3@uic.edu"
    msg = MIMEMultipart()
    msg['From'] = fromaddr
    msg['To'] = toaddr
    msg['Subject'] = "Code Execution Error"

    body = "Error occured for latest run. Check log file for details."
    msg.attach(MIMEText(body, 'plain'))

    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.starttls()
    server.login(fromaddr, "johndoe123")
    text = msg.as_string()
    server.sendmail(fromaddr, toaddr, text)
    server.quit()
    
else:
    create_log()
    with open('log.csv','a') as f:
        f.write('Success')
    f.close()

