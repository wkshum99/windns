import os
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import csv
import time

def df_date(engine, d):
    sql = 'select * from log where Date like "'+d+'%%"'
    #print (sql)
    df_date = pd.read_sql_query(sql, engine)
    return df_date

def log_dates(engine):
    sql = 'select distinct(substring_index(Date, " ", 1)) as Date from log'
    log_dates = pd.read_sql_query(sql, engine)
    return log_dates

def log_frequent_domains_on_date(df, date, freq=50):
    result = df[(df['Direction']=='Rcv') & (df['RemoteIP']!='172.20.78.2') & (df['RemoteIP']!='172.20.78.109') & (df['RemoteIP']!='8.8.8.8')][['Domain', 'RemoteIP']].groupby(['Domain']).count().sort_values(by='RemoteIP', ascending=False)
    result.columns = ['Count']
    # make the domain column a regular one
    result.reset_index(inplace=True)    
    result.insert(0, 'Date', date)    
    return result.iloc[0:freq][:]

def log_frequent_domains_on_date_by_users(df, date, domain, freq=20):
    result = df[(df['Direction']=='Rcv') & (df['RemoteIP']!='172.20.78.2') & (df['RemoteIP']!='172.20.78.109') & (df['RemoteIP']!='8.8.8.8') & (df['Domain']==domain)][['Domain', 'RemoteIP', 'Server']].groupby(['Domain', 'RemoteIP']).count().sort_values(by='Server', ascending=False)
    result.columns = ['Count']
    # make the domain column a regular one
    result.reset_index(inplace=True)    
    result.insert(0, 'Date', date)    
    return result.iloc[0:freq][:]

def log_unique_urls(df, date):
    unique_urls = pd.DataFrame(df['Domain'].unique())
    unique_urls.columns = ['Domain']
    unique_urls.insert(0, 'Date', date)
    unique_urls = unique_urls[~unique_urls['Domain'].str.contains('in-addr.arpa')]
    return unique_urls

def output_to_sqldb(engine, df, table, if_exists='append', index=False, mode='auto'):
    if mode == 'auto':
        try:
            df.to_sql(table, engine, if_exists=if_exists, index=index)
        except sqlalchemy.exc.IntegrityError:
    #        print ("Data already exists.")
            print (sys.exc_info()[0])
            pass
    elif mode == 'manual':
        # split into 1000 chunks
        c = 0
        step = 500
        for i in range(0, len(df), step):
            sql = u'INSERT IGNORE INTO '+table+' VALUES '
            if c+step > len(df):
                temp = len(df)
            else:
                temp = c+step+1
            for j in range(c, temp):
                sql += u'("{0}", "{1}"),'.format(df.iloc[j][0], df.iloc[j][1])
            sql = sql.strip(',')
            conn = engine.connect()
            try:
                conn.execute(sql)
            except:
                print (sys.exc_info()[0])
                print("process sql: " + sql)
                pass
            c = j

def output_unique_domains_sqldb(engine, date, unique_urls_table, unique_domains_table, flavor='mysql'):
    sql = 'INSERT IGNORE INTO {0} SELECT distinct Date, substring_index(URL, ".", -2) from {1} where Date = "{2}"'.format(unique_domains_table, unique_urls_table, date)
    conn = engine.connect()
    conn.execute(sql)

def read_settings(file):
    settings = {}
    if os.path.isfile(file):
        with open(file, 'r') as f:
            reader = csv.reader(f, delimiter='=')
            for line in reader:
                settings[line[0]]=line[1]
    return settings

def del_dates(date, table, engine):
    # remove date line to avoid duplicate processes
    sql = 'DELETE FROM ' + table + ' where Date like "'+date+'%%"'
    conn = engine.connect()
    conn.execute(sql)

def log_stats(setting_file):
    print("process started at: " + time.strftime('%m/%d/%Y %H:%S', time.localtime()))
    settings = read_settings(setting_file)
    #sqlite_engine = create_engine('sqlite:///' + settings['sqlitedb'], encoding='UTF-8')
    # sqlite_engine=create_engine('sqlite:///' + settings[sqlitedb], encoding='UTF-8')
    mysql_engine = create_engine('mysql+pymysql://'+settings['sqluser']+':'+settings['sqlpasswd']+'@'+settings['sqlserver']+'/'+settings['sqldb']+'?charset=utf8')

    dates = log_dates(mysql_engine)

    # for production
    output_to_sqldb(mysql_engine, dates, settings['dates_table'])

    for d in range(0, len(dates)):
        temp_domain = []
        temp_ip = []
        # find unique dates
        df = df_date(mysql_engine, dates.iloc[d][0])
        # find unique domains
        unique_urls = log_unique_urls(df, dates.iloc[d][0])
        output_to_sqldb(mysql_engine, unique_urls, settings['unique_urls_table'], mode='manual')
        output_unique_domains_sqldb(mysql_engine, dates.iloc[d][0], settings['unique_urls_table'], settings['unique_domains_table'])
        #output_to_sqldb(mysql_engine, unique_domains, settings['unique_domains_table'], mode='manual')
        # find the frequent domain queries
        temp_domain = log_frequent_domains_on_date(df, dates.iloc[d][0])
        output_to_sqldb(mysql_engine, temp_domain, settings['domains_table'])
        for e in range(0, len(temp_domain)):
            temp_ip = log_frequent_domains_on_date_by_users(df, dates.iloc[d][0], temp_domain.iloc[e][1])
            output_to_sqldb(mysql_engine, temp_ip, settings['ip_table'])
        # remove date line to avoid duplicate processes
        del_dates(dates.iloc[d][0], 'log', mysql_engine)
    print("process end at: " + time.strftime('%m/%d/%Y %H:%S', time.localtime()))

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("please specify the settings file")

    setting_file = sys.argv[1]

    log_stats(setting_file)

