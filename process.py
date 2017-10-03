import os
import re
import time
import codecs
import sys
import pandas as pd
from zipfile import ZipFile
from sqlalchemy import create_engine
import csv
import logging
import shutil
logging.basicConfig(filename='process_log.txt', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
#logging.disable(logging.CRITICAL)

def read_settings(file):
    settings = {}
    if os.path.isfile(file):
        with open(file, 'r') as f:
            reader = csv.reader(f, delimiter='=')
            for line in reader:
                settings[line[0]]=line[1]
    return settings

def process_dns_log(setting_file):
    settings = read_settings(setting_file)

    #disk_engine = create_engine('sqlite:///'+sqldb, encoding = 'UTF-8')
    mysql_engine = create_engine('mysql+pymysql://' + settings['sqluser'] + ':' + settings['sqlpasswd'] + '@' + settings['sqlserver'] + '/' +
        settings['sqldb'] + '?charset=utf8')
    #store = pd.HDFStore('windns-log.h5')

    names = ['Date', 'Threadid', 'Context', 'PacketID', 'Protocol', 'Direction', 'RemoteIP', 'Xid', 'QueryResponse',
            'Opscode', 'Flag', 'A', 'T', 'D', 'R', 'ResponseCode', 'Questiontype', 'Domain']

    dtype = {'Threadid':str, 'Context':str, 'Intpacketid':str, 'Protocol':str, 
             'Direction':str, 'Remoteip':str, 'Xid':str, 'QueryResponse':str,
            'Opcode':str, 'Flag':str, 'Author':str, 'Trun':str, 'Recurd':str, 
             'Recura':str, 'Response':str, 'Questiontype':str, 'Domain':str}
    # if time is single digit, use colspec
    # if time is two digits, use colspec1
    colspec = [5, 8, 17, 4, 4, 16, 5, 2, 2, 6, 1, 1, 1, 2, 10, 7, 100]
    #colspec = [(0, 20),(21, 25),(26, 32),(34, 50),(51, 54),(55, 58),(59, 75),(76, 81),(82, 83),
    #    (83, 84),(85, 89),(90, 91),(91, 92),(92, 93),(94, 95),(96, 103),(105, 111),(113, 200)]

    start_time = time.time()

    for file in os.listdir(settings['log_path']):
        if file[-4::] != '.zip':
            logging.debug (file + ' is not a zip file, ignored.')
            continue

        server = file.split('\\')[-1].split('dns')[0]
        thisFile = os.path.join(settings['log_path'], file)

        with ZipFile(thisFile, 'r') as zip:
           for filename in zip.namelist():
                if not os.path.isdir(filename):
                    with zip.open(filename, 'r') as f:
                        ret = []
                        #for line in f:
                        for line in codecs.iterdecode(f, 'utf8', errors='replace'):
                            if '[' in line and ']' in line:
                                #line = line.strip()
                                # remove the (d) in the domain field
                                line = re.sub(r'\(\d*\)', '.', line)
                                temp = []
                                start_pos = re.search('[A|P]M', line).end()+1
                                # first element
                                thisLine = line[0:start_pos].strip().strip('.')
                                temp.append(thisLine)
                                # second element and onwards
                                for i in colspec:
                                    thisLine = line[start_pos:start_pos+i].strip().strip('.')
                                    start_pos += i
                                    temp.append(thisLine)

                                ret.append(temp)
        #df = pd.read_fwf(zip.open(filename, 'r'), colspecs=colspec, skiprows=29, skip_blank_lines=True)
        logging.debug (filename + " read in: " + str(time.time() - start_time) + " seconds")

        start_time = time.time()
        #df = pd.read_fwf('./hkdc01dns2.zip', colspecs=colspec, names=names, skiprows=29, skip_blank_lines=True, compression='zip')
        df = pd.DataFrame(ret, columns=names)
        logging.debug ("changed to dataframe in: " + str(time.time() - start_time) + " seconds")

        start_time = time.time()
        df.dropna(axis=0, how='all', inplace=True) # somehow the skip_blank_lines is not working, so manually remove blank rows
        df = df[df['Context'] != 'EVENT'] # filter out the EVENT row
        # add a server name to the dataframe for differentiation
        df['Server'] = server
        #df['Domain'] = df['Domain'].str.replace('\x03', '?') # replace the unreadable char in the Domain column with '?'
        #df['Domain'] = df['Domain'].str.replace('\(\d*\)', '.')
        #df[names[1:]] = df[names[1:]].astype(str) # change all columns to string type
        logging.debug ("data cleanup in: " + str(time.time() - start_time) + " seconds")

        start_time = time.time()
        df.to_sql('log', mysql_engine, index=False, if_exists='append', chunksize=500)

        #df[473000:473150].to_sql('log', disk_engine, index=False, if_exists='append', chunksize=500)
        #df.to_hdf(store, key='log', format='table', mode='a', append=True, complevel=3, complib='zlib')
        #df.to_csv('windns-log.csv')
        logging.debug ("output data to sql in: " + str(time.time() - start_time) + " seconds")

        now = time.strftime('%Y%m%d%H%S', time.localtime())
        backFile = os.path.join(settings['backup_path'], server+'dns_'+now+".zip")

        if os.path.isfile(backFile) != True:
            # move the processed file to backup location
            shutil.move(thisFile, backFile)
        else:
            # delete the processed file
            logging.debug  ('backup already existed, just delete the processed file')
            os.unlink(thisFile)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("please specify the settings file")

    setting_file = sys.argv[1]
    
    process_dns_log(setting_file)

