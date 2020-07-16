#!/usr/bin/env python
# -*- coding: UTF-8 -*-

"""
Name                    : check_mocheckso
Description             : imports a csv file from ftp server, makes a minimal check and sends errors via email
Date                    : 1/May/2014 
copyright               : (C) 2014 by Tobias Reber
email                   : tobas.reber (at) bd.so.ch
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

 sys.exit(3) = UNKNOWN
 sys.exit(2) = ERROR
 sys.exit(1) = WARNING
 sys.exit(0) = OK

"""

try:
    import os
    import sys
    import urllib2
    import csv
    import logging
    import logging.handlers
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from optparse import OptionParser
    import psycopg2
    from ftplib import FTP

except ImportError, strerror:
    print strerror
    sys.exit(2)


# VARS
DB_CONNECTION = 'host=geodb.verw.rootso.org dbname=sogis user=datasync password=topsecret'
LOG_FILE = os.path.dirname(os.path.realpath(__file__)) + '/log_check_mocheckso.log'
CSV_FILE = os.path.dirname(os.path.realpath(__file__)) + '/errors.csv'
PROXY = {'http': 'http://proxyuser:access@193.135.67.150:8080',
'ftp': 'http://proxyuser:access@193.135.67.150:8080'}
TMP_IMPORT_TABLE = 'av_mocheckso.mocheckso_script_import' 
IMPORT_TABLE = 'mocheckso'
EMAIL_SENDER = 'import_mocheckso@srsofaioi4531.ktso.ch'
EMAIL_RECEIVER = 'andrea.luescher@bd.so.ch;daniel.rudin@bd.so.ch'

#VARS for FTPlib
URLFTP = 'ftp.infogrips.ch'
USRFTP = 'vaso'
PWDFTP = 'vaso123'
PATHFTP = 'DM01AVSO24LV95/ERROR/MOCHECKSO_ERRORS.CSV'
CMDFTP = 'RETR '+PATHFTP
URL_FTP = 'ftp://'+USRFTP+':'+PWDFTP+'@'+URLFTP+'/'+PATHFTP

# logging
logger = logging.getLogger('LogMOCHECKSO')  
logger.setLevel(logging.DEBUG)
log_handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1048576, backupCount=4) # logfile a 1MByte 
log_handler.setFormatter(logging.Formatter('%(levelname)s: %(asctime)s %(message)s'))
logger.addHandler(log_handler)


def main():
    """the main function"""
    
    # parsing command line arguments
    epilog = """
    Imports a file via ftp, checks it and writes the content into a database. If the
    check detects an error, an e-mail is being sent (ask tobias.reber@bd.so.ch). Configuration in file.\n
Examples:\n
   python check_mocheckso.py"
    \n"""

    OptionParser.format_epilog = lambda self, formatter: self.epilog
    parser = OptionParser(epilog=epilog)
    # not in use yet
    #parser.add_option("-x", dest="x", default=None, help="x-coordinate in EGSP21781")
    #parser.add_option("-y", dest="y", default=None, help="y-coordinate in EPSG21781")

    (options, args) = parser.parse_args()

    #if options.x != None:
    #    x = options.x
    #if options.y != None:
    #    y = options.y


    logger.debug('')
    logger.debug('---------------')
    logger.debug('Skript started:')
    logger.debug('---------------')
    logger.debug('')
    
    # db
    try:
        conn = psycopg2.connect(DB_CONNECTION)
        conn.set_isolation_level(0)
        cur = conn.cursor()

    except Exception, e:
        exit_error('Could not connect to database: ' + str(e))

    # function calls
    logger.debug('write csv-file ' + CSV_FILE)
    #get_csv(CSV_FILE, URL_FTP, PROXY)
    get_csv_ftplib(CSV_FILE,URLFTP,USRFTP,PWDFTP,CMDFTP)
    
    logger.debug('import csv to table') 
    load_csv(DB_CONNECTION, TMP_IMPORT_TABLE, CSV_FILE)

    logger.debug('update table')
    update_table(cur, TMP_IMPORT_TABLE, IMPORT_TABLE)

    logger.debug('validate csv-file')
    check_csv(cur, CSV_FILE, EMAIL_SENDER, EMAIL_RECEIVER)

    # entire script was successful
    strSuccess = "check_mocheckso konnte erfolgreich durchgeführt werden"
    print strSuccess
    logger.debug(strSuccess)
    sys.exit(0) #SUCCESS


def get_csv(csv_file, url_ftp, iproxy):
    """makes a ftp connection and writes the downloaded file to the given path"""
    try:
        #proxy = urllib2.ProxyHandler(iproxy)
        #auth = urllib2.HTTPBasicAuthHandler()
        #opener = urllib2.build_opener(proxy, auth, urllib2.HTTPHandler)
        #urllib2.install_opener(opener)

        response = urllib2.urlopen(url_ftp)
        ifile = open(csv_file,'w')
        ifile.write(response.read())
        ifile.close()
    except Exception, e:
        exit_error('The ftp-download did not work: ' + str(e))

def get_csv_ftplib(csv_file,url_ftp,usr_ftp,pwd_ftp,cmd_ftp):
    try:
        ftp = FTP(url_ftp,usr_ftp,pwd_ftp)
        gFile = open(csv_file,'w')
        ftp.retrbinary(cmd_ftp, gFile.write)
        gFile.close()
        ftp.quit()
    except Exception, e:
        exit_error('The ftp-download did not work: ' + str(e))


def load_csv(db, import_table, csv_file):
    """imports a csv into the database"""
    try:
        iconv = 'iconv -f iso-8859-1 -t utf-8 ' + csv_file + ' > ' + csv_file + 'utf8'  
        move = 'mv ' + csv_file + 'utf8 ' + csv_file
        ogr2ogr = '/usr/local/gdal/bin/ogr2ogr -f "PostgreSQL" PG:"' + db +'" '+ csv_file + ' -overwrite -nln ' + import_table
        exit_value = os.system(iconv + ' && '+ move + ' && ' + ogr2ogr)
    except Exception, e:
        exit_error('could not import csv to database: ' + str(e))
    if exit_value != 0:
        exit_error('could not import csv to database. exit ogr2ogr != 0')

def update_table(cur, tmp_table_name, table_name='mocheckso_script_import_test'):
    """updates a database table"""

    sql = u"""
    begin;
    DELETE FROM av_mocheckso.%s;
    INSERT INTO av_mocheckso.%s
    (
    id,
    "RICSService",
    "RICSDate",
    "DatasetName",
    "DatasetID",
    "ILModel",
    "ILTopic",
    "ILTable",
    "ErrorID",
    "ErrorCategory",
    "ErrorCount",
    "BFSNr",
    "Kt",
    "ErrorDescription",
    "RICSProfile",
    "ErrorX",
    "ErrorY",
    "Geometrie")
    SELECT
    ogc_fid,
    ricsservice,
    ricsdate,
    datasetname,
    datasetid,
    ilmodel,
    iltopic,
    iltable,
    errorid,
    errorcategory,
    errorcount,
    bfsnr,
    kt,
    errordescription,
    ricsprofile,
    errorx,
    errory,
    CASE WHEN errorx::text = 'NULL' AND errory::text = 'NULL' THEN
        ST_PointFromText('POINT('||'0'||' '||'0'||')', 2056)
    WHEN errorx::text = 'NULL' AND NOT errory::text = 'NULL' THEN
        ST_PointFromText('POINT('||'0'||' '||errory::text||')',2056)
    WHEN NOT errorx::text = 'NULL' AND errory::text = 'NULL' THEN
        ST_PointFromText('POINT('||errorx::text||' '||'0'||')',2056)
    ELSE
        ST_PointFromText('POINT('||errorx::text||' '||errory::text||')',2056)
    END AS wkb_geometry
    FROM %s
    WHERE ILModel LIKE 'DM01AVSO24LV95';
    DROP TABLE %s;
    commit;
    """ % (table_name, table_name, tmp_table_name, tmp_table_name)
    try:
        cur.execute(sql)
    except Exception, e:
        exit_error('table ' + table_name + ' in database could not be updated: ' + str(e))


def check_csv(cur, csv_file, email_sender, email_receiver):
    """checks the csv on given criterias
       - are the first 4 characters of DatasetName acurate BFS-numbers of SO
       - are the 2 following characters zeros
    """
    try:
        ifile = open(csv_file, 'r') # open csv
        dictContent = get_csvDict(ifile) # make csv-dictionary-object from csv
        arrErrorString = []
        for line in dictContent: # for each line in the csv-dictionary object
            # check bfs-number
            strDatasetNameBfs = str(line['DatasetName'])[0:4] # only first 4 numbers are bfs
            if is_int(strDatasetNameBfs):
                sql = """
                SELECT COUNT(*) 
                FROM geo_gemeinden_v 
                WHERE gem_bfs = %s
                """ % strDatasetNameBfs
                cur.execute(sql)
                rows = cur.fetchall()

                if rows[0][0] < 1: # if count < 1 warning 
                    arrErrorString.append('<b>File is not labled with a correct BFS-number: </b>' + line['DatasetID'])
                    logger.debug('BFS-number not accurate: ' + line['DatasetID'])
            else:
                arrErrorString.append('<b>File is not labeled with a correct BFS-number: </b>' + line['DatasetID'])
                logger.debug('BFS-number not accurate: ' + line['DatasetID'])


            # check zeros 
            strDatasetNameZeroZero = str(line['DatasetName'])[4:6]
            if strDatasetNameZeroZero != '00':
                arrErrorString.append('<b>BFS-number not followed by two zeros: </b>' + line['DatasetID'])
                logger.debug('BFS-number not followed by two zeros: ' + line['DatasetID'])


        # if error, send email
        if len(arrErrorString) > 0:
            logger.debug('trying to send mail')
            str_message = '<b>Der MOCHECKSO-Import hat Folgendes bemerkt:</b><br/>'
            str_message += '<ol>'
            for row in arrErrorString:
                str_message += '<li>' + row + '</li>' 
            str_message += '</ol>'
            str_message += '<br/><br/>Freundliche Gr&uuml;sse und einen wundersch&ouml;nen Tag'
            str_message += '<br/><i>Ihr MOCHECKSO-Import</i>'
            send_email(str_message, email_sender, email_receiver)
            logger.debug('mail with error messages should have been sent...')

    except Exception, e:
        exit_error('Error checking csv: ' + str(e))


def get_csvDict(ifile):
    """takes a csv-file and returns a csv-dict-object"""
    ifile.seek(0) #reset file to first line
    return csv.DictReader(ifile, delimiter='\t')


def send_email(msg, sender, receiver):
    """sends an email"""

    message = MIMEMultipart('alternative')
    message['Subject'] = 'Feedback MOCHECKSO Import'
    message['From'] = sender
    message['To'] = receiver

    text = "Fehler:\nDer MOCHECKSO hat Fehler festgestellt.\nMehr, wenn der Email-Client html interpretieren wuerde."
    html = """\
    <html>
        <head>
            <style type="text/css">
                <!--
                body { font-family: sans-serif; }
                -->
            </style>
        </head>
        <body>
            %s
        <body> 
    </html>
    """ % msg

    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    message.attach(part1)
    message.attach(part2)
    
    try:
        smtpObj = smtplib.SMTP('localhost')
        smtpObj.sendmail(sender, receiver.split(';'), message.as_string())
        smtpObj.quit()
        print "SUCCES email"
    except Exception, e:
        exit_error('Email has not been sent: ' + str(e))
    
def is_int(s):
    """check if the intput is an int"""
    try:
        x = int(s)
        if str(x) == str(s):
            return True
        else:
            return False
    except:
        return False 

def exit_error(err_msg):
        """logs error message and quits"""
        logger.error(err_msg)
        print err_msg
        sys.exit(2) #ERROR
        

if __name__ == "__main__":
    main()
