# @Author: Dex
# @Email: ykydxt@gmail.com

import boto3
import botocore.vendored.requests
import urllib
import fnmatch
import time
import json
import os
import csv
import sys
from bs4 import BeautifulSoup
import pymysql
import rds_config
import pysftp
import logging
import sftp_config
from io import BytesIO
from datetime import datetime, timedelta
import pytz


'''
Environment Variable：
    queue_name (string): the name of SQS queue
    source_bucket (string): the bucket of the source csv file
    source_key (string): the key of the source csv file
'''

sqs = boto3.client('sqs')
s3 = boto3.client('s3')
sns = boto3.client('sns')
secrets_manager = boto3.client('secretsmanager')

dbucket = os.environ["destinationbucket"]
snstopic = os.environ['snstopic']
accountID = os.environ['accountID']
secret_manager_name = os.environ['secret_manager_name']

queue_url = f'https://sqs.ap-southeast-2.amazonaws.com/{accountID}/{os.environ["queue_name"]}'

# rds config info
response = secrets_manager.get_secret_value(
    SecretId=secret_manager_name)
secret_values= json.loads(response["SecretString"])
rds_host = secret_values["host"]
name = secret_values["username"]
password = secret_values["password"]
db_name = rds_config.db_name
rds_table = os.environ["rds_table_name"]

# sftp config info
sftp_username = sftp_config.username
private_keyname = sftp_config.private_keyname


def init_log():
    '''
    modify the root logger
    '''
    log_format = "%(levelname)s - %(message)s"
    root = logging.getLogger()
    if root.handlers:
        for handler in root.handlers:
            root.removeHandler(handler)
    logging.basicConfig(format=log_format, level=logging.INFO)


def get_time_now(text=False, format="%Y-%m-%d %H:%M:%S"):
    dt = datetime.utcnow()
    aus_tz = 'Australia/Melbourne'
    aus = pytz.timezone(aus_tz)
    datetime_obj_aus = dt.astimezone(aus)
    hours_plus = int(str(datetime_obj_aus).split('+')[-1].split(':')[0])

    aus_time = dt + timedelta(hours=hours_plus)
    if text is True:
        return aus_time.strftime(format)
    else:
        return aus_time



def determine_report(fileName, sourceURL):
    '''
        check if error report needs to be sent
    '''
    if 'mercari' in sourceURL:
        fileDate = datetime.strptime(fileName, 'ClosingRates%Y-%m-%d.csv') + timedelta(hours=int(10))
        weekno = fileDate.weekday()
        if weekno >= 5:
            logging.info("weekends, don't need to report mercari error")
            return False
        else:
            current_hour = get_time_now().hour
            if current_hour >= 20:
                return True
            else:
                logging.info("still early, chill, don't need to report mercari error")
                return False
    return True

def sns_report(e_msg, e_dtl):
    '''
    send sns message to alert
    Args:
      e_message: the message of the error
      e_dtl: the details of the error
    '''
    try:
        e_msg = str(e_msg).replace('"', "'")
        e_dtl = str(e_dtl).replace('"', "'")
        msg = f'{{"REASON": "{e_msg}", "DETAIL": "{e_dtl}, time: {get_time_now()}"}}, "MESSAGE": "Please take a look"'

        sns.publish(TopicArn=f'arn:aws:sns:ap-southeast-2:{accountID}:{snstopic}',
                    Message=msg,
                    Subject='Error from MarketData Downloader!')
        logging.info(f"[DEV]: A report has been sent to {snstopic}, content: {msg}")
    except Exception as e:
        logging.error(f"[DEV] Fail to send SNS, reason: {e}",  exc_info=True)


def handle_error(e_id, e_url, e_message, msg_receipt):
    '''
    Handle the situation where there's something from with the source.
    1. Modify the source csv file.
    2. Send SNS nofication to developers
    3. Delete the message(task) in SQS
    Args:
      e_id (string): the ID(in the source csv file) of the invalid source
      e_url (string): the URL of the invalid source
      e_message (string): The error message returned from the invalid source, sent to developers
      msg_receipt (string): the receipt of the message from SQS, used to delete the message
    '''
    try:
        e_message = str(e_message).replace('"', "'")
        msg = f'{{"ID": "{e_id}", "URL": "{e_url}"}}'
        sns_report(e_message, msg)
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg_receipt)
        logging.info('SQS task deleted')
    except Exception as e:
        logging.error(f"[DEV] Fail to handle error, reason: {e}",  exc_info=True)


def fileexists(buckename, filekey):
    '''
    check if the key exists in the s3 bucket
    Args:
      bucketname: the name of the bucket
      filekey: the key of the file
    '''
    with conn.cursor() as cur:
        cur.execute(f'SELECT EXISTS(SELECT 1 FROM {rds_table} WHERE file_key="{filekey}" LIMIT 1)')
        if cur.fetchone()[0] == 0:
            return False
        else:
            logging.info(f'Filekey exists: {filekey}')
            return True


def insert_record(p_file_key, p_source_id, p_url):
    '''
    insert the file record to mysql db
    Args:
      p_file_key: the s3 key of the file
      p_source_id: the source id of file
      p_url: the url of the file
    '''
    with conn.cursor() as cur:
        dttm = get_time_now(format="%Y-%m-%d %H:%M:%S")
        insert_sql = f'insert ignore into {rds_table} (file_key, dttm, source_id, url, bucket) values("{p_file_key}", "{dttm}", {p_source_id}, "{p_url}", "{dbucket}")'
        cur.execute(insert_sql)
    conn.commit()
    logging.info(f'[DEV] A record has been inserted into {rds_table}, file key: {p_file_key}')


def download_upload(file_url, s3_path):
    '''
    download file and upload it to s3 bucket
    Args:
      file_url (string): the url of the target file
      s3_path (string): the key in s3 bucket
    '''
    logging.info(f'Start downloading a file, url: {file_url}')
    try:
        data = urllib.request.urlopen(file_url).read()
    except Exception as e:
        logging.error(f"[DEV] Fail to read page, reason: {e}", exc_info=True)
        logging.info(f'Start downloading a file with header, url: {file_url}')
        head = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.84 Safari/537.36',}
        req = urllib.request.Request(file_url, headers=head)
        data = urllib.request.urlopen(req, timeout=10).read()
    s3.put_object(Bucket=dbucket, Key=s3_path, Body=data)
    data = 0
    logging.info(f'File uploaded, key: {s3_path}')


def helper(func):
    '''
    build a decorator
    '''
    def _decorator(source, msg_receipt, overwrite=False):
        logging.info(f'[DEV] Start handling ID: {source["ID"]}, URL: {source["URL"]}')
        func(source, msg_receipt, overwrite)
        logging.info(f'Finished handling ID: {source["ID"]}')
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg_receipt)
        logging.info('SQS task deleted, because the task is finished')
    return _decorator


@helper
def link_files(source, msg_receipt, overwrite=False):
    '''
    Download files from a link and upload them to s3 bucket
    Args:
      source (dict): the message content from SQS(the complete info of the source)
      msg_receipt (string): the receipt of the message from SQS, used to delete the message
      overwrite (bool,optional): whether to overwrite the file in the s3 bucket
    '''
    source_url = source['URL']
    file_key = source['Key']
    source_id = source['ID']
    try:
        file_page = urllib.request.urlopen(source_url)
        file_page = BeautifulSoup(file_page, 'html.parser')
    except Exception as e:
        logging.error(f"[DEV] Fail to read page, reason: {e}",  exc_info=True)
        handle_error(source['ID'], source_url, e, msg_receipt)
    else:
        for f in file_page.find_all('a'):
            try:
                file_url = urllib.parse.urljoin(source_url, f.get('href'))
                logging.info(f'[DEV] File url is {file_url}')
                file_name = file_url.split('/')[-1]
                if file_name and fnmatch.fnmatch(file_name, source['PATTERN']):
                    if overwrite:
                        # overwrite the file, no need to check repeat
                        download_upload(file_url, f'{file_key}/{file_name}')
                    else:
                        if not fileexists(dbucket, f'{file_key}/{file_name}'):
                            download_upload(file_url, f'{file_key}/{file_name}')
                            insert_record(f'{file_key}/{file_name}', source_id, source_url)
            except Exception as e:
                logging.error(f'[DEV] Error when downloading a file, reason: {e}, ID: {source_id}, url: {source_url}, fname: {f} now continue',  exc_info=True)
                continue


@helper
def dlink_file(source, msg_receipt, overwrite=False):
    '''
    Download a file from direct link and upload it to s3 bucket
    Args:
      source (dict): the message content from SQS(the complete info of the source)
      msg_receipt (string): the receipt of the message from SQS, used to delete the message
    '''

    now = get_time_now()
    time_preffix = str(now.strftime("D%d%m%y_%H-"))

    o_file_name = str(source['PATTERN'])
    file_name = time_preffix + o_file_name
    file_key = source['Key']
    url = source['URL']
    source_id = source['ID']
    # if file_name == "wholesaleprice.csv" or file_name == "weeklyratecard.csv":
    #     cur_date = str(datetime.now().date())
    #     file_name = cur_date + "-" + file_name
    try:

        if not fileexists(dbucket, f'{file_key}/{file_name}'):
            download_upload(url, f'{file_key}/{file_name}')
            insert_record(f'{file_key}/{file_name}', source_id, url)
    except Exception as e:
        logging.error(f"[DEV] Fail to download a dlink file, reason: {e}, ID: {source_id}, url: {url}",  exc_info=True)
        whether_report = determine_report(o_file_name, url)
        if whether_report:
            handle_error(source_id, url, e, msg_receipt)

@helper
def ftp_files(source, msg_receipt, overwrite=False):
    """
    Download files from ftp source and upload them to s3 bucket
    Args:
      source (dict): the message content from SQS(the complete info of the source)
      msg_receipt (string): the receipt of the message from SQS, used to delete the message
    """

    file_key = source['Key']
    url = source["URL"]
    source_id = source['ID']

    try:
        response = urllib.request.urlopen(url)
        file_list = response.read().decode().split('\r\n')[0: -1]
        files = map(lambda x: x.split()[-1], file_list)
        fnames = fnmatch.filter(files, source['PATTERN'])
    except Exception as e:
        logging.error(f"[DEV] Fail to read ftp directory, reason: {e}",  exc_info=True)
        handle_error(source_id, url, e, msg_receipt)
    else:
        for file_name in fnames:
            try:
                file_url = urllib.parse.urljoin(url, file_name)
                logging.info(f'[DEV] File url is {file_url}')
                if not fileexists(dbucket, f'{file_key}/{file_name}'):
                    download_upload(file_url, f'{file_key}/{file_name}')
                    insert_record(f'{file_key}/{file_name}', source['ID'], url)
            except Exception as e:
                logging.error(f'[DEV] Error when handling downloading a file , reason: {e}, ID: {source_id}, url: {url}, fname: {file_name} now continue',  exc_info=True)
                continue


@helper
def dftp_files(source, msg_receipt, overwrite=False):
    '''
    Download a file from direct ftp source and upload it to s3 bucket
    Args:
      source (dict): the message content from SQS(the complete info of the source)
      msg_receipt (string): the receipt of the message from SQS, used to delete the message
    '''
    now = get_time_now()
    time_preffix = str(now.strftime("D%d%m%y_%H-"))

    file_name = str(source['PATTERN'])
    file_name = time_preffix + file_name
    file_key = source['Key']
    url = source["URL"]
    source_id = source['ID']
    try:
        if not fileexists(dbucket, f'{file_key}/{file_name}'):
            download_upload(url, f'{file_key}/{file_name}')
            insert_record(f'{file_key}/{file_name}', source_id, url)
    except Exception as e:
        logging.error(f"[DEV] Fail to download a dftp file, reason: {e}, ID: {source_id}, link: {url}",  exc_info=True)
        handle_error(source_id, source['URL'], e, msg_receipt)


@helper
def sftp_files(source, msg_receipt, overwrite=False):
    '''
    Download files from sftp server of asx and upload them to s3 bucket. Only for asx server!!!
    Args:
      source (dict): the message content from SQS(the complete info of the source)
      msg_receipt (string): the receipt of the message from SQS, used to delete the message
    '''
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    url = source["URL"]
    path = source["Key"]
    source_id = source['ID']
    file_name_pattern = source['PATTERN']
    asx_category = ["FinalSnapshot", "OpenInterest",
                    "PrelimSnapshot", "TradeLog"]
    try:
        response = secrets_manager.get_secret_value(SecretId='asx-private-key')
        private_key_str = response["SecretString"]
        with open(private_keyname, "w") as f:
            f.write(private_key_str)
        with pysftp.Connection(url, username=sftp_username, private_key=private_keyname, cnopts=cnopts) as sftp:
            for f in sftp.listdir(path):
                if fnmatch.fnmatch(f, file_name_pattern):
                    file_key_sftp = os.path.join(path, f)
                    if list(filter(lambda x: x in f, asx_category)):
                        file_key = os.path.join(path, filter(
                            lambda x: x in f, asx_category).__next__(), f)
                    else:
                        file_key = os.path.join(path, f)
                    if not fileexists(dbucket, f'{file_key}'):
                        flo = BytesIO()
                        sftp.getfo(file_key_sftp, flo)
                        flo.seek(0)
                        s3.put_object(Bucket=dbucket,
                                      Key=file_key, Body=flo)
                        logging.info(f"[INFO] Download and upload to {file_key}")
                        insert_record(file_key, source_id, url)
    except Exception as e:
        logging.error(f'[DEV] Error when handling downloading a sftp file , reason: {e}, ID: {source_id}, link: {url} now continue',  exc_info=True)
        handle_error(source_id, url, e, msg_receipt)

#build the connection to msql server, to check file record
try:
    conn = pymysql.connect(rds_host, user=name,
                           passwd=password, db=db_name, connect_timeout=5)
except Exception as e:
    logging.error(f"[DEV] Could not connect to MySql instance, reason: {e}",  exc_info=True)
    sns_report("Cannot connect to rds!!!", e)
    sys.exit()
logging.info("SUCCESS: Connection to RDS mysql instance succeeded")


def handler(event, context):
    '''
    Standard aws lambda handler function
    This function handler should be triggered by scheduled event at certain interval
    Receive messages(task) from SQS and execute tasks depends on the source type
    '''
    init_log()
    count = int(os.environ['msgcount'])
    logging.info(f"[INFO] Attempt to receive {count} messages")
    sftp_flag = 0
    for i in range(0, count):
        try:
            response1 = sqs.receive_message(
                QueueUrl=queue_url, MaxNumberOfMessages=1)
            if 'Messages' in response1:
                msg_receipt = response1['Messages'][0]['ReceiptHandle']
                msg_content = json.loads(response1['Messages'][0]['Body'])
                msg_content_type = msg_content['TYPE']
                if (msg_content_type == "LINKS"):
                    link_files(msg_content, msg_receipt)
                elif(msg_content_type == "LINKS_OVERWRITE"):
                    link_files(msg_content, msg_receipt, overwrite=True)
                elif(msg_content_type == "DIRECT"):
                    dlink_file(msg_content, msg_receipt)
                elif(msg_content_type == "DIRECT_FTP"):
                    dftp_files(msg_content, msg_receipt)
                elif(msg_content_type == "FTP_FILES"):
                    ftp_files(msg_content, msg_receipt)
                elif(msg_content_type == "SFTP"):
                    if sftp_flag == 0:
                        sftp_flag = 1
                    else:
                        logging.info("[INFO] Sleep 20s for another sftp connection")
                        time.sleep(20)   # if two sftp connection is too close, sftp connection gonna fail
                    sftp_files(msg_content, msg_receipt)
                else:
                    logging.error(f"[DEV] FILE TYPE ERROR!!!, MESSAGE: {msg_content}")
        except Exception as e:
            logging.error(f"[DEV] Fail to download, reason: {e}",  exc_info=True)
            continue
