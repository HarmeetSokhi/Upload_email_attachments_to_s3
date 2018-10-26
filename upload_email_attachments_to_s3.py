#!/usr/bin/python
# **********************************************************************************************************************
#    Python Script for Automating the Download and Upload of Email Attachments
#    -------------------------------------------------------------------------
#
#    Notes:
#
#    1. This script fetches the list of emails that are received yesterday.
#    2. Filters out emails that has particular keywords in the attachments from the list in [1].
#    3. Downloads the filtered emails from [2].
#    4. Parses out S3 path from the filename of the attachments itself.
#       For eg: if filename is folder1_folder2_folder3_S3upload_20180901.csv. In this case S3upload is the keyword.
#       s3_path = folder1/folder2/folder3/
#    5. Everything before keyword will be considered as s3 path where the file will be uploaded.
# **********************************************************************************************************************

from pprint import pprint
import traceback
import datetime
import imaplib
import email
import boto3
import sys
import os

imap_server = "imap.gmail.com"
dwld_dir = "xxxxxxx"
key_filename = os.environ['keyword']
s3_bucket = os.environ['s3_bucket']

# email user id and password defined as Jenkins passwords
user = os.environ['user_id']
pwd = os.environ['user_id_pwd']

# supply the particular date for which you want to download attachments
# supply in the format - "01-Jan-2018"
date_received = os.environ['date_received']

today = datetime.datetime.now()
yesterday = today - datetime.timedelta(days=1)
today_date = datetime.datetime.strftime(today, "%d-%b-%Y")
yesterday_date = datetime.datetime.strftime(yesterday, "%d-%b-%Y")

# when date is not supplied form the Jenkins parameter, always consider yesterday's date to search/sort emails
if date_received == "0":
    yesterday_date = yesterday_date
else:
    yesterday_date = date_received


# Connect to the gmail imap server
def connect_imapMail():
    try:
        print(">>> Connecting to the {}.................".format(imap_server))
        imapSession = imaplib.IMAP4_SSL(imap_server)
        imapSession.login(user, pwd)
        print(">>> Connection Successful!")
        print("-------------------------------------------------------------------------------------------------------")
        return imapSession
    except Exception as e:
        print('>> Error encountered while connecting to the server! \n Error:::: %s ' % e)
        type_, value_, traceback_ = sys.exc_info()
        print(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


"""
This function first makes a list of emails that was sent on yesterday. Then from that list, it filters out the 
emails with attachemts that has the specific keyword defined in the begining of the script ie. key_filename.
Then returns that final list of emails which are to be downloaded.
"""

def search_filter_mails(imapSession):
    try:
        print(">>> Searching INBOX for emails sent on {}".format(yesterday_date))
        # Select the mailbox
        imapSession.select("INBOX")
        # search for the emails from the particular sender
        resp, items = imapSession.search(None, '(SENTON "{}")'.format(yesterday_date))
        items = items[0].split()
        items.sort()
        print(">>> Number of emails found with current search criterion: {}".format(len(items)))
        print(">>> EmailIds found during search: {}".format(items))
        emailid_with_attachs = []
        filename_list = []
        for emailid in items:
            resp, data = imapSession.fetch(emailid, "(RFC822)")
            # email_body = data[0][1]                       # use this for compatibility with only python-2
            email_body = data[0][1].decode('utf-8')         # this for compatibility with both python-2 and Python-3
            mail = email.message_from_string(email_body)
            if mail.get_content_maintype() != 'multipart':
                continue
            for part in mail.walk():
                if part.get_content_type() == "text/plain":
                    body = part.get_payload(decode=True)
                    # pprint(body)
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                filename = part.get_filename()
                if filename is not None:
                    # make a list of emails with a specific keyword in the filename
                    if key_filename.lower() in filename.lower():
                        emailid_with_attachs.append(emailid)
                        filename_list.append(filename)
        print(">>> Number of Emails that satifies the filter parameter: {}".format(len(emailid_with_attachs)))
        print(">>> EmailIds that has the key - '{}' in the filename of the attachments: {}"
              .format(key_filename, emailid_with_attachs))
        print("-------------------------------------------------------------------------------------------------------")
        return emailid_with_attachs
    except Exception as e:
        pprint('Error encountered while searching inboxes for emails! \n Error:::: %s' % e)
        type_, value_, traceback_ = sys.exc_info()
        print(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


"""
This function downloads the attachments from the list of emails returned by function - search_filter_mails().
The functions for parsing out s3_path and function to upload file to S3 are also called within this function.
"""

def download_emails_attachs(imapSession, dwld_email_list):
    try:
        for emailid in dwld_email_list:
            print(">>> Downloading attachment for the email with id - {}..................".format(emailid))
            resp, data = imapSession.fetch(emailid, "(RFC822)")
            # email_body = data[0][1]                       # use this for compatibility with only python-2
            email_body = data[0][1].decode('utf-8')         # this for compatibility with both python-2 and Python-3
            mail = email.message_from_string(email_body)
            if mail.get_content_maintype() != 'multipart':
                continue
            subject = ""
            if mail["subject"] is not None:
                subject = mail["subject"]
            print(">>> [" + mail["From"] + "] :" + subject)
            for part in mail.walk():
                if part.get_content_maintype() == 'multipart':
                    continue
                if part.get('Content-Disposition') is None:
                    continue
                filename = part.get_filename()

                # add today's date in the filename
                f1, ext = os.path.splitext(filename)
                filename = f1 + "_" + today_date + ext

                # call a function to parse out the s3 path from the filename
                s3_path = parse_s3path_from_filename(filename)

                local_file_path = os.path.join(dwld_dir, filename)
                fp = open(local_file_path, 'wb')
                fp.write(part.get_payload(decode=True))
                fp.close()
                print(">>> Attachemnt file  - '{}' successfully downloaded for EmailId - '{}'!"
                      .format(filename, emailid))
                # replace() is used to avoid mixup of forward and back the slashes
                file_in_s3 = os.path.join(s3_path, filename).replace("\\", "/")

                # calling the function to upload the attachment to S3 bucket
                uploadToS3(local_file_path, s3_bucket, file_in_s3)

                print(">>> Removing file - '{}' from local EC2 path........".format(filename))
                os.remove(local_file_path)
                print(">>> File - '{}' successfully removed!".format(local_file_path))
            print("___________________________________________________________________________________________________")
    except Exception as e:
        pprint('Error encountered while downloading mails! \n Error:::: %s' % e)
        type_, value_, traceback_ = sys.exc_info()
        print(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


# this function parses out the s3 path present in the filename of the attachments
def parse_s3path_from_filename(filename):
    try:
        strs = filename.split("_" + key_filename)
        folders = strs[0].split("_")
        s3_path = ''
        for each in folders:
            s3_path = s3_path + each + '/'
        return s3_path
    except Exception as e:
        pprint('Error encountered while fparsing out s3_path from filename! \n Error:::: %s' % e)
        type_, value_, traceback_ = sys.exc_info()
        print(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


# this function uploads the downloaded attachment to S3 bucket
def uploadToS3(file_to_upload, s3_bucket, file_name_in_s3):
    try:
        print(">>> Uploading {} to S3.....................".format(file_to_upload))
        s3 = boto3.resource('s3')
        s3.Bucket(s3_bucket).upload_file(file_to_upload, file_name_in_s3)
        print(">>> Successfully Uploaded to the bucket s3://bucketname/{}".format(file_name_in_s3))
        return "s3://bucketname/" + file_name_in_s3
    except Exception as e:
        pprint('Unable to upload to s3! \n Error:::: %s' % e)
        type_, value_, traceback_ = sys.exc_info()
        print(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


def main():
    imapSession = connect_imapMail()
    dwld_email_list = search_filter_mails(imapSession)
    download_emails_attachs(imapSession, dwld_email_list)
    print(">>> Logging out and terminating Imap Session.....................")
    imapSession.close()
    imapSession.logout()
    print(">>> Imap Session successfully terminated!")
    print(">>>>>>>>>>>>>>>>>>>>>>>END OF SCRIPT<<<<<<<<<<<<<<<<<<<<<<<<<<<")


if __name__ == '__main__':
    main()