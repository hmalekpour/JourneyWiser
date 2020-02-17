import os
import urllib2
import boto3
from bs4 import BeautifulSoup
import gzip
import shutil
import wget
import validators

"""
This script is used to scrape listing and calendar data from insideairbnb.com

Steps: 
    Download a zip file
    Uncompress and rename the file
    Store in S3 bucket
    Remove the local copy
    Loop to get them all
"""


def main():
    URL = 'http://insideairbnb.com/get-the-data.html'
    conn = urllib2.urlopen(URL)
    html = conn.read()
    soup = BeautifulSoup(html) # save html to BeautifulSoup object
    links = soup.find_all('a')


    for tag in links:
        link = tag.get('href',None)

        try:
            linkls = link.split('/')
        except:
            continue

        if len(linkls) > 4:
        
            if linkls[-1] == 'listings.csv.gz' or linkls[-1] == 'calendar.csv.gz':
                #use the info in the url to rename the files in the "city_date_type.csv" format
                new_file_name = linkls[-4] + '_' + linkls[-3] + '_' + linkls[-1][:-3]

                if validators.url(link): #check if url is valid
                    file_name = wget.download(link) #download from url
                    print('\n' + new_file_name + ' downloaded')

                    try:
                        with gzip.open(file_name,'r') as f_in, open(file_name[:-3],'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out) #unzip
                            print('file unzipped')
               
                    except: #unzip error means file url empty so skip this
                        os.remove(file_name)
                        os.remove(file_name[:-3])
                        print(new_file_name + ' is empty')
                        continue

                    s3 = boto3.client('s3')
                    s3.upload_file(file_name[:-3], 'hodabnb', new_file_name)
                    print("Upload to s3")
                    os.remove(file_name)
                    os.remove(file_name[:-3])
                    print('file removed')
           
                else:
                    print(new_file_name + ' URL not valid')

if __name__ == "__main__":
    main()
