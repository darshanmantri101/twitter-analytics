#!/usr/bin/env python
# encoding: utf-8

import tweepy #https://github.com/tweepy/tweepy
import json
import matplotlib.pyplot as plt
import pandas as pd
import xlsxwriter
import datetime
import pytz
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
import requests 
import ftplib
import os


#To upload file on remote server via FTP
def upload(ftp, file):
    ext = os.path.splitext(file)[1]
    if ext in (".txt", ".htm", ".html",".js"):
        ftp.storlines("STOR " + file, open(file))
    else:
        ftp.storbinary("STOR " + file, open(file, "rb"), 1024)



#connecting to mysql DB
conn =  MySQLdb.connect(db="excelv6b_twitter_data", user="excelv6b_root", passwd="excel", host="exceltechserve.in")

#Twitter API credentials
consumer_key = "dR0SOF71UwMRLPjKAkSMDxAln"
consumer_secret = "oMlwTuKUAimidQMpYXSy2lI2m33jfH6C1Mwhdr1IoRznzcd9HZ"
access_key = "445966223-O58nnYLy2hmcYnp37RUnsVrJFZyqJv1igmDuNLaM"
access_secret = "ZoaQBGrBF8LasKQgdXf3CJroQVch2aEQpWFPQ6HdDF5nh"
gmt = pytz.timezone('GMT')

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ("Type %s not serializable" % type(obj))

#to retrieve tweets
def get_all_tweets(screen_name):
    
    #Twitter only allows access to a users most recent 3240 tweets with this method
    
    #authorize twitter, initialize tweepy
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)
    
    #initialize a list to hold all the tweepy Tweets
    alltweets = []    
    
    #make initial request for most recent tweets (200 is the maximum allowed count)
    new_tweets = api.user_timeline(screen_name = screen_name,count=200)
    
    #save most recent tweets
    alltweets.extend(new_tweets)
    
    #save the id of the oldest tweet less one
    oldest = alltweets[-1].id - 1
    
    #keep grabbing tweets until there are no tweets left to grab
    while len(new_tweets) > 0:
        
        #all subsiquent requests use the max_id param to prevent duplicates
        new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)
        
        #save most recent tweets
        alltweets.extend(new_tweets)
        
        #update the id of the oldest tweet less one
        oldest = alltweets[-1].id - 1

        print("...%s tweets downloaded so far" % (len(alltweets)))


    #workbook = xlsxwriter.Workbook(screen_name+'.xlsx')
    #worksheet = workbook.add_worksheet()
    #row = 0
    #col = 0
    #worksheet.write(row, col,     "ID:")
    #worksheet.write(row, col + 1, "User ID")
    #//worksheet.write(row, col , "Created")
    #//worksheet.write(row, col + 1, "Favorited Count:")
    #worksheet.write(row, col + 5, "In reply to screen name:")
    #worksheet.write(row, col + 6, "In reply to status ID:")
    #worksheet.write(row, col + 7, "In reply to status ID str:")
    #worksheet.write(row, col + 8, "In reply to user ID:")
    #worksheet.write(row, col + 9, "In reply to user ID str:")
    #worksheet.write(row, col + 10, "Retweeted:")
    #//worksheet.write(row, col + 2, "Retweet count:")

    #row += 1

    cursor = conn.cursor()
    
    #delete table if exists
    query="DROP TABLE IF EXISTS `"+screen_name+"`;"
    print(query)
    cursor.execute(query)
    #create table
    query="CREATE TABLE IF NOT EXISTS `"+screen_name+"` (creation_date DATETIME,fav_count INT,retweet_count INT);"
    print(query)
    cursor.execute(query)
    
    #iterate through retreived tweets 
    for tweet in alltweets:
        #worksheet.write(row, col,     tweet.id)
        # make python recognize tweet as GMT
        gmt_date = gmt.localize(tweet.created_at) 
        # print the results to make sure it worked
        fmt = '%Y-%m-%d %H:%M:%S'
        gmt_date=gmt_date.strftime(fmt)
        #print(gmt_date)
        fav_count=int(tweet.favorite_count)
        #date=gmt_date.date
        #time=gmt_date.time
        #hour=gmt_date.hour
        #print(hour)
        #worksheet.write(row, col + 1, tweet.user.id)
        #//worksheet.write(row, col , gmt_date.strftime(fmt))
        #//worksheet.write(row, col + 1, tweet.favorite_count)
        #worksheet.write(row, col + 5, tweet.in_reply_to_screen_name)
        #worksheet.write(row, col + 6, tweet.in_reply_to_status_id)
        #worksheet.write(row, col + 7, tweet.in_reply_to_status_id_str)
        #worksheet.write(row, col + 8, tweet.in_reply_to_user_id)
        #worksheet.write(row, col + 9, tweet.in_reply_to_user_id_str)
        #worksheet.write(row, col + 10, tweet.retweeted)
        #//worksheet.write(row, col + 2, tweet.retweet_count)

        #insert tweet data in table
        cursor.execute("INSERT INTO `"+screen_name+"` (creation_date,fav_count,retweet_count) values(%s,%s,%s)",(gmt_date,int(tweet.favorite_count),int(tweet.retweet_count),))
        
        #row += 1
    #write tweet objects to JSON
    #//workbook.close()
    #file = open('tweet.json', 'w')
    
    #JSON file for overall data
    cursor.execute("select DATE_FORMAT(a.creation_date, '%m/%d/%Y') as 'Date', DAYNAME(a.creation_date) as 'Day of the Week', CONCAT(Time(a.creation_date)) as 'Time', HOUR(a.creation_date) as 'Hour of the day',  case when HOUR(a.creation_date) > 6 and HOUR(a.creation_date) < 12 then 'Morning' when HOUR(a.creation_date) >= 12 and HOUR(a.creation_date) < 16 then 'Afternoon' when HOUR(a.creation_date) >= 16  and HOUR(a.creation_date)< 21 then 'Night' else 'Early morning' end as 'Hour Bucket', a.fav_count as 'Fav Count', a.retweet_count as 'Retweet count' from `"+screen_name+"` a")
    columns = cursor.description
    result = [{columns[index][0]:column for index, column in enumerate(value)}   for value in cursor.fetchall()]
    with open('data.json', 'w') as outfile:  
        json.dump(result, outfile)



    #JSON file for frequency analysis data
    cursor.execute("SELECT DATE_FORMAT( a.creation_date,  '%m/%d/%Y' ) AS  'Date', COUNT( a.creation_date ) AS  'total_tweet', CAST( SUM( a.fav_count ) AS UNSIGNED ) AS 'fav_ount', CAST( SUM( a.retweet_count ) AS UNSIGNED ) AS  'retweet_count' from `"+screen_name+"` a GROUP BY DATE_FORMAT( a.creation_date,  '%m/%d/%Y' )")
    columns = cursor.description
    result = [{columns[index][0]:column for index, column in enumerate(value)}   for value in cursor.fetchall()]
    with open('frequency_analysis.json', 'w') as outfile:  
        json.dump(result, outfile)



    #JSON file for hourly analysis data   
    cursor.execute("select HOUR(a.creation_date) as 'hour_of_the_day', count(a.creation_date) as 'total_tweets', CAST(sum(a.fav_count) as UNSIGNED) as 'fav_count', CAST(sum(a.retweet_count) as UNSIGNED) as 'retweet_count' from `"+screen_name+"` a group by HOUR(a.creation_date)")
    columns = cursor.description
    result = [{columns[index][0]:column for index, column in enumerate(value)}   for value in cursor.fetchall()]
    with open('hourly_analysis.json', 'w') as outfile:  
        json.dump(result, outfile)



    #JSON file for time-range analysis data
    cursor.execute("SELECT CASE WHEN HOUR( a.creation_date ) >=7 AND HOUR( a.creation_date ) <12 THEN 'Morning' WHEN HOUR( a.creation_date ) >=12 AND HOUR( a.creation_date ) <=16 THEN 'Afternoon' WHEN ( HOUR( a.creation_date ) >16 AND HOUR( a.creation_date ) <=23 ) THEN 'Night' WHEN ( HOUR( a.creation_date ) >=0 AND HOUR( a.creation_date ) <=3 ) THEN 'Night' ELSE 'Early morning' END AS hourBucket, COUNT( a.creation_date ) AS 'total_tweets', CAST( SUM( a.fav_count ) AS UNSIGNED ) AS 'fav_count', CAST( SUM( a.retweet_count ) AS UNSIGNED ) AS 'retweet_count' from `"+screen_name+"` a GROUP BY hourBucket ORDER BY ( CASE hourBucket WHEN 'Early Morning' THEN 0 WHEN 'Morning' THEN 1 WHEN 'Afternoon' THEN 2 WHEN 'Night' THEN 3 END )")
    columns = cursor.description
    result = [{columns[index][0]:column for index, column in enumerate(value)}   for value in cursor.fetchall()]
    with open('hourbucket_analysis.json', 'w') as outfile:  
        json.dump(result, outfile)


    #JSON file for Day wise analysis data
    cursor.execute("select DAYNAME(a.creation_date) as 'day_of_the_week', count(a.creation_date) as 'total_tweets', CAST(sum(a.fav_count) as UNSIGNED) as 'fav_count', CAST(sum(a.retweet_count) as UNSIGNED) as 'retweet_count' from `"+screen_name+"` a group by DAYNAME(a.creation_date) ORDER BY FIELD( DAYNAME( a.creation_date ) ,  'MONDAY',  'TUESDAY',  'WEDNESDAY',  'THURSDAY',  'FRIDAY',  'SATURDAY',  'SUNDAY' )")
    columns = cursor.description
    result = [{columns[index][0]:column for index, column in enumerate(value)}   for value in cursor.fetchall()]
    with open('DayofWeek_analysis.json', 'w') as outfile:  
        json.dump(result, outfile)


    #connect using FTP and upload files
    ftp = ftplib.FTP("ftp.exceltechserve.in")
    ftp.login("darshan@exceltechserve.in", "root1234")
    filename = "DayofWeek_analysis.json"
    upload(ftp, filename)
    filename = "hourbucket_analysis.json"
    upload(ftp, filename)
    filename = "hourly_analysis.json"
    upload(ftp, filename)
    filename = "frequency_analysis.json"
    upload(ftp, filename)
    filename = "data.json"
    upload(ftp, filename)

    

    conn.commit()
    conn.close()
    #file = open('tweet.json', 'w') 
    #print("Writing tweet objects to JSON please wait...")
    #for status in alltweets:
     #   json.dump(status._json,file,sort_keys = True,indent = 4)
    
    #close the file
    print("Done")
    
    #file=json.dumps(result,cls=DateTimeEncoder)

    

if __name__ == '__main__':
    
    #pass in the username of the account you want to download
    get_all_tweets("@SrBachchan")
