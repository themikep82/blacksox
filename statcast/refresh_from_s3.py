import sys
import requests
import io
import os
from time import sleep
from datetime import datetime, timedelta
from slackclient import SlackClient
sys.path.append("..")

from dbhelper import *

SLACK_ENABLED=1

START_DATE=datetime(2008, 3, 25)
END_DATE=datetime(2017, 11, 1)	#datetime.today()

def main():

	if SLACK_ENABLED:
		sc = SlackClient(credentials['slackOAuthToken'])
		sc.api_call(
			"chat.postMessage",
			channel="#dataimport",
			text="Data Import Started!",
			user="U945VE14N")
		
	dateWindow=START_DATE
	
	initTable()
	
	while dateWindow <= END_DATE:
	
		if dateWindow.month not in [1,2,12]: #ignore January, February, December
	
			print(dateWindow.strftime('%Y-%m-%d'))

			loadDataToRedshift(dateWindow.strftime('%Y-%m-%d'))

			dateWindow+=timedelta(days=1)
		
		else:
		
			dateWindow+=timedelta(days=1)
		
	if SLACK_ENABLED:
		sc.api_call(
			"chat.postMessage",
			channel="#dataimport",
			text="Data Import Complete!",
			user="U945VE14N")

def initTable():
	#create statcast events table
	runSQLFromFile('table_init.sql')

def loadDataToRedshift(dateString):

	#Redshift SQL COPY command to load data from CSV into DB table
	try:
		executeSQL("COPY statcast.pitch_events FROM 's3://blacksox-data/statcast-historic/statcast_ready" + dateString +".csv' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ',';")

	except:
		pass
	
if __name__ == "__main__":
	main()