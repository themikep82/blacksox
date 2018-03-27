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

START_DATE=datetime(2008, 3, 1)
END_DATE=datetime.today()

def main():

	if SLACK_ENABLED:
		sc = SlackClient(credentials['slackOAuthToken'])
		sc.api_call(
			"chat.postMessage",
			channel="#dataimport",
			text="Data Import Started!",
			user="U945VE14N")
		
	dateWindow=START_DATE
	
	#initTable() uncomment if this is your first time running this script. It will drop and recreate the table
	
	while dateWindow <= END_DATE:
	
		if dateWindow.month in [11]: #ignore January, February, December
	
			print(dateWindow.strftime('%Y-%m-%d'))
			
			try:
		
				pitchEventData = collectStatcastPitchEvents(dateWindow.strftime('%Y-%m-%d'))
				
				if not pitchEventData.empty:
					
					pitchEventData = cleanData(pitchEventData)
				
					loadDataToRedshift(pitchEventData, dateWindow.strftime('%Y-%m-%d'))
					
					os.remove('statcast_ready' + dateWindow.strftime('%Y-%m-%d') +'.csv')
				
				dateWindow+=timedelta(days=1)
				
			except ValueException as e:
				
				if SLACK_ENABLED:
					sc.api_call(
						"chat.postMessage",
						channel="#dataimport",
						text="ValueError. Retrying in 5 seconds",
						user="U945VE14N")
			
					sleep(5)
							
			except ValueException as e:
				
				if SLACK_ENABLED:
					sc.api_call(
						"chat.postMessage",
						channel="#dataimport",
						text=str(e),
						user="U945VE14N")
						
					break
		
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

def cleanData(pitchEventData):
	
	#remove deprecated data and large 'des' column
	pitchEventData=pitchEventData.drop(['spin_dir','spin_rate_deprecated','break_angle_deprecated','break_length_deprecated','des','tfs_deprecated','tfs_zulu_deprecated'], axis=1)
	
	#fix duplicate pos2_person_id column
	pitchEventData=pitchEventData.drop(['pos2_person_id'], axis=1)
	pitchEventData.rename(columns={'pos2_person_id.1':'pos2_person_id'}, inplace=True)
	
	#replace the word 'null' in data with actual null value
	pitchEventData=pitchEventData.replace('null', '')
	
	#clean up decimal values on integer data
	pitchEventData['on_3b']=dotZeroBugFix(pitchEventData['on_3b'])
	pitchEventData['on_2b']=dotZeroBugFix(pitchEventData['on_2b'])
	pitchEventData['on_1b']=dotZeroBugFix(pitchEventData['on_1b'])
	
	return pitchEventData
	
def loadDataToRedshift(pitchEventData, dateString):

	#write sanatized data file to csv
	pitchEventData.to_csv('statcast_ready' + dateString +'.csv', index=False)
	
	#move csv to S3 bucket
	S3upload('statcast_ready' + dateString +'.csv','/statcast-historic/','blacksox-data')
	
	#Redshift SQL COPY command to load data from CSV into DB table
	executeSQL("COPY statcast.pitch_events FROM 's3://blacksox-data/statcast-historic/statcast_ready" + dateString +".csv' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ',';")

def collectStatcastPitchEvents(dateString):
	#modified from https://github.com/jldbc/pybaseball/blob/master/pybaseball/team_pitching.py

	url = 'https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details&'.format(dateString, dateString)
	
	s=requests.get(url).content
	df = pandas.read_csv(io.StringIO(s.decode('utf-8')))
	df.head()
		
	return df
	
if __name__ == "__main__":
	main()
