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

QUERY_DATE=datetime.today() + timedelta(days=-1)

def main():

	if SLACK_ENABLED:
		sc = SlackClient(credentials['slackOAuthToken'])
		sc.api_call(
			"chat.postMessage",
			channel="#dataimport",
			text="Data Import Started!")
		
	dateWindow=QUERY_DATE
		
	if dateWindow.month not in [1, 2, 12]: #ignore January, February, December

		print(dateWindow.strftime('%Y-%m-%d'))
		
		try:
	
			pitchEventData = collectStatcastPitchEvents(dateWindow.strftime('%Y-%m-%d'))
			
			if not pitchEventData.empty:
				
				pitchEventData = cleanData(pitchEventData)
			
				loadDataToRedshift(pitchEventData, dateWindow.strftime('%Y-%m-%d'))
				
				os.remove('statcast_ready' + dateWindow.strftime('%Y-%m-%d') +'.csv')
						
		except Exception as e:
			
			if SLACK_ENABLED:
				sc.api_call(
					"chat.postMessage",
					channel="#dataimport",
					text=str(e))
		
	if SLACK_ENABLED:
		sc.api_call(
			"chat.postMessage",
			channel="#dataimport",
			text="Data Import Complete!")

def cleanData(pitchEventData):
	
	#remove deprecated data and large 'des' column
	pitchEventData=pitchEventData.drop(['spin_dir','spin_rate_deprecated','break_angle_deprecated','break_length_deprecated','des','tfs_deprecated','tfs_zulu_deprecated'], axis=1)
	
	#remove new columns
	pitchEventData=pitchEventData.drop(['pitch_name', 'home_score', 'away_score', 'bat_score', 'fld_score', 'post_away_score', 'post_home_score', 'post_bat_score', 'post_fld_score'], axis=1)
	
	#fix duplicate pos2_person_id column
	pitchEventData=pitchEventData.drop(['pos2_person_id'], axis=1)
	pitchEventData.rename(columns={'pos2_person_id.1':'pos2_person_id'}, inplace=True)
	
	#replace the word 'null' in data with actual null value
	pitchEventData=pitchEventData.replace('null', '')
	
	#clean up decimal values on integer data
	pitchEventData['on_3b']=dotZeroBugFix(pitchEventData['on_3b'])
	pitchEventData['on_2b']=dotZeroBugFix(pitchEventData['on_2b'])
	pitchEventData['on_1b']=dotZeroBugFix(pitchEventData['on_1b'])
	pitchEventData['zone']=dotZeroBugFix(pitchEventData['zone'])
	pitchEventData['hit_distance_sc']=dotZeroBugFix(pitchEventData['hit_distance_sc'])
	pitchEventData['release_spin_rate']=dotZeroBugFix(pitchEventData['release_spin_rate'])
	pitchEventData['pos1_person_id']=dotZeroBugFix(pitchEventData['pos1_person_id'])
	pitchEventData['pos2_person_id']=dotZeroBugFix(pitchEventData['pos2_person_id'])
	pitchEventData['pos3_person_id']=dotZeroBugFix(pitchEventData['pos3_person_id'])
	pitchEventData['pos4_person_id']=dotZeroBugFix(pitchEventData['pos4_person_id'])
	pitchEventData['pos5_person_id']=dotZeroBugFix(pitchEventData['pos5_person_id'])
	pitchEventData['pos6_person_id']=dotZeroBugFix(pitchEventData['pos6_person_id'])
	pitchEventData['pos7_person_id']=dotZeroBugFix(pitchEventData['pos7_person_id'])
	pitchEventData['pos8_person_id']=dotZeroBugFix(pitchEventData['pos8_person_id'])
	pitchEventData['pos9_person_id']=dotZeroBugFix(pitchEventData['pos9_person_id'])
	pitchEventData['babip_value']=dotZeroBugFix(pitchEventData['babip_value'])
	pitchEventData['iso_value']=dotZeroBugFix(pitchEventData['iso_value'])
	pitchEventData['launch_speed_angle']=dotZeroBugFix(pitchEventData['launch_speed_angle'])
	pitchEventData['hit_location']=dotZeroBugFix(pitchEventData['hit_location'])

	
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