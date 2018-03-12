import sys
sys.path.append("..")

from dbhelper import *

DATA_FILE_NAME='statcast_pitch_data_2008-2017.csv'

def main():
	
	initTable()
	
	loadDataToRedshift(cleanData())

def initTable():
	#create statcast events table
	runSQLFromFile('table_init.sql')

def cleanData():
	
	#load data from CSV.
	#Data obtained from https://baseballsavant.mlb.com/statcast_search
	#Group by Player Name & Event, click search, then click the download CSV link
	pitchData = pandas.read_csv(DATA_FILE_NAME, low_memory=False)
	
	#remove deprecated data and large 'des' column
	pitchData=pitchData.drop(['spin_dir','spin_rate_deprecated','break_angle_deprecated','break_length_deprecated','des','tfs_deprecated','tfs_zulu_deprecated','pos2_person_id'], axis=1)
	
	#fix duplicate pos2_person_id column
	pitchData.rename(columns={'pos2_person_id.1':'pos2_person_id'}, inplace=True)
	
	#replace the word 'null' in data with actual null value
	pitchData=pitchData.replace('null', '')
	
	#clean up decimal values on integer data
	pitchData['on_3b']=dotZeroBugFix(pitchData['on_3b'])
	pitchData['on_2b']=dotZeroBugFix(pitchData['on_2b'])
	pitchData['on_1b']=dotZeroBugFix(pitchData['on_1b'])
	
	return pitchData
	
def loadDataToRedshift(pitchData):

	#write sanatized data file to csv
	pitchData.to_csv('statcast_ready.csv', index=False)
	
	#move csv to S3 bucket
	S3upload('statcast_ready.csv','/statcast-historic/','blacksox-data')
	
	#Redshift SQL COPY command to load data from CSV into DB table
	executeSQL("COPY statcast.pitch_events FROM 's3://blacksox-data/statcast-historic/statcast_ready.csv' CREDENTIALS 'aws_access_key_id=" + credentials["AWS_Key"] + ";aws_secret_access_key=" + credentials["AWS_Secret"] + "' IGNOREHEADER 1 DELIMITER AS ',';")
	
if __name__ == "__main__":
	main()
