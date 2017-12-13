from boto.s3.key import Key
from boto.s3.connection import S3Connection
import psycopg2
import numpy
import pandas

from credentials import *

DEBUG=0	#set to 1 for verbose output

con=psycopg2.connect(dbname=credentials['database'],host=credentials['host'], port=credentials['port'], user=credentials['user'], password=credentials['password'])
con.autocommit=True
cur=con.cursor()
	
def S3upload(myFile, keyName, bucketString):

	conn = S3Connection(aws_access_key_id=credentials['AWS_Key'], aws_secret_access_key=credentials['AWS_Secret'], host='s3.amazonaws.com')
	bucket = conn.get_bucket(bucketString)
	keyName+=myFile
	if DEBUG:
		print(keyName)
	key = bucket.new_key(keyName).set_contents_from_filename(myFile)
	
def executeSQL(query):

	if DEBUG:	
		print(query)
	cur.execute(query)
	con.commit()
	
def runSQLFromFile(fileName):
	queryFileData=''
	
	with open(fileName, 'r') as queryFile:
		queryFileData+=queryFile.read()
	
	allQueries=queryFileData.split(';')
	
	for query in allQueries:
		
		query=query.strip()

		if any(char.isalpha() for char in query): #ignore empty queries
			executeSQL(query)

def dotZeroBugFix(dataFrame):
	#call this on pandas dataframes you are trying to load into redshift but dump into stl_load_errors because of a trailing '.0' on integer values
	dataFrame=dataFrame.apply(str)
	dataFrame=dataFrame.str.split('.').str[0]
	return dataFrame.str.replace('nan','')
