Welcome to the Ninja Blacksox Project, a baseball data project intended to unite all major baseball data sources into a single AWS Redshift cluster.

All loaders require two files from this project:

credentials.py -- A file containing AWS credentials for Redshift and other services

dbhelper.py -- includes several functions for working with Redshift and AWS

1. Sean Lahman Baseball Database
  Simply download the lahman directory and its contents and run 'python lahmanloader.py' from the command line. It will curl the zipped lahman sql file and load it all into it's own schema.

2. Statcast data can be collected here: https://baseballsavant.mlb.com/statcast_search
	 Group by Player Name & Event, click search, then click the download CSV link
   Do not filter any columns
   Set DATA_FILE_NAME to the path and filename of your downloaded file.
   
   Still need to clean up some NUMERIC precision issues.
