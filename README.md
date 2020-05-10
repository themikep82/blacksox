Welcome to the Ninja Blacksox Project, a baseball data project intended to unite all major baseball data sources into a single AWS RDS Postgres instance.

All loaders require two files from this project:

credentials.py -- A file containing AWS credentials for Postgres and other services

dbhelper.py -- includes several functions for working with Postgres and AWS

1. Sean Lahman Baseball Database
  Simply download the lahman directory and its contents and run 'python lahmanloader.py' from the command line. It will curl the zipped lahman sql file and load it all into it's own schema.

2. Statcast data collected from Baseball Savant CSV downloads. I use code from the excellent https://github.com/jldbc/pybaseball