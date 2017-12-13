import sys
sys.path.append("..")

import subprocess
from dbhelper import *

def main():
	
	subprocess.call("sed -i 's/\r$//' get_db.sh", shell=True) # fix spurious \r characters caused by some windows text editors
	subprocess.call('bash get_db.sh', shell=True)
	runSQLFromFile('lahmanready.sql')
	
	print('COMPLETE!')
	
if __name__ == "__main__":
	main()
