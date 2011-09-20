import NCARFlightMonitor.database
import sys

## def _loadFile(file_path, dbname, host, user, password, dbstart):
## file is in the format of one of the examples (has a large header).
## dbstart is a database name that already exists at the host location

NcarChem.database._loadFile(sys.argv[1], sys.argv[2], sys.argv[3], "postgres", "", "postgres")
