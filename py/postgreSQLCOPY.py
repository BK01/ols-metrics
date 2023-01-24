# -------------------------------------------------------------------
# -------------------------------------------------------------------

# DESCRIPTION
"""
This script will create a PostgreSQL table (drop if already exists)
and copy the contents of the CSV into the table.


Instructions:

 1. Start menu -> Run -> Type 'cmd'
 2. Navigate to the folder where this script is located

        python <Script_name.py> <db user name> <db password> <CSV filename>
                                <workspace path including final slash>

Example:
        python geocoderLogTable.py username123 pasword123
                                   GEOCODER_API_LOG_12-2022.csv
                                   H:\DataBC\analytics_LOC_log_table\

Assumptions:

- There is an 'output' folder in the workspace that contains the CSV
"""

# -------------------------------------------------------------------
# -------------------------------------------------------------------

# IMPORT MODULES

import sys
import psycopg2
import time

# -------------------------------------------------------------------
# -------------------------------------------------------------------

# FUNCTIONS


# Create a table in PostgreSQL and import CSV content
def create_tables(dbUri, sqlCmd, input_file):

    conn = None
    try:
        print("Reading CSV file")
        columns = input_file.readline().strip().split(",")

        # connect to the PostgreSQL server
        print("Connecting to the PostgreSQL database")
        conn = psycopg2.connect(dbUri)
        cur = conn.cursor()
        print("Executing SQL queries")
        # execute queries
        for query in sqlCmd:
            cur.execute(query)
            # print(query)

        print("PostgreSQL table created")

        # close communication with the PostgreSQL database
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


# -------------------------------------------------------------------
# -------------------------------------------------------------------

# VARIABLE DEFINITIONS

# The PostgreSQL user name and password
dbUser = sys.argv[1]
dbPswd = sys.argv[2]

# PostgreSQL database connection values - OpenShift
dbHost = sys.argv[3]
dbName = sys.argv[4]
dbPort = sys.argv[5]

# The CSV name to load into PostgreSQL
filename = sys.argv[6]
# The workspace path including the final slash
wrkspcPath = sys.argv[7]

# The full path to the CSV to load into PostgreSQL
apiLogTable = wrkspcPath + "output" + "\\" + filename
apiLogTableSQL = "'" + apiLogTable + "'"

# -------------------------------------------------------------------
# -------------------------------------------------------------------

# PROCESSING

# Record the time the script began processing
startTime = time.time()

# Check that the correct number of arguments were provided
if len(sys.argv) < 7 or len(sys.argv) > 8:
    print("Missing arguments (7 required). Script will now exit.")
    exit()

# PostgreSQL database connection string
dbUri = f"postgres://{dbUser}:{dbPswd}@{dbHost}:{dbPort}/{dbName}"

sqlQueries = (
    """
    TRUNCATE TABLE geocoder_api_log_with_parameters;
    """,
    """
    COPY geocoder_api_log_with_parameters (log_datetime,ip_address,api_key,method,protocol,status_code,resource,format,log_entry,addressString,autoComplete,bbox,brief,centre,civicNumber,civicNumberSuffix,echo,excludeUnits,extrapolate,interpolation,localities,localityName,locationDescriptor,matchPrecision,matchPrecisionNot,maxDistance,maxResults,maxDegree,minDegree,minScore,notLocalities,onlyCivic,outputSRS,parcelPoint,point,provinceCode,setBack,siteName,streetDirection,streetName,streetQualifier,streetType,tags,unitDesignator,unitNumber,unitNumberSuffix)
    FROM {}
    DELIMITER ','
    CSV HEADER
    """.format(apiLogTableSQL),
)

input_file = open(apiLogTable, "r")

create_tables(dbUri, sqlQueries, input_file)

input_file.close()

processingTime = time.time() - startTime
print(
    "Execution time:",
    time.strftime("%H:%M:%S", time.gmtime(processingTime)) + " hrs"
)
