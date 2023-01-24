# -------------------------------------------------------------------
# -------------------------------------------------------------------

# DESCRIPTION
"""

This script will use the contents of the log files (txt) to
create a single CSV file of parsed log content.


Instructions:

 1. Start menu -> Run -> Type 'cmd'
 2. Navigate to the folder where this script is located

        python <Script_name.py> <log file vintage>
                                <workspace path including final slash>

Example:
        python geocoderLogTable.py 12-2022
                                   X:\workspace\analytics_LOC_log_table\

Assumptions:

- There is both an 'input' and 'output' folder in the workspace
- The workspace 'config' subfolder contains all CSV config files

"""
# -------------------------------------------------------------------
# -------------------------------------------------------------------

# IMPORT MODULES

from logFunctions_2 import (
    apiKeyParameter,
    assembleList,
    convertTimeStamp,
    csvToDict,
    valueRangeChecker,
    lineChecker,
    parseParameters,
    platformCheck,
    subfolderCheck,
    writeLogHeaderToCsv,
)

import os
import re
import sys
import time

# -------------------------------------------------------------------
# -------------------------------------------------------------------

# FUNCTIONS


# Parse each line of the log file and send substrings to the output file
def searchLog(fldrCont, logFilePath, geoRejected):

    print("Processing: " + logFilePath)

    with open(logFilePath, "r") as file:
        for line in file.readlines():

            validEntry = lineChecker(line, apiResourceDict)

            if validEntry == True:
                try:
                    if line.split(" ")[5].replace('"', "") == "GET":
                        line = line.replace('"', "")
                        newRow = {
                            "ip": line.split(" ")[0],
                            "dateTime": line.split("[")[1].split("]")[0],
                            "method": line.split(" ")[5],
                            "resource": line.split("GET /")[1].split(".")[0],
                            "format": line.split("?")[0].split(".")[-1],
                            "parameters": line.split(" ")[6].split("?")[-1],
                            "protocol": line.split(" ")[7],
                            "statusCode": line.split(" ")[8],
                            "apiKeyVal": line.split(" ")[12],
                        }
                        if "parcels" in newRow["resource"]:
                            newRow["format"] = line.split(" ")[6].split(".")[-1]
                            newRow["resource"] = "parcels/pids"
                        elif "subsites" in newRow["resource"]:
                            newRow["resource"] = "sites/siteID/subsites"
                        if "apikey=" in line:
                            apiKeyParamVal = apiKeyParameter(line)
                        elif (
                            newRow["apiKeyVal"] is not None
                            and "-" not in newRow["apiKeyVal"]
                        ):
                            apiKeyParamVal = newRow["apiKeyVal"]
                        else:
                            apiKeyParamVal = ""
                        logEntry = str(line).replace('"','').strip()
                        if len(logEntry) > 500:
                            logEntry = logEntry[:499]
                        logEntry = '"' + logEntry + '"'
                        isoTime = convertTimeStamp(newRow["dateTime"])
                        # Validate the string value is allowed for 'format'.
                        fCheck = valueRangeChecker(newRow["format"], apiFormatDict)
                        if fCheck == False:
                            newRow["format"] = "not recognized"
                        # Validate the string value is allowed for 'resource'.
                        rCheck = valueRangeChecker(newRow["resource"], apiResourceDict)
                        if rCheck == False:
                            newRow["resource"] = "not recognized"
                        parseParameters(
                            newRow["ip"],
                            apiKeyParamVal.strip(),
                            isoTime,
                            newRow["method"],
                            newRow["protocol"],
                            newRow["statusCode"],
                            newRow["resource"],
                            newRow["format"],
                            newRow["parameters"],
                            logEntry,
                            apiLog,
                            apiParamDict,
                        )
                    elif line.split(" ")[5].replace('"', "") == "HEAD":
                        line = line.replace('"', "")
                        apiKeyParamVal = ""
                        newRow = {
                            "ip": line.split(" ")[0],
                            "dateTime": line.split("[")[1].split("]")[0],
                            "method": line.split(" ")[5],
                            "resource": line.split("HEAD /")[1].split(".")[0],
                            "format": line.split(" ")[6].split(".")[-1],
                            "protocol": line.split(" ")[7],
                            "statusCode": line.split(" ")[8],
                        }
                        logEntry = str(line).replace('"','').strip()
                        if len(logEntry) > 500:
                            logEntry = logEntry[:499]
                        logEntry = '"' + logEntry + '"'
                        isoTime = convertTimeStamp(newRow["dateTime"])
                        assembleList(
                            newRow["ip"],
                            apiKeyParamVal.strip(),
                            isoTime,
                            newRow["method"],
                            newRow["protocol"],
                            newRow["statusCode"],
                            newRow["resource"],
                            newRow["format"],
                            ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,",
                            logEntry,
                            apiLog,
                        )
                    elif line.split(" ")[5].replace('"', "") == "POST":
                        line = line.replace('"', "")
                        apiKeyParamVal = ""
                        newRow = {
                            "ip": line.split(" ")[0],
                            "dateTime": line.split("[")[1].split("]")[0],
                            "method": line.split(" ")[5],
                            "resource": line.split("POST /")[1].split(".")[0],
                            "format": line.split(" ")[6].split(".")[-1],
                            "protocol": line.split(" ")[7],
                            "statusCode": line.split(" ")[8],
                        }
                        logEntry = str(line).replace('"','').strip()
                        if len(logEntry) > 500:
                            logEntry = logEntry[:499]
                        logEntry = '"' + logEntry + '"'
                        isoTime = convertTimeStamp(newRow["dateTime"])
                        assembleList(
                            newRow["ip"],
                            apiKeyParamVal.strip(),
                            isoTime,
                            newRow["method"],
                            newRow["protocol"],
                            newRow["statusCode"],
                            newRow["resource"],
                            newRow["format"],
                            ",,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,",
                            logEntry,
                            apiLog,
                        )
                except:
                    print("Exception raised for line: " + str(line))
                    logRejected.write(logFilePath + "," + line)


# -------------------------------------------------------------------
# -------------------------------------------------------------------

# VARIABLE DEFINITIONS


# The vintage of the log files
logVintage = sys.argv[1]
# The workspace path
wrkspcPath = sys.argv[2]

# At the time of writing this script all LOC logs were in TXT format
allowedFileTypes = ["txt"]

slash = platformCheck()

# Subfolder paths to store the logfiles
logFilesGeocoder = wrkspcPath + "input" + slash + "geocoder"

# Subfolder paths to configuration files
configFileGeocoder = wrkspcPath + "config" + slash + "logToCsv_geocoder.csv"


# -------------------------------------------------------------------
# -------------------------------------------------------------------

# PROCESSING

# Record the time the script began processing
startTime = time.time()

# Check that the correct number of arguments were provided
if len(sys.argv) < 2 or len(sys.argv) > 3:
    print("Missing arguments (2 required). Script will now exit.")
    exit()

# Create dictionaries containing keys from columns in the config file
# Default values will be 0.
apiFormatDict = {}
apiParamDict = {}
apiResourceDict = {}

apiFormatDict = csvToDict(configFileGeocoder, apiFormatDict, 4)
apiParamDict = csvToDict(configFileGeocoder, apiParamDict, 5)
apiResourceDict = csvToDict(configFileGeocoder, apiResourceDict, 3)

# The CSV containing metrics
apiLog = open(
    wrkspcPath + "output" + slash + "geocoder_api_log_" + logVintage + ".csv",
    "w",
)

# The CSV containing rejected rows from log files
logRejected = open(
    wrkspcPath + "output" + slash + "geocoder_api_rejected_" +
    logVintage + ".csv", "w",
)

# Write the header to the rejected log row file
logRejected.write("logFile,failedLine\n")

# Record a list of subfolders in the main log folder for the API
subfolderLst = subfolderCheck(logFilesGeocoder)

# Write the header to the metrics file
writeLogHeaderToCsv(
    apiParamDict,
    apiLog,
)

for fldr in subfolderLst:
    # Filter folder contents to those with allowed formats
    folderContents = [
        f
        for f in os.listdir(logFilesGeocoder + slash + fldr)
        if re.match(r"^.*\.(?:txt)$", f)
    ]
    for file in folderContents:
        # Process each log file sequentially
        logPath = logFilesGeocoder + slash + fldr + slash + file
        searchLog(folderContents, logPath, logRejected)

# Close the output files and clear lists
apiLog.close()
logRejected.close()

processingTime = time.time() - startTime
print(
    "Execution time:",
    time.strftime("%H:%M:%S", time.gmtime(processingTime)) + " hrs"
)
