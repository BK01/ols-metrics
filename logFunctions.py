# -------------------------------------------------------------------
# -------------------------------------------------------------------

# DESCRIPTION
"""

This script contains a collection of functions that are used
by the LOC API log scripts.

Assumptions:

- All LOC API log scripts are stored in the same folder
- There is both an 'input' and 'output' folder in the workspace
- The workspace 'config' subfolder contains all CSV config files

"""
# -------------------------------------------------------------------
# -------------------------------------------------------------------

# IMPORT MODULES

import csv
import os
import platform
import psycopg2
import time

# -------------------------------------------------------------------
# -------------------------------------------------------------------

# FUNCTIONS


# Parse an API key out of a log file line, where
# the 'apikey=' parameter is used. API keys appended to
# the end of a log file line are parsed in the API specific script.
def apiKeyParameter(logLine):

    # The log file line split on & to create a list
    parameterList = logLine.split("&")
    # The substring to identify the apikey parameter
    substringVal = "apikey="
    # The single list item that contains the substring
    apiKey = [i for i in parameterList if substringVal in i]
    apiKeyStr = str(apiKey)
    # The location index in the list item of the substring
    paramIndex = apiKeyStr.index("apikey=") + 7
    apiKeyStrTrim = apiKeyStr[paramIndex:]

    if " " in apiKeyStrTrim:
        blankSpaceIndex = apiKeyStrTrim.index(" ")
        apiKeyStrTrim2 = apiKeyStrTrim[0:blankSpaceIndex]
    else:
        apiKeyStrTrim2 = apiKeyStrTrim

    # Remove special characters (brackets, commas etc)
    alphaNum = list([val for val in apiKeyStrTrim2 if val.isalpha() or val.isnumeric()])
    apiKeyAlphaNum = "".join(alphaNum)

    if apiKeyAlphaNum == "":
        apiKeyAlphaNum = "API key parameter had no value"

    return apiKeyAlphaNum


# Assemble the string values into a comma delimited string to write to CSV
def assembleList(
    ipAddress,
    apiKey,
    isoTime,
    method,
    protocol,
    statusCode,
    resource,
    responseFormat,
    parameters,
    logEntry,
    fileName,
):

    tableEntryString = (
        isoTime
        + ","
        + ipAddress
        + ","
        + apiKey
        + ","
        + method
        + ","
        + protocol
        + ","
        + statusCode
        + ","
        + resource
        + ","
        + responseFormat
        + ","
        + logEntry
        + ","
        + parameters
        + "\n"
    )

    writeToFile(tableEntryString, fileName)


# Convert the date timestamp in the log file to a new format
def convertTimeStamp(timestamp):

    calendar = {
        "Jan": "01",
        "Feb": "02",
        "Mar": "03",
        "Apr": "04",
        "May": "05",
        "Jun": "06",
        "Jul": "07",
        "Aug": "08",
        "Sep": "09",
        "Oct": "10",
        "Nov": "11",
        "Dec": "12",
    }

    day = timestamp[0:2]
    month = timestamp[3:6]
    year = timestamp[7:11]
    time = timestamp[12:20]

    monthNum = calendar[month]

    isoDate = year + "-" + monthNum + "-" + day + "T" + time + "-" + "08:00"

    return isoDate


# Convert specific columns from the config CSV files to dictionaries
def csvToDict(configFile, dictName, colNum):

    with open(configFile, "r") as apiConfig:
        next(apiConfig)  # skip the CSV header
        data = csv.reader(apiConfig)
        dictName = {rows[colNum]: 0 for rows in data}

    return dictName


# Confirm that the provided string is listed in the
# dictionary of allowed values
def valueRangeChecker(strValue, dictValues):

    validFormat = False

    for key in dictValues:
        if key == strValue and key != "":
            validFormat = True
            break
        else:
            validFormat = False

    return validFormat


# Only parse lines from the logs where an API
# resource is listed.
def lineChecker(logLine, resDict):

    validLine = False

    for key in resDict:
        if key in logLine and key != "":
            validLine = True
            break
        else:
            validLine = False

    return validLine


# Assemble a comma delimited string containing a boolean value for
# each parameter used in an API request
def parseParameters(
    ipAddress,
    apiKey,
    isoTime,
    method,
    protocol,
    statusCode,
    resource,
    responseFormat,
    parameters,
    logEntry,
    fileName,
    paramDt,
):

    parameterStrList = ""

    for key in paramDt:
        if key in parameters:
            paramVal = "Y"
            parameterStrList += paramVal + ","
        else:
            paramVal = ""
            parameterStrList += paramVal + ","

    if parameterStrList.endswith(","):
        parameterStrList = parameterStrList[:-1]

    assembleList(
        ipAddress,
        apiKey,
        isoTime,
        method,
        protocol,
        statusCode,
        resource,
        responseFormat,
        parameterStrList,
        logEntry,
        fileName,
    )


# Determine if script is running on Windows or Linux
def platformCheck():

    pltFrm = platform.system()

    # Define which slash to use based on platform
    if pltFrm == "Windows":
        slash = "\\"
    elif pltFrm == "Linux":
        slash = "/"

    return slash


# Create a list of subfolders containing log files to iterate through.
def subfolderCheck(parentFolder):

    subList = [f.name for f in os.scandir(parentFolder) if f.is_dir()]

    return subList


# Write the file header to a result CSV file
def writeLogHeaderToCsv(dParameters, logTableFile):

    headerString = "log_datetime,ip_address,api_key,method,"
    headerString += "protocol,status_code,resource,format,log_entry,"

    for key in dParameters:
        if key != "":
            headerString += str(key) + ","

    csvHeader = headerString.rstrip(",")
    csvHeader += "\n"

    logTableFile.write(csvHeader)


# Write the final CSV entry to file
def writeToFile(outputString, logOutputFile):

    logOutputFile.write(outputString)
