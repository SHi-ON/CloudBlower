# IBM's Cloudant API implementation in Python
# Cloudant database adapter to feed database with a CSV file.
# by Shayan Amani (SHi-ON)
# https://shayanamani.com
# Sep 2018

from cloudant.client import Cloudant
from cloudant.error import CloudantException
from cloudant.result import Result, ResultByKey
import csv
import time


def csv_inflator(db):
    # Show casing use of different fields.
    # Example file is provided in the repository.
    csvFile = open('National_Shelter_System_Facilities.csv', 'r')

    fieldNames = (
        "X", "Y", "OBJECTID", "ID", "NAME", "ADDRESS", "ADDRESS2", "CITY", "STATE", "ZIP", "ZIP4", "TELEPHONE", "TYPE",
        "STATUS", "POPULATION", "COUNTY", "COUNTYFIPS", "COUNTRY", "LATITUDE", "LONGITUDE", "NAICS_CODE", "NAICS_DESC",
        "SOURCE", "SOURCEDATE", "VAL_METHOD", "VAL_DATE", "WEBSITE", "FEMA_ID", "ARC_ID", "EVAC_CAP", "POST_CAP",
        "SURGE", "FLOOD_100", "FLOOD_500", "PET_CODE", "PET_DESC", "ADA", "WHEEL", "ELECTRIC", "PRE", "FEMA_REG")

    reader = csv.DictReader(csvFile, fieldNames)

    c = 0
    for r in reader:
        c += 1
        jsonDoc = {
            "X": r["X"],
            "Y": r["Y"],
            "OBJECTID": r["OBJECTID"],
            "ID": r["ID"],
            "NAME": r["NAME"],
            "ADDRESS": r["ADDRESS"],
            "ADDRESS2": r["ADDRESS2"],
            "CITY": r["CITY"],
            "STATE": r["STATE"],
            "ZIP": r["ZIP"],
            "ZIP4": r["ZIP4"],
            "TELEPHONE": r["TELEPHONE"],
            "TYPE": r["TYPE"],
            "STATUS": r["STATUS"],
            "POPULATION": r["POPULATION"],
            "COUNTY": r["COUNTY"],
            "COUNTYFIPS": r["COUNTYFIPS"],
            "COUNTRY": r["COUNTRY"],
            "LATITUDE": r["LATITUDE"],
            "LONGITUDE": r["LONGITUDE"],
            "NAICS_CODE": r["NAICS_CODE"],
            "NAICS_DESC": r["NAICS_DESC"],
            "SOURCE": r["SOURCE"],
            "SOURCEDATE": r["SOURCEDATE"],
            "VAL_METHOD": r["VAL_METHOD"],
            "VAL_DATE": r["VAL_DATE"],
            "WEBSITE": r["WEBSITE"],
            "FEMA_ID": r["FEMA_ID"],
            "ARC_ID": r["ARC_ID"],
            "EVAC_CAP": r["EVAC_CAP"],
            "POST_CAP": r["POST_CAP"],
            "SURGE": r["SURGE"],
            "FLOOD_100": r["FLOOD_100"],
            "FLOOD_500": r["FLOOD_500"],
            "PET_CODE": r["PET_CODE"],
            "PET_DESC": r["PET_DESC"],
            "ADA": r["ADA"],
            "WHEEL": r["WHEEL"],
            "ELECTRIC": r["ELECTRIC"],
            "PRE": r["PRE"],
            "FEMA_REG": r["FEMA_REG"]
        }

        newDocument = db.create_document(jsonDoc)
        if newDocument.exists():
            msg = "document '{0}' successfully created!"
            print(msg.format(r["ID"]))

        # safety margin to not exceed Cloudant's Lite plan limit (10 write/sec)
        if c == 8:
            c = 0
            time.sleep(2)

    return db


# just for testing and get familiarized with Cloudant functionality
def sample_inflator(db):
    # a sample document of data
    # last field is using for categorizing users (dummy variable) based on 3 levels (0, 1, 2)
    # user_id, latitude, longitude, category
    sampleData = [
        [1, "lat-1", "long-1", 2],
        [2, "lat-2", "long-2", 0],
        [3, "lat-3", "long-3", 1],
        [4, "lat-4", "long-4", 0],
        [5, "lat-5", "long-5", 1],
        [6, "lat-6", "long-6", 2]
    ]

    for doc in sampleData:
        f_id = doc[0]
        f_lat = doc[1]
        f_long = doc[2]
        f_cat = doc[3]

        # create a document (like a record on a SQL database) in JSON format
        jsonDocument = {
            "idField": f_id,
            "latField": f_lat,
            "longField": f_long,
            "catField": f_cat
        }

        newDocument = db.create_document(jsonDocument)

        if newDocument.exists():
            msg = "document '{0}' successfully created!"
            print(msg.format(f_id))


def main():
    # For more info on how to get the credentials:
    # https://console.bluemix.net/docs/services/Cloudant/getting-started.html#getting-started-with-cloudant
    cClient = Cloudant("<USERNAME>",
                       "<PASSWORD>",
                       url="https://<USERNAME>:<PASSWORD>@<USERNAME>.cloudant.com")
    cClient.connect()

    # just an example
    db_name = "finanza"
    database = cClient.create_database(db_name)

    if database.exists():
        msg = "database '{0}' successfully created.\n"
        print(msg.format(db_name))

    csv_inflator(database)
    # sample_inflator(database)

    cClient.disconnect()


if __name__ == "__main__":
    main()
