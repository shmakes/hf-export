import couchdb
from datetime import datetime
import json

class CouchDB_ExportByFlightAndType():
    
  def __init__(self, host, dataBase, flightDateStart, flightDateEnd):
    self.host = host
    couch = couchdb.Server(host)
    self.db = couch[dataBase]
    self.flightDateStart = flightDateStart
    self.flightDateEnd = flightDateEnd

  def clean_data(self, doc):
    del doc["accommodations"]
    #del doc["alt_contact"]
    del doc["emerg_contact"]
    #del doc["mail_call"]
    del doc["weight"]
    out = json.dumps(doc).replace('"_id":', '"id":')
    return out.replace("\n", "") + "\n"

  def export_flight(self, flightDate, flightName):
    data = {}
    vetData = open("Flight-" + flightDate + "-" + flightName + "-Veterans.jsonl", "w")
    data["Veteran"] = vetData
    grdData = open("Flight-" + flightDate + "-" + flightName + "-Guardians.jsonl", "w")
    data["Guardian"] = grdData
    for row in self.db.view('basic/all_by_flight_and_name', startkey=[flightName], endkey=[flightName, {}], include_docs=True):
      data[row["value"]["type"]].write(self.clean_data(row.doc))
    vetData.close()
    grdData.close()

  def export_waitlist(self):
    data = {}
    vetData = open("Waitlist-Veterans.jsonl", "w")
    data["Veteran"] = vetData
    grdData = open("Waitlist-Guardians.jsonl", "w")
    data["Guardian"] = grdData
    for row in self.db.view('basic/all_by_flight_and_name', startkey=["None"], endkey=["None", {}], include_docs=True):
      if (row["value"]["status"] == "Active" or row["value"]["status"].startswith("Future-")):
        out = json.dumps(row.doc).replace('"_id":', '"id":')
        data[row["value"]["type"]].write(out.replace("\n", "") + "\n")
      else:
        print(row["value"]["status"])
    vetData.close()
    grdData.close()

  def export_data(self):
    if (self.flightDateStart == "Waitlist" and self.flightDateEnd == "None"):
      print(self.flightDateStart)
      self.export_waitlist()
    else:
      for rowFlt in self.db.view('basic/flights', startkey=[self.flightDateStart], endkey=[self.flightDateEnd, {}], include_docs=False):
        flightDate = rowFlt.key[0]
        flightName = rowFlt.key[1]
        print(flightName)
        self.export_flight(flightDate, flightName)

def main():
  from sys import argv, exit
  if len(argv) != 3 and len(argv) != 5:
    print("Usage: %s host database flightDateStart flightDateEnd" % argv[0])
    print("Example: %s http://localhost:5984 hf 2023-10-07, 2023-12-31" % argv[0])
    exit(1)
  if len(argv) == 5:
    importer = CouchDB_ExportByFlightAndType(argv[1], argv[2], argv[3], argv[4])
  else:
    importer = CouchDB_ExportByFlightAndType(argv[1], argv[2], "Waitlist", "None")
  importer.export_data()

if __name__ == "__main__": main()