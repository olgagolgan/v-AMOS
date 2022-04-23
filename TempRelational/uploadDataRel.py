#Additional Classes
from AdditionalClasses import RelationalProcessor
from json import load
import pandas as pd
from sqlite3 import connect
from pandas import read_csv, read_json, Series
from rdflib import Graph, URIRef, RDF, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pathlib import Path

class RelationalDataProcessor(RelationalProcessor):
    def __init__(self, dbPath):
      super().__init__(dbPath)

    def uploadData(self, path):
        if path != '':
            if path.endswith(".csv"):
                with connect(self.dbPath) as con:
                    CSVDf = read_csv(path, keep_default_na=False)
                    CSVDf.to_sql("General", con, if_exists="replace", index=False)
                    con.commit()
                    return True
            elif path.endswith(".json"):
                with connect(self.dbPath) as con:
                    with open(path, "r", encoding="utf-8") as f:
                        json_doc = load(f)

                    # ========== AUTHOR and PUBLICATION =======
                    authors = json_doc['authors']
                    rows_author = []
                    rows_first = []
                    for doi in authors:
                        data_row = authors[doi]
                        for row in data_row:
                            rows_author.append(row)
                        for id in range(len(authors[doi])):
                            row = [doi, id+1]
                            rows_first.append(row)
                    df1 = pd.DataFrame(rows_author)
                    df2 = pd.DataFrame(rows_first); df2.columns = ["doi", "coauthor no."]
                    author_pubDf = df2.join(df1)
                    author_pubDf.to_sql("Author", con, if_exists="replace", index=False)

                    # ========== VENUES =======================  
                    venues = json_doc['venues_id']
                    rows_ven = []
                    rows_first = []
                    for doi in venues:
                        data_row = venues[doi]
                        for idx, item_row in enumerate(data_row): 
                            rows_ven.append(item_row)
                            idno = idx + 1
                            row = [doi, idno]
                            rows_first.append(row)
                    df1 = pd.DataFrame(rows_ven); df1.columns = ["id"]
                    df2 = pd.DataFrame(rows_first); df2.columns = ["doi", "id no."]
                    venueDf = df2.join(df1)
                    venueDf.to_sql("Venue", con, if_exists="replace", index=False)

                    # ========== REFERENCES ===================
                    references = json_doc['references']
                    rows_ref = []
                    rows_first = []
                    for doi in references:
                        data_row = references[doi]
                        for row in data_row:
                            rows_ref.append(row)
                        for id in range(len(references[doi])):
                            row = [doi, id]
                            rows_first.append(row)
                    df1 = pd.DataFrame(rows_ref); df1.columns = ["doi mention"]
                    df2 = pd.DataFrame(rows_first); df2.columns = ["doi", "reference no."]
                    refDf = df2.join(df1)
                    refDf.to_sql("References", con, if_exists="replace", index=False)
                    
                    # ========== PUBLISHERS ===================
                    publishers = json_doc['publishers']
                    rowsID = []
                    rowsName = []
                    for cross_ref in publishers:
                        data_row = publishers[cross_ref]
                        rowsID.append(data_row["id"])
                        rowsName.append(data_row["name"])
                    data_tuples = list(zip(rowsID,rowsName))
                    publisherDf = pd.DataFrame(data_tuples, columns=['id','name'])
                    publisherDf.to_sql("Publisher", con, if_exists="replace", index=False)
                    con.commit()
                    return True
            else:
                return False
        else:
            return False