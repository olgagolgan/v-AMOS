#Additional Classes
from json import load
import pandas as pd
from pandas import read_csv, read_json, Series
from sqlite3 import connect
from rdflib import Graph, URIRef, RDF, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from pathlib import Path

uri_list = []

class RelationalProcessor:
    def __init__(self, dbPath):
        self.dbPath = dbPath

    def getDbPath(self):  # not really sure about it
        return self.dbPath

    def setDbPath(self, path):
        if path != '': 
            self.dbPath = path
            return True  # I tried to handle the possibility of an error
        else:
            return False
            
"""
class RelationalDataProcessor(RelationalProcessor):
    def __init__(self, dbPath):
      super().__init__(dbPath)

    def uploadData(self, path, nameTable):
        if path != '':
            if path.endswith(".csv"):
                with connect(self.dbPath) as con:
                    myTable = read_csv(path, keep_default_na=False)
                    myTable.to_sql(nameTable, con, if_exists="replace", index=False)
                    con.commit()
                    return True
            elif path.endswith(".json"):
                with connect(self.dbPath) as con:
                    otherTable = read_json(path)
                    otherTable.to_sql(nameTable, con, if_exists="replace", index=False)
                    con.commit()
                    return True
            else:
                return False
        else:
            return False         
"""
"""
PER FARLO ATTIVARE!!!           
abc = RelationalProcessor.setDbPath(RelationalProcessor, "publication2.db")
temp = RelationalDataProcessor.uploadData(RelationalDataProcessor, "JSONParser/relational_publications.csv", "GeneralDraft")
""" 
class TriplestoreProcessor:
    def __init__(self, endpointUri):
        self.endpointUri = endpointUri

    def getEndPoint(self):
        return self.endpointUri

    def setEndpointUri(self, url):
        if url != '':
            self.endpointUri = url
            return True
        else:
            return False


class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self, endpointUri):
        super().__init__(endpointUri+"/sparql")

    def uploadData(self, path):
        my_graph = Graph()
        store = SPARQLUpdateStore()
        base_url = "https://github.com/olgagolgan/v-AMOS"
        JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
        BookChapter = URIRef("https://schema.org/Chapter")
        Journal = URIRef("https://schema.org/Periodical")
        Book = URIRef("https://schema.org/Book")

        # attributes related to classes
        identifier = URIRef("https://schema.org/identifier")
        publicationYear = URIRef("https://schema.org/datePublished")
        title = URIRef("https://schema.org/name")
        issue = URIRef("https://schema.org/issueNumber")
        volume = URIRef("https://schema.org/volumeNumber")
        name = URIRef("https://schema.org/name")
        event = URIRef("https://schema.org/Event")
        chapterNumber = URIRef("https://schema.org/Chapter")
        givenName = URIRef("https://schema.org/givenName")
        familyName = URIRef("https://schema.org/familyName")
        # relations among classes
        publicationVenue = URIRef("https://schema.org/isPartOf")
        cites = URIRef("https://schema.org/citation")
        publisher = URIRef("https://schema.org/publishedBy")
        if path != "":
            if path.endswith(".csv"):
                table = read_csv(path, keep_default_na=False)
                for idx, row in table.iterrows():
                    local_id = str(Path(path).stem) + '-' + str(idx)
                    subj = URIRef(base_url + local_id)
                    if row["venue_type"] == "journal":
                        my_graph.add((subj, RDF.type, Journal))
                    elif row["venue_type"] == "book":
                        my_graph.add((subj, RDF.type, Book))
                    if row["type"] == "journal-article":
                        my_graph.add((subj, RDF.type, JournalArticle))
                    elif row["type"] == "book-chapter":
                        my_graph.add((subj, RDF.type, BookChapter))
                    my_graph.add((subj, identifier, Literal(row["id"])))
                    my_graph.add((subj, title, Literal(row["title"])))
                    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    my_graph.add((subj, issue, Literal(row["issue"])))
                    my_graph.add((subj, volume, Literal(row["volume"])))
                    my_graph.add((subj, chapterNumber, Literal(row["chapter"])))
                    my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))
                    my_graph.add((subj, publisher, Literal(row["publisher"])))
                    my_graph.add((subj, event, Literal(row["event"])))
                store.open((self.endpointUri, self.endpointUri))
                for triple in my_graph.triples((None, None, None)):
                    store.add(triple)
                store.close()
                return True
            # elif path.endswith(".json"):
            #
        else:
            return False


#
#
# RP = RelationalProcessor("publication.db")
# print(RP.getDbPath())
#
# RDB = RelationalDataProcessor("publication.db")
#
# result = RDB.uploadData("relational_publications.csv", "Publications")
# print(result)
#
# result2 = RDB.uploadData("relational_other_data", "other")
# print(result2)
#
# graph1 = TriplestoreProcessor("http://192.168.1.18:9999/blazegraph/")
# print(graph1.getEndPoint())

# graph2 = TriplestoreDataProcessor("http://127.0.0.1:8888/blazegraph/")
# print(graph2.uploadData("relational_publications.csv"))