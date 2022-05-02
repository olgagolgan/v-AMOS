#Additional Classes
from sqlite3 import connect
from pandas import read_csv
from rdflib import Graph, URIRef, RDF, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import json
from pathlib import Path

base_url = "https://github.com/olgagolgan/v-AMOS"
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")

# attributes related to classes
doi = URIRef("https://schema.org/productID")
identifier = URIRef("https://schema.org/identifier")
publicationYear = URIRef("https://schema.org/datePublished")
title = URIRef("https://schema.org/title")
issue = URIRef("https://schema.org/issueNumber")
volume = URIRef("https://schema.org/volumeNumber")
event = URIRef("https://schema.org/Event")
chapterNumber = URIRef("https://schema.org/Chapter")
givenName = URIRef("https://schema.org/givenName")
familyName = URIRef("https://schema.org/familyName")
orcid = URIRef("https://schema.org/creator")
name = URIRef("https://schema.org/name")
author = URIRef("https://schema.org/author")
DOI = URIRef("https://schema.org/productID")
# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")
cites = URIRef("https://schema.org/citation")
publisher = URIRef("https://schema.org/publishedBy")

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
            

# class RelationalDataProcessor(RelationalProcessor):
#     def __init__(self, dbPath):
#       super().__init__(dbPath)
#
#     def uploadData(self, path, nameTable):
#         if path != '':
#             if path.endswith(".csv"):
#                 with connect(self.dbPath) as con:
#                     myTable = read_csv(path, keep_default_na=False)
#                     myTable.to_sql(nameTable, con, if_exists="replace", index=False)
#                     con.commit()
#                     return True
#             elif path.endswith(".json"):
#                 with connect(self.dbPath) as con:
#                     # otherTable = read_json(path)
#                     otherTable.to_sql(nameTable, con, if_exists="replace", index=False)
#                     con.commit()
#                     return True
#             else:
#                 return False
#         else:
#             return False


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
        if path != "":
            my_graph = Graph()
            store = SPARQLUpdateStore()
            store.open((self.endpointUri, self.endpointUri))
            if path.endswith(".csv"):
                table = read_csv(path, keep_default_na=False)
                for idx, row in table.iterrows():
                    local_id = row["id"]
                    subj = URIRef(base_url + "/" + local_id)
                    if row["venue_type"] == "journal":
                        my_graph.add((subj, RDF.type, Journal))
                    elif row["venue_type"] == "book":
                        my_graph.add((subj, RDF.type, Book))
                    if row["type"] == "journal-article":
                        my_graph.add((subj, RDF.type, JournalArticle))
                    elif row["type"] == "book-chapter":
                        my_graph.add((subj, RDF.type, BookChapter))
                    my_graph.add((subj, DOI, Literal(row["id"])))
                    my_graph.add((subj, title, Literal(row["title"])))
                    my_graph.add((subj, publicationYear, Literal(row["publication_year"])))
                    my_graph.add((subj, issue, Literal(row["issue"])))
                    my_graph.add((subj, volume, Literal(row["volume"])))
                    my_graph.add((subj, chapterNumber, Literal(row["chapter"])))
                    my_graph.add((subj, publicationVenue, Literal(row["publication_venue"])))
                    my_graph.add((subj, publisher, Literal(row["publisher"])))
                    my_graph.add((subj, event, Literal(row["event"])))
            elif path.endswith(".json"):
                jsonfile = open(path, mode="r", encoding="utf-8")
                table = json.load(jsonfile)
                for doi in table["authors"]:
                    for author in table["authors"][doi]:
                        subj = URIRef(base_url + "/" + author["orcid"])
                        my_graph.add((subj, familyName, Literal(author['family'])))
                        my_graph.add((subj, givenName, Literal(author['given'])))
                        my_graph.add((subj, orcid, Literal(author['orcid'])))
                        subj = URIRef(base_url + "/" + doi)
                        my_graph.add((subj, orcid, Literal(author["orcid"])))
                        # my_graph.add((subj, familyName, Literal(author["family"])))
                        # my_graph.add((subj, givenName, Literal(author["given"])))
                for venues_id in table["venues_id"]:
                    for idi in table["venues_id"][venues_id]:
                        subj = URIRef(base_url + "/" + venues_id)
                        my_graph.add((subj, identifier, Literal(idi)))
                for reference in table["references"]:
                    for ref in table["references"][reference]:
                        subj = URIRef(base_url + "/" + reference)
                        my_graph.add((subj, cites, Literal(ref)))
                for publish in table["publishers"]:
                    subj = URIRef(base_url + "/" + table["publishers"][publish]["id"])
                    my_graph.add((subj, identifier, Literal(table["publishers"][publish]["id"])))
                    my_graph.add((subj, name, Literal(table["publishers"][publish]["name"])))
            for triple in my_graph.triples((None, None, None)):
                store.add(triple)
            store.close()
            return True
        else:
            return False









# java -server -Xmx1g -jar blazegraph.jar
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
# graph1 = TriplestoreProcessor("http://127.0.0.1:9999/blazegraph")
# print(graph1.getEndPoint())

# graph2 = TriplestoreDataProcessor("http://127.0.0.1:9999/blazegraph")
# print(graph2.uploadData("relational_publications.csv"))
# print(graph2.uploadData("relational_other_data.json"))