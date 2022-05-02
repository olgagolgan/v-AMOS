#Additional Classes
from sqlite3 import connect
from pandas import read_csv
from rdflib import Graph, URIRef, RDF, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from json import load
import pandas as pd
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
            
class RelationalDataProcessor(RelationalProcessor):
    def __init__(self, dbPath):
      super().__init__(dbPath)

    def uploadData(self, path):
        if path != '':
            with open(path, "r", encoding="utf-8") as f:
                jsonData = load(f)

            csvData = pd.read_csv(path)

            # ========== AUTHOR =======================
            authors = jsonData['authors']
            datum = []
            for doi in authors:
                data_row = authors[doi]
                for row in data_row:
                    datum.append(row)
            authorDf = pd.DataFrame(datum)
            
            # ========== PUBLISHERS ===================
            publishers = jsonData['publishers']
            datum = []
            for cross_ref in publishers:
                datum.append([publishers[cross_ref]['id'], publishers[cross_ref]['name']])
            publisherDf = pd.DataFrame(datum, columns=['id','name'])

            # ========= JOURNAL ARTICLE ==============
            journal_articles = csvData.query("type == 'journal-article'")
            journal_articles = journal_articles[["id","title","publication_year","issue","volume"]]

            # Add venuesId
            venues = jsonData['venues_id']
            datum = []
            for doi in venues:
                datum.append([doi, str(venues[doi])])
            venuePub = pd.DataFrame(datum, columns=["doi","issn/isbn"])
            journal_articles = journal_articles.merge(venuePub, how='left', left_on="id", right_on="doi")
            journal_articles = journal_articles[["id","title","publication_year","issue","volume", "issn/isbn"]] 

            #Add authors
            authors = jsonData['authors']
            datum = []
            for doi in authors:
                setOrcid = set()
                for item in authors[doi]:
                    setOrcid.add(item['orcid'])
                datum.append([doi, str(setOrcid)])   
            autOfPub = pd.DataFrame(datum, columns=["doi","orcid"])
            journal_articles = journal_articles.merge(autOfPub, how='left', left_on="id", right_on="doi") 
            journal_articles = journal_articles[["id","title","publication_year","issue","volume", "issn/isbn", "orcid"]] 

            #Add citation
            references = jsonData['references']
            datum = []
            for doi in references:
                datum.append([doi,str(references[doi])])
            citeDf = pd.DataFrame(datum, columns=["doi","cite"])
            journal_articles = journal_articles.merge(citeDf, how='left', left_on="id", right_on="doi") 
            journal_articles = journal_articles[["id","title","publication_year", "issue","volume", "issn/isbn", "orcid", "cite"]] 


            # ========= BOOK CHAPTER ============== 
            book_chapter = csvData.query("type == 'book-chapter'")
            book_chapter = book_chapter[["id","title","publication_year", "chapter"]]

            # Add venuesId
            venues = jsonData['venues_id']
            datum = []
            for doi in venues:
                datum.append([doi, str(venues[doi])])
            venuePub = pd.DataFrame(datum, columns=["doi","issn/isbn"])
            book_chapter = book_chapter.merge(venuePub, how='left', left_on="id", right_on="doi") 
            book_chapter = book_chapter[["id","title","publication_year", "chapter", "issn/isbn"]]

            #Add authors
            authors = jsonData['authors']
            datum = []
            for doi in authors:
                setOrcid = set()
                for item in authors[doi]:
                    setOrcid.add(item['orcid'])
                datum.append([doi, str(setOrcid)])    
            autOfPub = pd.DataFrame(datum, columns=["doi","orcid"])
            book_chapter = book_chapter.merge(autOfPub, how='left', left_on="id", right_on="doi") 
            book_chapter = book_chapter[["id","title","publication_year", "chapter", "issn/isbn", "orcid"]]

            #Add citation
            references = jsonData['references']
            datum = []
            for doi in references:
                datum.append([doi,str(references[doi])])
            citeDf = pd.DataFrame(datum, columns=["doi","cite"])
            book_chapter = book_chapter.merge(citeDf, how='left', left_on="id", right_on="doi") 
            book_chapter = book_chapter[["id","title","publication_year", "chapter", "issn/isbn", "orcid", "cite"]]

            # ========= PROCEEDINGS-PAPER ============== 
            proceedings_paper = csvData.query("type == 'proceedings-paper'")
            proceedings_paper = proceedings_paper[["id","title","publication_year"]]

            # Add venuesId
            venues = jsonData['venues_id']
            datum = []
            for doi in venues:
                datum.append([doi, str(venues[doi])])
            venuePub = pd.DataFrame(datum, columns=["doi","issn/isbn"])
            proceedings_paper = proceedings_paper.merge(venuePub, how='left', left_on="id", right_on="doi") 
            proceedings_paper = proceedings_paper[["id","title","publication_year", "issn/isbn"]]

            #Add authors
            authors = jsonData['authors']
            datum = []
            for doi in authors:
                setOrcid = set()
                for item in authors[doi]:
                    setOrcid.add(item['orcid'])
                datum.append([doi, str(setOrcid)])      
            autOfPub = pd.DataFrame(datum, columns=["doi","orcid"])
            proceedings_paper = proceedings_paper.merge(autOfPub, how='left', left_on="id", right_on="doi") 
            proceedings_paper = proceedings_paper[["id","title","publication_year", "issn/isbn","orcid"]] #ALL THE QUERY: id included 

            #Add citation
            references = jsonData['references']
            datum = []
            for doi in references:
                datum.append([doi,str(references[doi])])
            citeDf = pd.DataFrame(datum, columns=["doi","cite"])
            proceedings_paper = proceedings_paper.merge(citeDf, how='left', left_on="id", right_on="doi") 
            proceedings_paper = proceedings_paper[["id","title","publication_year", "issn/isbn", "orcid", "cite"]]

            # ============= JOURNAL ===========
            journal = csvData.query("venue_type == 'journal'")
            journal = journal[["id","publication_venue","publisher"]]
            journal = journal.merge(venuePub, how='left', left_on="id", right_on="doi") # Add venuesId
            journal = journal[["publication_venue","issn/isbn","publisher"]]

            # ============= BOOK ===========
            book = csvData.query("venue_type == 'book'")
            book = book[["id","publication_venue","publisher"]]
            book = book.merge(venuePub, how='left', left_on="id", right_on="doi") # Add venuesId
            book = book[["publication_venue","issn/isbn","publisher"]]

            # ============= PROCEEDINGS ===========
            proceedings = csvData.query("venue_type == 'proceedings'")
            proceedings = proceedings[["id","publication_venue","publisher","event"]]
            proceedings = proceedings.merge(venuePub, how='left', left_on="id", right_on="doi") # Add venuesId
            proceedings = proceedings[["publication_venue","issn/isbn","publisher","event"]]

            # ========== REFERENCES ===================
            references = jsonData['references']
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

            with connect(self.dbPath) as con: 
                authorDf.to_sql("Author", con, if_exists="replace", index=False)
                publisherDf.to_sql("Publisher", con, if_exists="replace", index=False)
                journal_articles.to_sql("JournalArticles", con, if_exists="replace", index=False)
                book_chapter.to_sql("BookChapter", con, if_exists="replace", index=False)
                proceedings_paper.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)
                journal.to_sql("Journal", con, if_exists="replace", index=False)
                book.to_sql("Book", con, if_exists="replace", index=False)
                proceedings.to_sql("Proceedings", con, if_exists="replace", index=False)
                refDf.to_sql("Citations", con, if_exists="replace", index=False)
                con.commit()
                return True
        else:
            return False

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