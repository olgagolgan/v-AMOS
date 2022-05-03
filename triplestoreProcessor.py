from sparql_dataframe import get
from pandas import read_csv
from rdflib import Graph, URIRef, RDF, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import json

# uri's for Triplestore DB

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
venueID = URIRef("https://schema.org/VirtualLocation")
# relations among classes
publicationVenue = URIRef("https://schema.org/isPartOf")
cites = URIRef("https://schema.org/citation")
publisher = URIRef("https://schema.org/publishedBy")


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
                for venues_id in table["venues_id"]:
                    for idi in table["venues_id"][venues_id]:
                        subj = URIRef(base_url + "/" + venues_id)
                        my_graph.add((subj, venueID, Literal(idi)))
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


class TriplestoreQueryProcessor(TriplestoreProcessor):
    def __init__(self, endpointUri):
        super().__init__(endpointUri + "/sparql")

    def getPublicationsPublishedInYear(self, year):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?orcid ?name ?surname ?title ?doi ?venue ?publisher ?date
            WHERE {
            {SELECT ?orcid ?title ?doi ?venue ?publisher ?date
            WHERE{
              ?s schema:creator ?orcid.
              ?s schema:title ?title.
              ?s schema:productID ?doi.
              ?s schema:publishedBy ?publisher.
              ?s schema:isPartOf ?venue.
              ?s schema:datePublished ?date.
              ?s schema:datePublished """ + str(year) + """ 
            }              
            }
             ?x schema:creator ?orcid.
             ?x schema:givenName ?name.
             ?x schema:familyName ?surname
        }"""

        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getPublicationsByAuthorId(self, id):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?orcid ?name ?surname ?title ?doi ?venue ?publisher ?date
            WHERE {
            {SELECT ?orcid ?name ?surname
            WHERE{
             ?x schema:creator ?orcid.
             ?x schema:givenName ?name.
             ?x schema:familyName ?surname.
             ?x schema:creator '""" + str(id) + """'
            }                          
            }
          ?s schema:creator ?orcid.
          ?s schema:title ?title.
          ?s schema:productID ?doi.
          ?s schema:publishedBy ?publisher.
          ?s schema:isPartOf ?venue.
          ?s schema:datePublished ?date.                       
            }"""
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getMostCitedPublication(self):
        qry = """
                PREFIX schema: <https://schema.org/>
                SELECT ?orcid ?name ?surname ?title ?doi ?venue ?publisher ?date (COUNT(?reference) as ?mostCited)
                WHERE {
                {
                SELECT ?orcid ?name ?surname
                    WHERE{
                         ?x schema:creator ?orcid.
                         ?x schema:givenName ?name.
                         ?x schema:familyName ?surname
                    }
                 }           
                  ?s schema:creator ?orcid.
                  ?s schema:title ?title.
                  ?s schema:productID ?doi.
                  ?s schema:publishedBy ?publisher.
                  ?s schema:isPartOf ?venue.
                  ?s schema:datePublished ?date.
                  ?s schema:citation ?reference
                }
                GROUP BY ?orcid ?name ?surname ?title ?doi ?venue ?publisher ?date
                ORDER BY DESC(?mostCited)
            """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getMostCitedVenue(self):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?venues ?venueid ?publisher ?title (COUNT(?reference) as ?mostCited)
            WHERE {
              ?s schema:isPartOf ?venues.
              ?s schema:title ?title.
              ?s schema:publishedBy ?publisher.
              ?s schema:VirtualLocation ?venueid.
              ?s schema:citation ?reference
            }
            GROUP BY ?venues ?venueid ?title ?publisher
            ORDER BY DESC(?mostCited)
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getVenuesByPublisherId(self, id):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?title ?venueId ?publisher ?publisherName ?publisherId
            WHERE{
           {SELECT ?publisherName ?publisherId
            WHERE {
                ?x schema:name ?publisherName.
                ?x schema:identifier ?publisherId.
                ?x schema:identifier '""" + str(id) + """'
            }
           }
           ?s schema:title ?title.
           ?s schema:VirtualLocation ?venueId.
           ?s schema:publishedBy ?publisher
            }
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getPublicationInVenue(self, venueId):
        qry = """ 
                PREFIX schema: <https://schema.org/>
                SELECT ?orcid ?name ?surname ?title ?doi ?venue ?date
                WHERE {
                {SELECT ?orcid ?name ?surname
                  WHERE{
               ?x schema:creator ?orcid.
               ?x schema:givenName ?name.
               ?x schema:familyName ?surname.
                }                          
                }
              ?s schema:creator ?orcid.
              ?s schema:title ?title.
              ?s schema:productID ?doi.
              ?s schema:datePublished ?date.
              ?s schema:isPartOf ?venue.
              ?s schema:VirtualLocation ?venueId.
              ?s schema:VirtualLocation '""" + str(venueId) + """'
                        }"""

        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?orcid ?name ?surname ?title ?venue ?date
            WHERE {
            {SELECT ?orcid ?name ?surname
              WHERE{
               ?x schema:creator ?orcid.
               ?x schema:givenName ?name.
               ?x schema:familyName ?surname.
            }                          
            }
          ?s schema:creator ?orcid.
          ?s schema:title ?title.
          ?s schema:datePublished ?date.
          ?s schema:isPartOf ?venue.
          ?s schema:issueNumber ?issue.
          ?s schema:issueNumber '""" + str(issue) + """'.
          ?s schema:volumeNumber ?volume.
          ?s schema:volumeNumber '""" + str(volume) + """'.
          ?s schema:productID ?journalId.
          ?s schema:productID '""" + str(journalId) + """'
          }
         """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getJournalArticlesInVolume(self, volume, journalId):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?orcid ?name ?surname ?title ?venue ?date
            WHERE {
            {SELECT ?orcid ?name ?surname
              WHERE{
               ?x schema:creator ?orcid.
               ?x schema:givenName ?name.
               ?x schema:familyName ?surname.
            }                          
            }
          ?s schema:creator ?orcid.
          ?s schema:title ?title.
          ?s schema:datePublished ?date.
          ?s schema:isPartOf ?venue.
          ?s schema:volumeNumber ?volume.
          ?s schema:volumeNumber '""" + str(volume) + """'.
          ?s schema:productID ?journalId.
          ?s schema:productID '""" + str(journalId) + """'
          }    
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getJournalArticlesInJournal(self, journalId):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?orcid ?name ?surname ?title ?venue ?date
            WHERE {
            {SELECT ?orcid ?name ?surname
              WHERE{
               ?x schema:creator ?orcid.
               ?x schema:givenName ?name.
               ?x schema:familyName ?surname.
            }                          
            }
          ?s schema:creator ?orcid.
          ?s schema:title ?title.
          ?s schema:datePublished ?date.
          ?s schema:isPartOf ?venue.
          ?s schema:productID ?journalId.
          ?s schema:productID '""" + str(journalId) + """'
          }       
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getProceedingsByEvent(self, eventPartialName):
        qry = """
         PREFIX schema:<https://schema.org/>

          SELECT ?title ?venueId ?publisher
          WHERE {
              ?s schema:title ?title.
              ?s schema:VirtualLocation ?venueId.
              ?s schema:publishedBy ?publisher.
              ?s schema:Event ?event.
              FILTER (REGEX(?event,'""" + str(eventPartialName) + """', "i"))
          }
         """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getPublicationAuthors(self, publicationId):
        qry = """
        PREFIX schema: <https://schema.org/>
        SELECT ?orcid ?name ?surname
        WHERE
        { 
            ?x schema:creator ?orcid.
            ?x schema:givenName ?name.
            ?x schema:familyName ?surname.
            {
                SELECT ?orcid
                WHERE {
                    ?y schema:productID '""" + str(publicationId) + """'.
                    ?y schema:creator ?orcid}
            }
        }
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getPublicationsByAuthorName(self, authorPartialName):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?doi ?title ?date ?publicationVenue ?name ?surname
            WHERE 
            {
                ?x schema:creator ?orcid.
                ?x schema:productID ?doi.
                ?x schema:title ?title.
                ?x schema:datePublished ?date.
                ?x schema:isPartOf ?publicationVenue.
                
            {
                SELECT ?orcid ?name ?surname
                WHERE
                    {
                        ?s schema:creator ?orcid.
                        ?s schema:givenName ?name.
                        ?s schema:familyName ?surname.
                        BIND (CONCAT (?surname, " ", ?name) as ?author).
                        FILTER(REGEX(?author, '""" + str(authorPartialName) + """', "i"))  
                    }
                }
            } 
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getDistinctPublisherOfPublications(self, pubIdList):
        listsparql = []
        for i in range(len(pubIdList)):
            qry = """
            PREFIX schema: <https://schema.org/>
            SELECT DISTINCT *
            WHERE{ 
                ?s schema:publishedBy ?publisher.
                ?s schema:isPartOf ?venues.
                ?s schema:productID ?doi.
                ?s schema:productID '""" + str(pubIdList[i]) + """' }
            """
            df_sparql = get(self.endpointUri, qry, True)
            listsparql.append(df_sparql)
        return listsparql