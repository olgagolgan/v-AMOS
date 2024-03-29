from sparql_dataframe import get
import pandas as pd
from pandas import read_csv, concat, DataFrame
from rdflib import Graph, URIRef, RDF, Literal
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
import json
from dataModelClasses import *

# uri's for Triplestore DB

base_url = "https://github.com/olgagolgan/v-AMOS"
JournalArticle = URIRef("https://schema.org/ScholarlyArticle")
BookChapter = URIRef("https://schema.org/Chapter")
Journal = URIRef("https://schema.org/Periodical")
Book = URIRef("https://schema.org/Book")

# attributes related to classes
doi = URIRef("https://schema.org/productID")
venueType = URIRef("https://schema.org/additionalType")
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
    def __init__(self, endpointUrl=None):
            self.endpointUrl = endpointUrl

    def getEndpointUrl(self):
        return self.endpointUrl

    def setEndpointUrl(self, url):
        if url != '':
            self.endpointUrl = url
            return True
        else:
            return False


class TriplestoreDataProcessor(TriplestoreProcessor):
    def __init__(self, endpointUrl=None):
        super().__init__(endpointUrl)

    def uploadData(self, path):
        if path != "":
            my_graph = Graph()
            store = SPARQLUpdateStore()
            store.open((self.endpointUrl, self.endpointUrl))
            if path.endswith(".csv"):
                table = read_csv(path, keep_default_na=False)
                for idx, row in table.iterrows():
                    local_id = row["id"]
                    subj = URIRef(base_url + "/" + local_id)
                    my_graph.add((subj, RDF.type, Literal(row["type"])))
                    my_graph.add((subj, venueType, Literal(row["venue_type"])))
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
    def __init__(self, endpointUrl=None):
        super().__init__(endpointUrl)

    def getPublicationsPublishedInYear(self, year):
        qry = """
            PREFIX schema: <https://schema.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
            SELECT ?orcid ?given ?family ?title ?doi ?publication_venue ?publisher ?publication_year ?issue ?volume ?chapter ?type 
            WHERE {
            {SELECT ?orcid ?title ?doi ?publication_venue ?publisher ?publication_year ?issue ?volume ?chapter ?type
            WHERE{
              ?s schema:creator ?orcid.
              ?s schema:title ?title.
              ?s schema:productID ?doi.
              ?s schema:publishedBy ?publisher.
              ?s schema:isPartOf ?publication_venue.
              ?s rdf:type ?type.
              ?s schema:issueNumber ?issue.
              ?s schema:volumeNumber ?volume.
              ?s schema:Chapter ?chapter.
              ?s schema:datePublished ?publication_year.
              ?s schema:datePublished """ + str(year) + """ 
            }              
            }
             ?x schema:creator ?orcid.
             ?x schema:givenName ?given.
             ?x schema:familyName ?family
        }"""

        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getPublicationsByAuthorId(self, id):
        qry = """
            PREFIX schema: <https://schema.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
            SELECT ?orcid ?given ?family ?title ?doi ?publication_venue ?publisher ?publication_year ?issue ?volume ?chapter ?type
            WHERE {
            {SELECT ?orcid ?given ?family
            WHERE{
             ?x schema:creator ?orcid.
             ?x schema:givenName ?given.
             ?x schema:familyName ?family.
             ?x schema:creator '""" + str(id) + """'
            }                          
            }
          ?s schema:creator ?orcid.
          ?s schema:title ?title.
          ?s schema:productID ?doi.
          ?s schema:publishedBy ?publisher.
          ?s schema:isPartOf ?publication_venue.
          ?s rdf:type ?type.
          ?s schema:issueNumber ?issue.
          ?s schema:volumeNumber ?volume.
          ?s schema:Chapter ?chapter.
          ?s schema:datePublished ?publication_year                       
        }"""
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    # def getPublicationData(self, publicationID):
    #     qry = """
    #             PREFIX schema: <https://schema.org/>
    #             PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    #             SELECT ?orcid ?given ?family ?title ?doi ?publication_venue ?publisher ?publication_year ?issue ?volume ?chapter ?type (COUNT(?cite) as ?MostCited)
    #             WHERE {
    #             {
    #             SELECT ?orcid ?given ?family
    #                 WHERE{
    #                      ?x schema:creator ?orcid.
    #                      ?x schema:givenName ?given.
    #                      ?x schema:familyName ?family
    #                 }
    #             }
    #               ?s schema:creator ?orcid.
    #               ?s schema:title ?title.
    #               ?s schema:productID ?doi.
    #               ?s schema:publishedBy ?publisher.
    #               ?s schema:isPartOf ?publication_venue.
    #               ?s schema:datePublished ?publication_year.
    #               ?s rdf:type ?type.
    #               ?s schema:issueNumber ?issue.
    #               ?s schema:volumeNumber ?volume.
    #               ?s schema:Chapter ?chapter.
    #               ?s schema:citation ?cite
    #             }
    #             GROUP BY ?orcid ?given ?family ?title ?doi ?publication_venue ?publisher ?publication_year ?issue ?volume ?chapter ?type
    #             ORDER BY DESC(?MostCited)
    #         """
    #     df_sparql = get(self.endpointUrl, qry, True)
    #     return df_sparql


    def getMostCitedPublication(self):
        qry = """
                PREFIX schema: <https://schema.org/>
                SELECT ?doi
                WHERE{ 
                      ?s schema:productID ?doi.

                    }
            """
        df_sparql = get(self.endpointUrl, qry, True)
        df_nuovo = DataFrame()
        for row_idx, row in df_sparql.iterrows():
            query = """
                PREFIX schema: <https://schema.org/>
                SELECT ?doi ?doi_mention
                WHERE{ 
                      ?s schema:citation ?doi_mention.
                      ?s schema:productID ?doi.
                      ?s schema:productID'""" + str(row["doi"]) + """'
                    }
                """
            df_nuovo = pd.concat([df_nuovo, get(self.endpointUrl, query, True)])
        end_df = pd.concat([df_sparql, df_nuovo])
        return end_df



    def getMostCitedVenue(self):
        qry = """
        PREFIX schema: <https://schema.org/>
        SELECT ?doi ?doi_mention ?id_no ?title ?venue_id ?publisher ?name ?venue_type ?event
        WHERE {
            ?x schema:identifier ?publisher.
            ?x schema:name ?name
            {SELECT ?doi ?title ?publisher ?venue_id ?venue_type ?event ?doi_mention
            WHERE {
             ?s schema:productID ?doi.
             ?s schema:isPartOf ?title.
             ?s schema:publishedBy ?publisher.
             ?s schema:VirtualLocation ?venue_id.
             ?s schema:additionalType ?venue_type.
             ?s schema:Event ?event.
             ?s schema:citation ?doi_mention
                }    
            }
        }
        """
        df_sparql = get(self.endpointUrl, qry, True)
        output = df_sparql.rename(columns={"id_no": "id_no."})
        return output

    # qry = """
    #     PREFIX schema: <https://schema.org/>
    #     SELECT ?publication_venue ?venue_id ?publisher ?title ?venue_type ?event (COUNT(?cite) as ?MostCited)
    #     WHERE {
    #       ?s schema:isPartOf ?publication_venue.
    #       ?s schema:title ?title.
    #       ?s schema:publishedBy ?publisher.
    #       ?s schema:VirtualLocation ?venue_id.
    #       ?s schema:additionalType ?venue_type.
    #       ?s schema:Event ?event.
    #       ?s schema:citation ?cite
    #     }
    #     GROUP BY ?publication_venue ?venue_id ?title ?publisher ?venue_type ?event
    #     ORDER BY DESC(?MostCited)
    #     LIMIT 1
    # """


    def getVenuesByPublisherId(self, id):
        qry = """
            PREFIX schema: <https://schema.org/>
                SELECT ?publication_venue ?venue_id ?id ?name ?venue_type ?event
                WHERE{

                {SELECT ?name ?id 
                        WHERE {
                        ?x schema:name ?name.
                        ?x schema:identifier ?id.
                        ?x schema:identifier '""" + str(id) + """'.
                        }
                }
                ?s schema:publishedBy '""" + str(id) + """'.
                ?s schema:isPartOf ?publication_venue.
                ?s schema:VirtualLocation ?venue_id.
                ?s schema:additionalType ?venue_type.
                ?s schema:Event ?event

                }
        """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getPublicationInVenue(self, venueId):
        qry = """ 
                PREFIX schema: <https://schema.org/>
                PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
                SELECT ?orcid ?given ?family ?title ?doi ?publication_venue ?publication_year ?issue ?volume ?chapter ?type
                WHERE {
                {SELECT ?orcid ?given ?family
                WHERE{
               ?x schema:creator ?orcid.
               ?x schema:givenName ?given.
               ?x schema:familyName ?family
                }                          
                }
              ?s schema:creator ?orcid.
              ?s schema:title ?title.
              ?s schema:productID ?doi.
              ?s schema:datePublished ?publication_year.
              ?s schema:isPartOf ?publication_venue.
              ?s rdf:type ?type.
              ?s schema:issueNumber ?issue.
              ?s schema:volumeNumber ?volume.
              ?s schema:Chapter ?chapter.
              ?s schema:VirtualLocation ?venue_id.
              ?s schema:VirtualLocation '""" + str(venueId) + """'
            }"""

        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?orcid ?given ?family ?title ?doi ?publication_venue ?issue ?volume ?publisher ?publication_year 
            WHERE {
            {SELECT ?orcid ?given ?family
              WHERE{
               ?x schema:creator ?orcid.
               ?x schema:givenName ?given.
               ?x schema:familyName ?family.
            }                          
            }
          ?s schema:publishedBy ?publisher.
          ?s schema:productID ?doi.
          ?s schema:creator ?orcid.
          ?s schema:title ?title.
          ?s schema:datePublished ?publication_year.
          ?s schema:isPartOf ?publication_venue.
          ?s schema:issueNumber ?issue.
          ?s schema:issueNumber '""" + str(issue) + """'.
          ?s schema:volumeNumber ?volume.
          ?s schema:volumeNumber '""" + str(volume) + """'.
          ?s schema:VirtualLocation ?venue_id.
          ?s schema:VirtualLocation '""" + str(journalId) + """'
          }
         """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getJournalArticlesInVolume(self, volume, journalId):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?orcid ?given ?family ?title ?doi ?publication_venue ?issue ?volume ?publisher ?publication_year 
            WHERE {
            {SELECT ?orcid ?given ?family
              WHERE{
               ?x schema:creator ?orcid.
               ?x schema:givenName ?given.
               ?x schema:familyName ?family.
            }                          
            }
          ?s schema:publishedBy ?publisher.
          ?s schema:productID ?doi.
          ?s schema:creator ?orcid.
          ?s schema:title ?title.
          ?s schema:datePublished ?publication_year.
          ?s schema:isPartOf ?publication_venue.
          ?s schema:issueNumber ?issue.
          ?s schema:volumeNumber ?volume.
          ?s schema:volumeNumber '""" + str(volume) + """'.
          ?s schema:VirtualLocation ?venue_id.
          ?s schema:VirtualLocation '""" + str(journalId) + """'
          }    
        """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getJournalArticlesInJournal(self, journalId):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?orcid ?given ?family ?title ?doi ?publication_venue ?issue ?volume ?publisher ?publication_year 
            WHERE {
            {SELECT ?orcid ?given ?family
              WHERE{
               ?x schema:creator ?orcid.
               ?x schema:givenName ?given.
               ?x schema:familyName ?family.
            }                          
            }
          ?s schema:publishedBy ?publisher.
          ?s schema:productID ?doi.
          ?s schema:creator ?orcid.
          ?s schema:title ?title.
          ?s schema:datePublished ?publication_year.
          ?s schema:isPartOf ?publication_venue.
          ?s schema:issueNumber ?issue.
          ?s schema:volumeNumber ?volume.
          ?s schema:VirtualLocation ?venue_id.
          ?s schema:VirtualLocation '""" + str(journalId) + """'
          }      
        """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getProceedingsByEvent(self, eventPartialName):
        qry = """
        PREFIX schema:<https://schema.org/>

          SELECT ?publication_venue ?venue_id ?publisher ?event
          WHERE {
          ?x schema:productID ?doi.
          ?x schema:VirtualLocation ?venue_id

             {SELECT ?publication_venue ?publisher ?doi ?event
             WHERE{
              ?s schema:isPartOf ?publication_venue.
              ?s schema:publishedBy ?publisher.
              ?s schema:productID ?doi.
              ?s schema:Event ?event
              FILTER (REGEX(?event,'""" + str(eventPartialName) + """', "i"))
            }}
          }
         
         """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getPublicationAuthors(self, publicationId):
        qry = """
        PREFIX schema: <https://schema.org/>
        SELECT ?orcid ?given ?family
        WHERE
        { 
            ?x schema:creator ?orcid.
            ?x schema:givenName ?given.
            ?x schema:familyName ?family.
            {
                SELECT ?orcid
                WHERE {
                    
                    ?y schema:productID '""" + str(publicationId) + """'.
                    ?y schema:creator ?orcid}
            }
        }
        """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getPublicationsByAuthorName(self, authorPartialName):
        qry = """
            PREFIX schema: <https://schema.org/>
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> 
            SELECT ?orcid ?given ?family ?title ?doi ?publication_venue ?publication_year ?issue ?volume ?chapter ?type
            WHERE 
            {
                ?x schema:creator ?orcid.
                ?x schema:productID ?doi.
                ?x schema:title ?title.
                ?x schema:datePublished ?publication_year.
                ?x rdf:type ?type.
                ?x schema:issueNumber ?issue.
                ?x schema:volumeNumber ?volume.
                ?x schema:Chapter ?chapter.
                ?x schema:isPartOf ?publication_venue.

            {
                SELECT ?orcid ?given ?family
                WHERE
                    {
                        ?s schema:creator ?orcid.
                        ?s schema:givenName ?given.
                        ?s schema:familyName ?family.
                        BIND (CONCAT (?family, " ", ?given) as ?author).
                        FILTER(REGEX(?author, '""" + str(authorPartialName) + """', "i"))  
                    }
                }
            } 
        """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getDistinctPublisherOfPublications(self, pubIdList):
        listOfTarget = ''
        for pubId in pubIdList:
            listOfTarget = listOfTarget + '"' + pubId + '" '

        qry = """
        PREFIX schema: <https://schema.org/>
        SELECT ?id ?name
        WHERE {
          ?x schema:name ?name.
          ?x schema:identifier ?id. {
        SELECT ?id
        WHERE {
          VALUES ?target {""" + listOfTarget + """}
          ?s schema:publishedBy ?id.
          ?s schema:isPartOf ?publication_venue.
          ?s schema:productID ?doi.
          ?s schema:productID  ?target }  }    
          }
          """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getCitedOfPublication(self, publicationId):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?doi ?orcid ?given ?family ?title ?publication_venue ?publisher ?publication_year ?issue ?volume ?chapter ?type
            WHERE{ 
              {SELECT ?title ?publication_venue ?publication_year ?publisher ?orcid ?doi ?issue ?volume ?chapter ?type
               WHERE{ 
                 {SELECT ?doi
                  WHERE{
                       ?y schema:citation ?doi.
                       ?y schema:productID '""" + str(publicationId) + """'
                    }
                 }
                     ?x schema:citation ?doi.
                     ?x schema:title ?title.
                     ?x rdf:type ?type.
                     ?x schema:issueNumber ?issue.
                     ?x schema:volumeNumber ?volume.
                     ?x schema:Chapter ?chapter.
                     ?x schema:isPartOf ?publication_venue.
                     ?x schema:datePublished ?publication_year.
                     ?x schema:publishedBy ?publisher.
                     ?x schema:creator ?orcid
                    }

                }
                ?x schema:creator ?orcid.
                ?x schema:givenName ?given.
                ?x schema:familyName ?family
            }          
            """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    def getPubInfo(self, publicationId):
        qry = """
                    PREFIX schema: <https://schema.org/>
                    SELECT ?doi ?orcid ?given ?family ?title ?publication_venue ?publisher ?publication_year ?issue ?volume ?chapter ?type
                    WHERE{ 
                      {SELECT ?title ?publication_venue ?publication_year ?publisher ?orcid ?doi ?issue ?volume ?chapter ?type
                       WHERE{ 
                         {SELECT ?doi
                          WHERE{
                               ?y schema:productID ?doi.
                               ?y schema:productID '""" + str(publicationId) + """'
                            }
                         }
                             ?x schema:productID ?doi.
                             ?x schema:title ?title.
                             ?x rdf:type ?type.
                             ?x schema:issueNumber ?issue.
                             ?x schema:volumeNumber ?volume.
                             ?x schema:Chapter ?chapter.
                             ?x schema:isPartOf ?publication_venue.
                             ?x schema:datePublished ?publication_year.
                             ?x schema:publishedBy ?publisher.
                             ?x schema:creator ?orcid
                            }

                        }
                        ?x schema:creator ?orcid.
                        ?x schema:givenName ?given.
                        ?x schema:familyName ?family
                    }          
                    """
        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

    # def getOnlyCite(self, publicationId):
    #     qry = """
    #         PREFIX schema: <https://schema.org/>
    #         SELECT ?doi ?cite
    #         WHERE{
    #               ?s schema:citation ?cite.
    #               ?s schema:productID ?doi.
    #               ?s schema:productID  '""" + str(publicationId) + """'
    #             }
    #         """
    #     df_sparql = get(self.endpointUrl, qry, True)
    #     print(df_sparql)
    #     return df_sparql

    # def getAllDoi(self):
    #     qry = """
    #         PREFIX schema: <https://schema.org/>
    #         SELECT ?doi
    #         WHERE{
    #               ?s schema:productID ?doi.
    #
    #             }
    #         """
    #     df_sparql = get(self.endpointUrl, qry, True)
    #     print(df_sparql)
    #     return df_sparql

    def getVenueByPublicationId(self, doi):
        qry = """
            PREFIX schema: <https://schema.org/>
            SELECT ?venue_id ?title ?id ?name ?venue_type ?event 
            WHERE{ 
            {SELECT ?doi ?title ?id ?venue_type ?event
            WHERE{
                  ?y schema:publishedBy ?id.
                  ?y schema:isPartOf ?title.
                  ?y schema:additionalType ?venue_type.
                  ?y schema:Event ?event.
                  ?y schema:productID ?doi.
                  ?y schema:productID  '""" + str(doi) + """'
                }
             }
              ?y schema:productID ?doi.
              ?y schema:VirtualLocation ?venue_id.
              ?x schema:identifier ?id.
              ?x schema:name ?name.
         }                
        """

        df_sparql = get(self.endpointUrl, qry, True)
        return df_sparql

# setting the environment for testing based on our dataset


# graph1 = TriplestoreProcessor()
# graph2 = TriplestoreDataProcessor()
# graph2.setEndpointUrl("http://127.0.0.1:9999/blazegraph/sparql")
# print(graph2.uploadData("data/graph_publications.csv"))
# print(graph2.uploadData("data/graph_other_data.json"))
# trp_qp = TriplestoreQueryProcessor()
# trp_qp.setEndpointUrl("http://127.0.0.1:9999/blazegraph/sparql")
# trp_qp.getMostCitedPublication()
# print(ciao)

