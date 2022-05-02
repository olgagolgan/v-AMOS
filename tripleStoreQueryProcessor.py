from sparql_dataframe import get
from AdditionalClasses import TriplestoreProcessor

class TriplestoreQueryProcessor(TriplestoreProcessor):
    def __init__(self, endpointUri):
        super().__init__(endpointUri + "/sparql")

    def getPublicationsPublishedInYear(self, year=2014):
        qry = """
        PREFIX schema: <https://schema.org/>
        
        SELECT *
        WHERE {
          ?s schema:title ?title.
          ?s schema:datePublished ?date.
          ?s schema:datePublished """ + str(year) + """
        }"""
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getPublicationsByAuthorId(self, id):
        qry = """
        PREFIX schema: <https://schema.org/>
        SELECT *
        WHERE {
            ?s schema:title ?title.
            ?s schema:creator ?orcid.
            ?s schema:creator """ + id + """
        }"""
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getMostCitedPublication(self):
        qry = """
        PREFIX schema: <https://schema.org/>
        SELECT ?title (COUNT(?reference) as ?mostCited)
        WHERE {
            ?s schema:title ?title.
            ?s schema:citation ?reference
            }
        GROUP BY ?title
        ORDER BY DESC(?mostCited) 
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getMostCitedVenue(self):
        qry = """
        PREFIX schema: <https://schema.org/>

        SELECT ?venues(COUNT(?reference) as ?mostCited)
        WHERE {
            ?s schema:isPartOf ?venues.
            ?s schema:citation ?reference
        }
        GROUP BY ?venues
        ORDER BY DESC(?mostCited)
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getVenuesByPublisherId(self, id):
        qry = """
        PREFIX schema: <https://schema.org/>
        SELECT *
        WHERE {
            ?s schema:title ?title.
            ?s schema:isPartOf ?venues.
            ?s schema:publishedBy """ + id + """
        }"""
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getPublicationInVenue(self, venueId):
        qry = """ 
                PREFIX schema:<https://schema.org/>
                SELECT ?title
                WHERE {
                    ?s schema:title ?title.
                    ?s schema:isPartOf ?venues.
                    ?s schema:identifier """ + venueId + """
                    }"""
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        qry = """
                PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                PREFIX schema:<https://schema.org/>

                SELECT ?JournalArticle
                    WHERE {
                      ?s rdf:type schema:ScholarlyArticle.
                      ?s schema:name ?JournalArticle.
                      ?s schema:isPartOf ?issue.
                      ?s schema:issueNumber """ + str(issue) + """ .
                      ?s schema:isPartOf ?volume.
                      ?s schema:volumeNumber """ + str(volume) + """ . 
                      ?s schema:isPartOf ?journal.
                      ?s schema:identifier """ + journalId + """
                     }
                    """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getJournalArticlesInVolume(self, volume, journalId):
        qry = """
                    PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX schema:<https://schema.org/>

                    SELECT ?JournalArticle
                            WHERE {
                              ?s rdf:type schema:ScholarlyArticle.
                              ?s schema:name ?JournalArticle.
                              ?s schema:isPartOf ?volume.
                              ?s schema:volumeNumber """ + str(volume) + """ . 
                              ?s schema:isPartOf ?journal.
                              ?s schema:identifier """ + journalId + """

                             }
                    """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getJournalArticlesInJournal(self, journalId):
        qry = """
                    PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX schema:<https://schema.org/>

                    SELECT ?JournalArticle
                            WHERE {
                              ?s rdf:type schema:ScholarlyArticle.
                              ?s schema:name ?JournalArticle.
                              ?s schema:isPartOf ?journal.
                              ?s schema:identifier """ + journalId + """
                             }
                    """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getProceedingsByEvent(self, eventPartialName):
        qry = """
          PREFIX schema:<https://schema.org/>
          PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

          SELECT ?title ?doi
          WHERE {
              ?s rdf:type schema:ScholarlyArticle.
              ?s schema:title ?title.
              ?s schema:productID ?doi.
              ?s schema:Event ?event.
              FILTER (REGEX(?event, """ + eventPartialName + """ , "i")).
      
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
                    ?y schema:productID """ + publicationId + """
                    ?y schema:creator ?orcid.}
            }
        }
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getPublicationsByAuthorName(self, authorPartialName):
        qry = """
        PREFIX schema: < https: // schema.org / >
        SELECT ?doi ?title
        WHERE
        {
            ?x schema: creator ?orcid.
            ?x schema: productID ?doi.
            ?x schema: title ?title.
            {
            SELECT ?orcid
            WHERE
                {
                    ?s schema: creator ?orcid.
                    ?s schema: givenName ?name.
                    ?s schema: familyName ?surname.
                    FILTER(REGEX(?surname, """ + authorPartialName + """, "i") | | REGEX(?name, """ + authorPartialName + """, "i")).
                }
            }
        }
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql

    def getDistinctPublisherOfPublications(self, pubIdList):
        qry = """
        PREFIX schema: <https://schema.org/>
        SELECT DISTINCT *
        WHERE{ 
            ?s schema:publishedBy ?publisher.
            ?s schema:isPartOf ?venues.
            ?s schema:productID """ + pubIdList + """ }
        """
        df_sparql = get(self.endpointUri, qry, True)
        return df_sparql