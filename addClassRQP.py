from sqlite3 import connect
import pandas as pd
from pandas import DataFrame,read_sql
from AdditionalClasses import RelationalProcessor

class RelationQueryProcessor(RelationalProcessor):
    def __init__(self, dbPath):
        super().__init__(dbPath)
    
    def getPublicationsPublishedInYear(self, year):
        with connect("publications.db") as con:  
            query = "SELECT * FROM General WHERE publication_year = '{0}';".format(year)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsPublishedInYear: It returns a data frame with all the publications (i.e. the rows) that have been published in the input year (e.g. 2020).
    """

    def getPublicationsByAuthorId(self, id):
        with connect("publications.db") as con:  
            query = "SELECT * FROM Author LEFT JOIN General ON Author.doi == General.id WHERE orcid = '{0}';".format(id)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsByAuthorId: It returns a data frame with all the publications (i.e. the rows) that have been authored by the person having the identifier specified as input (e.g. "0000-0001-9857-1511").
    """

    def getMostCitedPublication(self):
        with connect("publications.db") as con:  
            query = "NOT WORKING !!!;"
            df_sql = read_sql(query, con)
            return df_sql

    """
    getMostCitedPublication: It returns a data frame with all the publications (i.e. the rows) that have received the most number of citations by other publications.
    """

    def getMostCitedVenue(self):
        with connect("publications.db") as con:  
            query = "NOT WORKING !!!;"
            df_sql = read_sql(query, con)
            return df_sql

    """
    getMostCitedVenue: It returns a data frame with all the venues (i.e. the rows) containing the publications that, overall, have received the most number of citations by other publications.
    """

    def getVenuesByPublisherId(self):
        with connect("publications.db") as con:
            query = "SELECT * FROM General WHERE publisher = '{0}';".format(id) 
            df_sql = read_sql(query, con)
            return df_sql

    """
    getVenuesByPublisherId: It returns a data frame with all the venues (i.e. the rows) that have been published by the organisation having the identifier specified as input (e.g. "crossref:78").
    """

    def getPublicationInVenue(self, venueId):
        with connect("publications.db") as con:
            query = "SELECT * FROM General LEFT JOIN Venues ON General.id==Venues.doi WHERE Venues.id ='{0}';".format(venueId)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationInVenue: It returns a data frame with all the publications (i.e. the rows) that have been included in the venue having the identifier specified as input (e.g. "issn:0944-1344").
    """

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        with connect("publications.db") as con:
            query = "SELECT * FROM General LEFT JOIN Venues ON General.id==Venues.doi WHERE type = 'journal-article' AND issue='{0}' AND volume ='{1}' AND Venues.id ='{2}';".format(issue, volume, journalId)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getJournalArticlesInIssue: It returns a data frame with all the journal articles (i.e. the rows) that have been included in the input issue (e.g. "9") of the input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getJournalArticlesInVolume(self, volume, journalId):
        with connect("publications.db") as con:
            query = "SELECT * FROM General LEFT JOIN Venues ON General.id==Venues.doi WHERE type = 'journal-article' AND volume ='{0}' AND Venues.id ='{1}';".format(volume, journalId)
            df_sql = read_sql(query, con)
            return df_sql
    """
    getJournalArticlesInVolume: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue, in input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getJournalArticlesInJournal(self, journalId):
        with connect("publications.db") as con:
            query = "SELECT * FROM General LEFT JOIN Venues ON General.id==Venues.doi WHERE type = 'journal-article' AND Venues.id ='{0}';".format(journalId)
            df_sql = read_sql(query, con)
            return df_sql
    """
    getJournalArticlesInJournal: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue and the volume, in the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getProceedingsByEvent(self, eventPartialName):
        with connect("publications.db") as con:
            query = "SELECT * FROM General WHERE type = 'proceedings' AND event COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%';".format(eventPartialName)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getProceedingsByEvent: It returns a data frame with all the proceedings (i.e. the rows) that refer to the events that match (in lowercase), even partially, with the name specified as input (e.g. "web").
    """

    def getPublicationAuthors(self, publicationId):
        with connect("publications.db") as con:
            query = "SELECT family, given, orcid FROM Author WHERE doi = '{0}';".format(publicationId)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationAuthors: It returns a data frame with all the authors (i.e. the rows) of the publication with the identifier specified as input (e.g. "doi:10.1080/21645515.2021.1910000").
    """

    def getPublicationsByAuthorName(self, authorPartialName):
        with connect("publications.db") as con: 
            query = "SELECT * FROM General LEFT JOIN Author ON General.id == Author.doi WHERE Author.family COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%' OR Author.given COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%';".format(authorPartialName)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsByAuthorName: It returns a data frame with all the publications (i.e. the rows) that have been authored by the people having their name matching (in lowercase), even partially, with the name specified as input (e.g. "doe").
    """

    def getDistinctPublisherOfPublications(self, pubIdList):
        with connect("publications.db") as con:
            output = DataFrame()
            for el in pubIdList:
                query = "SELECT Publishers.id, Publishers.name FROM Publishers LEFT JOIN General ON Publishers.id == General.publisher WHERE General.id = '{0}'".format(el)
                df_sql = read_sql(query, con)
                output = pd.concat([output, df_sql])
            return output

    """
    getDistinctPublisherOfPublications: It returns a data frame with all the distinct publishers (i.e. the rows) that have published the venues of the publications with identifiers those specified as input (e.g. [ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]).
    """