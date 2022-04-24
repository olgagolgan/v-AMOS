from numpy import empty
from commitDB import *
from pandas import DataFrame, read_sql
from sqlite3 import connect

database = createDB()
"""
print("=== General === \n", csvDf.info)
print("=== Author === \n", author_pubDf.info)
print("=== Venues === \n", venueDf.info)
print("=== References === \n", refDf.info)
print("=== Publishers === \n", publisherDf.info)
"""

def getPublicationsPublishedInYear(self, year):
    with connect("publications.db") as con:  
        query = "SELECT * FROM General WHERE publication_year = '{0}';".format(year)
        df_sql = read_sql(query, con)
        return df_sql

def getPublicationsByAuthorId(self, id):
    with connect("publications.db") as con:  
        query = "SELECT * FROM Author LEFT JOIN General ON Author.doi == General.id WHERE orcid = '{0}';".format(id)
        df_sql = read_sql(query, con)
        return df_sql

def getMostCitedPublication(self):
    with connect("publications.db") as con:  
        query = "NOT WORKING !!!;"
        df_sql = read_sql(query, con)
        return df_sql

def getMostCitedVenue(self):
    with connect("publications.db") as con:  
        query = "NOT WORKING !!!;"
        df_sql = read_sql(query, con)
        return df_sql

def getVenuesByPublisherId(self):
    with connect("publications.db") as con:
        query = "SELECT * FROM General WHERE publisher = '{0}';".format(id) 
        df_sql = read_sql(query, con)
        return df_sql

def getPublicationInVenue(self, venueId):
    with connect("publications.db") as con:
        query = "SELECT * FROM General LEFT JOIN Venues ON General.id==Venues.doi WHERE Venues.id ='{0}';".format(venueId)
        df_sql = read_sql(query, con)
        return df_sql 

def getJournalArticlesInIssue(self, issue, volume, journalId):
    with connect("publications.db") as con:
        query = "SELECT * FROM General LEFT JOIN Venues ON General.id==Venues.doi WHERE type = 'journal-article' AND issue='{0}' AND volume ='{1}' AND Venues.id ='{2}';".format(issue, volume, journalId)
        df_sql = read_sql(query, con)
        return df_sql

def getJournalArticlesInVolume(self, volume, journalId):
    with connect("publications.db") as con:
        query = "SELECT * FROM General LEFT JOIN Venues ON General.id==Venues.doi WHERE type = 'journal-article' AND volume ='{0}' AND Venues.id ='{1}';".format(volume, journalId)
        df_sql = read_sql(query, con)
        return df_sql

def getJournalArticlesInJournal(self, journalId):
    with connect("publications.db") as con:
        query = "SELECT * FROM General LEFT JOIN Venues ON General.id==Venues.doi WHERE type = 'journal-article' AND Venues.id ='{0}';".format(journalId)
        df_sql = read_sql(query, con)
        return df_sql

def getProceedingsByEvent(self, eventPartialName):
    with connect("publications.db") as con:
        query = "SELECT * FROM General WHERE type = 'proceedings' AND event LIKE '%{0}%';".format(eventPartialName)
        df_sql = read_sql(query, con)
        return df_sql

def getPublicationAuthors(self, publicationId):
    with connect("publications.db") as con:
        query = "SELECT family, given, orcid FROM Author WHERE doi = '{0}';".format(publicationId)
        df_sql = read_sql(query, con)
        return df_sql

def getPublicationsByAuthorName(self, authorPartialName):
    with connect("publications.db") as con: 
        query = "SELECT * FROM General LEFT JOIN Author ON General.id == Author.doi WHERE Author.family COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%' OR Author.given COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%';".format(authorPartialName)
        df_sql = read_sql(query, con)
        return df_sql

def getDistinctPublisherOfPublications(self, pubIdList):
    with connect("publications.db") as con:
        output = DataFrame()
        for el in pubIdList:
            query = "SELECT Publishers.id, Publishers.name FROM Publishers LEFT JOIN General ON Publishers.id == General.publisher WHERE General.id = '{0}'".format(el)
            df_sql = read_sql(query, con)
            output = empty.append(df_sql)
        return output
    """
    getDistinctPublisherOfPublications: It returns a data frame with all the distinct publishers (i.e. the rows) that have published the venues of the publications with identifiers those specified as input (e.g. [ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]).
    """

self = 0
#print(" 1° query \n", getPublicationsPublishedInYear(self, "2020"))
#print(" 2° query \n", getPublicationsByAuthorId(self, "0000-0003-0530-4305"))
#print(" 3° query \n", getMostCitedPublication(self))                               * 
#print(" 4° query \n", getMostCitedVenue(self))                                     *
#print(" 5° query \n", getVenuesByPublisherId(self, "crossref:78"))
#print(" 6° query \n", getPublicationInVenue(self, "issn:0944-1344"))
#print(" 7° query \n", getJournalArticlesInIssue(self, 9, 17, 'issn:2164-5515'))
#print(" 8° query \n", getJournalArticlesInVolume(self, 17, 'issn:2164-5515'))
#print(" 9° query \n", getJournalArticlesInJournal(self, 'issn:2164-5515'))
#print("10° query \n", getProceedingsByEvent(self, "web"))
#print("11° query \n", getPublicationAuthors(self, "doi:10.1080/21645515.2021.1910000"))
#print("12° query \n", getPublicationsByAuthorName(self, "iv"))                    
#print("13° query \n", getDistinctPublisherOfPublications(self, ["doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035"]))