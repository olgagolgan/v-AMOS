from parser import *
from sqlite3 import connect
from pandas import read_sql

print("=== Author === \n", author_pubDB)
print("=== Venues === \n", venueDB)
print("=== References === \n", refDB)
print("=== Publishers === \n", publisherDB)

def getPublicationsPublishedInYear(year):
    with connect(self.dbPath) as con:
        query = "SELECT * FROM XXX WHERE publication_year = " + str(year) 
        query = "SELECT * FROM Publication WHERE publication_year = '{0}'".format(year)


        df_sql = read_sql(query, con)
        return df_sql

    """
    getPublicationsPublishedInYear: It returns a data frame with all the publications (i.e. the rows) that have been published in the input year (e.g. 2020).
    """

def getPublicationsByAuthorId(self, id):
    pass
    """
    getPublicationsByAuthorId: It returns a data frame with all the publications (i.e. the rows) that have been authored by the person having the identifier specified as input (e.g. "0000-0001-9857-1511").
    """

def getMostCitedPublication(self):
    pass

    """
    getMostCitedPublication: It returns a data frame with all the publications (i.e. the rows) that have received the most number of citations by other publications.
    """

def getMostCitedVenue(self):
    pass

    """
    getMostCitedVenue: It returns a data frame with all the venues (i.e. the rows) containing the publications that, overall, have received the most number of citations by other publications.
    """

def getVenuesByPublisherId(self, id):
    with connect(self.dbPath) as con:
        query = "SELECT * FROM XXX WHERE publisher = " + str(id) 
                #per il FROM come faccio a capire qual è il nome del databse? 
        df_sql = read_sql(query, con)
        return df_sql

    """
    getVenuesByPublisherId: It returns a data frame with all the venues (i.e. the rows) that have been published by the organisation having the identifier specified as input (e.g. "crossref:78").
    """

def getPublicationInVenue(self, venueId):
    pass

    """
    getPublicationInVenue: It returns a data frame with all the publications (i.e. the rows) that have been included in the venue having the identifier specified as input (e.g. "issn:0944-1344").
    """

def getJournalArticlesInIssue(self, issue, volume, journalId):
    with connect(self.dbPath) as con:
        query = "SELECT * FROM XXX WHERE type = journal-article AND issue = " + str(issue) + " AND volume = " + str(volume) # Manca journal ID ==> deve aprire anche il json
                #per il FROM come faccio a capire qual è il nome del databse? 
        df_sql = read_sql(query, con)
        return df_sql

    """
    getJournalArticlesInIssue: It returns a data frame with all the journal articles (i.e. the rows) that have been included in the input issue (e.g. "9") of the input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

def getJournalArticlesInVolume(self, volume, journalId):
    pass

    """
    getJournalArticlesInVolume: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue, in input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

def getJournalArticlesInJournal(self, journalId):
    pass
    """
    getJournalArticlesInJournal: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue and the volume, in the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

def getProceedingsByEvent(self, eventPartialName):
    pass

    """
    getProceedingsByEvent: It returns a data frame with all the proceedings (i.e. the rows) that refer to the events that match (in lowercase), even partially, with the name specified as input (e.g. "web").
    """

def getPublicationAuthors(self, publicationId):
    pass

    """
    getPublicationAuthors: It returns a data frame with all the authors (i.e. the rows) of the publication with the identifier specified as input (e.g. "doi:10.1080/21645515.2021.1910000").
    """

def getPublicationsByAuthorName(self, authorPartialName):
    pass

    """
    getPublicationsByAuthorName: It returns a data frame with all the publications (i.e. the rows) that have been authored by the people having their name matching (in lowercase), even partially, with the name specified as input (e.g. "doe").
    """

def getDistinctPublisherOfPublications(self, pubIdList):
    pass

    """
    getDistinctPublisherOfPublications: It returns a data frame with all the distinct publishers (i.e. the rows) that have published the venues of the publications with identifiers those specified as input (e.g. [ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]).
    """