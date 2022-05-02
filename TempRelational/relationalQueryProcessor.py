from platform import release
from sqlite3 import connect
import pandas as pd
from pandas import DataFrame,read_sql
from AdditionalClasses import RelationalProcessor, RelationalDataProcessor

class RelationQueryProcessor(RelationalProcessor):
    def __init__(self, dbPath):
        super().__init__(dbPath)
    
    def getPublicationsPublishedInYear(self, year):
        with connect(self.dbPath) as con:  
            query = '''SELECT id, publication_year FROM JournalArticles WHERE publication_year = '{0}' UNION  SELECT id, publication_year FROM BookChapter WHERE publication_year ='{0}' UNION SELECT id, publication_year FROM ProceedingsPaper WHERE publication_year ='{0}';'''.format(year)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsPublishedInYear: It returns a data frame with all the publications (i.e. the rows) that have been published in the input year (e.g. 2020).
    """

    def getPublicationsByAuthorId(self, id):
        with connect(self.dbPath) as con:  
            query = '''SELECT  DISTINCT id FROM JournalArticles  WHERE orcid like  "%'{0}'%" UNION SELECT  DISTINCT id FROM BookChapter  WHERE orcid like  "%'{0}'%" UNION SELECT  DISTINCT id FROM ProceedingsPaper  WHERE orcid like  "%'{0}'%";'''.format(id)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsByAuthorId: It returns a data frame with all the publications (i.e. the rows) that have been authored by the person having the identifier specified as input (e.g. "0000-0001-9857-1511").
    """

    def getMostCitedPublication(self):
        with connect(self.dbPath) as con:  
            query = "NOT WORKING !!!;"
            df_sql = read_sql(query, con)
            return df_sql

    """
    getMostCitedPublication: It returns a data frame with all the publications (i.e. the rows) that have received the most number of citations by other publications.
    """

    def getMostCitedVenue(self):
        with connect(self.dbPath) as con:  
            query = "NOT WORKING !!!;"
            df_sql = read_sql(query, con)
            return df_sql

    """
    getMostCitedVenue: It returns a data frame with all the venues (i.e. the rows) containing the publications that, overall, have received the most number of citations by other publications.
    """

    def getVenuesByPublisherId(self, id):
        with connect(self.dbPath) as con:
            query = '''SELECT  "issn/isbn" FROM Journal WHERE publisher="{0}" UNION SELECT  "issn/isbn" FROM Book WHERE publisher="{0}" UNION  SELECT  "issn/isbn" FROM Proceedings WHERE publisher="{0}";'''.format(id)
            df_sql = read_sql(query, con)
            return df_sql
    
    """
    getVenuesByPublisherId: It returns a data frame with all the venues (i.e. the rows) that have been published by the organisation having the identifier specified as input (e.g. "crossref:78").
    """

    def getPublicationInVenue(self, venueId):
        with connect(self.dbPath) as con:
            query = '''SELECT  DISTINCT id FROM JournalArticles WHERE "issn/isbn" like  "%'{0}'%"  UNION SELECT  DISTINCT id FROM BookChapter WHERE "issn/isbn" like  "%'{0}'%"  UNION SELECT  DISTINCT id FROM ProceedingsPaper WHERE "issn/isbn" like  "%'{0}'%" ;'''.format(venueId)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationInVenue: It returns a data frame with all the publications (i.e. the rows) that have been included in the venue having the identifier specified as input (e.g. "issn:0944-1344").
    """

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        with connect(self.dbPath) as con:
            query = '''SELECT id FROM JournalArticles WHERE issue = '{0}' AND volume = '{1}' AND "issn/isbn" like  "%'{2}'%";'''.format(issue, volume, journalId)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getJournalArticlesInIssue: It returns a data frame with all the journal articles (i.e. the rows) that have been included in the input issue (e.g. "9") of the input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getJournalArticlesInVolume(self, volume, journalId):
        with connect(self.dbPath) as con:
            query = '''SELECT id FROM JournalArticles WHERE volume = '{0}' AND "issn/isbn" like  "%'{1}'%";'''.format(volume, journalId)
            df_sql = read_sql(query, con)
            return df_sql
        
    """
    getJournalArticlesInVolume: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue, in input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getJournalArticlesInJournal(self, journalId):
        with connect(self.dbPath) as con:
            query = '''SELECT id FROM JournalArticles WHERE "issn/isbn" like  "%'{0}'%";'''.format(journalId)
            df_sql = read_sql(query, con)
            return df_sql
        
    """
    getJournalArticlesInJournal: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue and the volume, in the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getProceedingsByEvent(self, eventPartialName):
        with connect(self.dbPath) as con:
            query = '''SELECT  "issn/isbn" FROM Proceedings COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%';'''.format(eventPartialName)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getProceedingsByEvent: It returns a data frame with all the proceedings (i.e. the rows) that refer to the events that match (in lowercase), even partially, with the name specified as input (e.g. "web").
    """

    def getPublicationAuthors(self, publicationId):
        with connect(self.dbPath) as con:
            query = '''SELECT orcid FROM JournalArticles WHERE id = '{0}' UNION  SELECT orcid FROM BookChapter WHERE id = '{0}' UNION SELECT orcid FROM ProceedingsPaper WHERE id = '{0}';'''.format(publicationId)
            coauthors = read_sql(query, con)
            for label, content in coauthors.iteritems():
                for el in content:
                    authorIds = el
                    listOrcid = authorIds.strip('}{').split(', ')
            output = DataFrame({"orcid":listOrcid})
            return output

    """
    getPublicationAuthors: It returns a data frame with all the authors (i.e. the rows) of the publication with the identifier specified as input (e.g. "doi:10.1080/21645515.2021.1910000").
    """

    def getPublicationsByAuthorName(self, authorPartialName):
        with connect(self.dbPath) as con: 
            query = "SELECT DISTINCT orcid FROM Author WHERE family COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%' OR given COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%';".format(authorPartialName)
            authorId = read_sql(query, con)
            for label, content in authorId.iteritems():
                for el in content:
                    orcidQuery = el
            query = '''SELECT  DISTINCT id FROM JournalArticles  WHERE orcid like  "%{0}%" UNION SELECT  DISTINCT id FROM BookChapter  WHERE orcid like  "%{0}%" UNION SELECT  DISTINCT id FROM ProceedingsPaper  WHERE orcid like  "%{0}%";'''.format(orcidQuery)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsByAuthorName: It returns a data frame with all the publications (i.e. the rows) that have been authored by the people having their name matching (in lowercase), even partially, with the name specified as input (e.g. "doe").
    """

    def getDistinctPublisherOfPublications(self, pubIdList):
        with connect(self.dbPath) as con:
            output = DataFrame()
            for el in pubIdList:
                query = '''SELECT Journal.publisher FROM  JournalArticles LEFT JOIN Journal ON JournalArticles."issn/isbn" == Journal."issn/isbn" WHERE JournalArticles.id = "{0}" UNION SELECT Book.publisher FROM  BookChapter LEFT JOIN Book ON BookChapter."issn/isbn" == Book."issn/isbn" WHERE BookChapter.id = "{0}" UNION SELECT Proceedings.publisher FROM  ProceedingsPaper LEFT JOIN Proceedings ON ProceedingsPaper."issn/isbn" == Proceedings."issn/isbn" WHERE ProceedingsPaper.id = "{0}";'''.format(el)
                df_sql = read_sql(query, con)
                output = pd.concat([output, df_sql])
            return output

    """
    getDistinctPublisherOfPublications: It returns a data frame with all the distinct publishers (i.e. the rows) that have published the venues of the publications with identifiers those specified as input (e.g. [ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]).
    """

relPro = RelationalProcessor("attempt0502.db")
relProUp = RelationalDataProcessor(relPro)
csv = relProUp.uploadData("data/relational_publications.csv")
json = relProUp.uploadData("data/relationalJSON.json")
test = RelationQueryProcessor(relPro)
q1 = test.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ])