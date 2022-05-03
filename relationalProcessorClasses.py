from sqlite3 import connect
from pandas import read_csv, read_sql, DataFrame
from json import load
import pandas as pd

class RelationalProcessor:
    def __init__(self, dbPath):
        self.dbPath = dbPath

    def getDbPath(self):  
        return self.dbPath

    def setDbPath(self, path):
        if path != '': 
            self.dbPath = path
            return True 
        else:
            return False
            
class RelationalDataProcessor(RelationalProcessor):
    def __init__(self, dbPath):
      super().__init__(dbPath)

    def uploadData(self, path):
        if path != '':            
            if format == "json": 
                with open(path, "r", encoding="utf-8") as f:
                    jsonData = load(f)

                    # ========== AUTHOR =======================
                    authors = jsonData['authors']
                    datum = []
                    for doi in authors:
                        data_row = authors[doi]
                        for row in data_row:
                            datum.append(row)
                    authorDf = pd.DataFrame(datum)
                    
                    # =========== AUTHOR of PUB ================

                    datum = []
                    for doi in authors:
                        setOrcid = set()
                        for item in authors[doi]:
                            setOrcid.add(item['orcid'])
                            datum.append([doi, str(setOrcid)])   
                    autOfPub = pd.DataFrame(datum, columns=["doi","orcid"])

                    # ========== PUBLISHERS ===================
                    publishers = jsonData['publishers']
                    datum = []
                    for cross_ref in publishers:
                        datum.append([publishers[cross_ref]['id'], publishers[cross_ref]['name']])
                    publisherDf = pd.DataFrame(datum, columns=['id','name'])

                    # ========== REFERENCES SINGLE CELL ===================
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
                    bibliographydDf = df2.join(df1)

                    # ========= CITATION ============
                    references = jsonData['references']
                    datum = []
                    for doi in references:
                        datum.append([doi,str(references[doi])])
                        citeDf = pd.DataFrame(datum, columns=["doi","cite"])

                    #========= VENUES ID ====================

                    venues = jsonData['venues_id']
                    datum = []
                    for doi in venues:
                        datum.append([doi, str(venues[doi])])
                    venuePub = pd.DataFrame(datum, columns=["doi","issn/isbn"])

                    with connect(self.dbPath) as con:
                        authorDf.to_sql("Author", con, if_exists="replace", index=False)
                        autOfPub.to_sql("Author&Publication", con, if_exists="replace", index=False)        
                        publisherDf.to_sql("Publisher", con, if_exists="replace", index=False)
                        bibliographydDf.to_sql("CitationsListed", con, if_exists="replace", index=False)
                        citeDf.to_sql("CitationsCondensed", con, if_exists="replace", index=False)
                        venuePub.to_sql("Venues", con, if_exists="replace", index=False)
                        con.commit()

                    return True
            elif format == "csv":
                csvData = pd.read_csv("data/relational_publications.csv")

                # ========= JOURNAL ARTICLE ==============
                journal_articles = csvData.query("type == 'journal-article'")
                journal_articles = journal_articles[["id","title","publication_year","issue","volume", "publication_venue", "publisher"]]
                print(journal_articles)

                # ========= BOOK CHAPTER ============== 
                book_chapter = csvData.query("type == 'book-chapter'")
                book_chapter = book_chapter[["id","title","publication_year","chapter", "publication_venue", "publisher"]]

                # ========= PROCEEDINGS-PAPER ============== 
                proceedings_paper = csvData.query("type == 'proceedings-paper'")
                proceedings_paper = proceedings_paper[["id","title","publication_year", "publication_venue", "publisher", "event"]]

                with connect(self.dbPath) as con:
                    journal_articles.to_sql("JournalArticles", con, if_exists="replace", index=False)
                    book_chapter.to_sql("BookChapter", con, if_exists="replace", index=False)
                    proceedings_paper.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)
                    con.commit()
                return True
            else:
                return False
        else:
            return False

class RelationalQueryProcessor(RelationalProcessor):
    def __init__(self, dbPath):
        super().__init__(dbPath)
    
    def getPublicationsPublishedInYear(self, year):
        with connect(self.dbPath) as con:  
            query = '''SELECT id, title, publication_year, orcid, "issn/isbn" FROM JournalArticles WHERE publication_year = '{0}' UNION  SELECT id, title, publication_year, orcid, "issn/isbn" FROM BookChapter WHERE publication_year ='{0}' UNION SELECT id, title, publication_year, orcid, "issn/isbn" FROM ProceedingsPaper WHERE publication_year ='{0}';'''.format(year)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsPublishedInYear: It returns a data frame with all the publications (i.e. the rows) that have been published in the input year (e.g. 2020).
    """

    def getPublicationsByAuthorId(self, id):
        with connect(self.dbPath) as con:  
            query = '''SELECT  DISTINCT id, title, publication_year, orcid, "issn/isbn" FROM JournalArticles  WHERE orcid like  "%'{0}'%" UNION SELECT  DISTINCT id, title, publication_year, orcid, "issn/isbn" FROM BookChapter  WHERE orcid like  "%'{0}'%" UNION SELECT  DISTINCT id, title, publication_year, orcid, "issn/isbn" FROM ProceedingsPaper  WHERE orcid like  "%'{0}'%";'''.format(id)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsByAuthorId: It returns a data frame with all the publications (i.e. the rows) that have been authored by the person having the identifier specified as input (e.g. "0000-0001-9857-1511").
    """

    def getMostCitedPublication(self):
        with connect(self.dbPath) as con:  
            query = """SELECT "doi mention", COUNT("doi mention") AS `value_occurrence` FROM Citations GROUP BY "doi mention" ORDER BY `value_occurrence` DESC LIMIT 1;"""
            freqMat = read_sql(query, con)
            freqMat = freqMat["doi mention"]
            output = DataFrame()
            for label, content in freqMat.iteritems():
                query = """SELECT id, title, publication_year, orcid, "issn/isbn" FROM JournalArticles WHERE id = "{0}" UNION SELECT id, title, publication_year, orcid, "issn/isbn" FROM BookChapter WHERE id = "{0}" UNION SELECT id, title, publication_year, orcid, "issn/isbn" FROM ProceedingsPaper WHERE id = "{0}";""".format(content)
                df_sql = read_sql(query, con)
                output = pd.concat([output, df_sql])
            return output

    """
    getMostCitedPublication: It returns a data frame with all the publications (i.e. the rows) that have received the most number of citations by other publications.
    """

    def getMostCitedVenue(self):
        mostCitVen = getMostCitedPublication()["issn/isbn"]
        output = DataFrame()
        with connect("data/publicationsRelTest.db") as con: 
            for label, content in mostCitVen.iteritems():
                query = """SELECT * FROM Journal WHERE "issn/isbn" = "{0}" UNION SELECT * FROM Book WHERE "issn/isbn" = "{0}" UNION SELECT publication_venue, "issn/isbn", "publisher" FROM Proceedings WHERE "issn/isbn" = "{0}";""".format(content)
                df_sql = read_sql(query, con)
                output = pd.concat([output, df_sql])
            return output

    """
    getMostCitedVenue: It returns a data frame with all the venues (i.e. the rows) containing the publications that, overall, have received the most number of citations by other publications.
    """

    def getVenuesByPublisherId(self, id):
        with connect(self.dbPath) as con:
            query = '''SELECT   "issn/isbn", publication_venue  FROM Journal WHERE publisher="{0}" UNION SELECT   "issn/isbn", publication_venue  FROM Book WHERE publisher="{0}" UNION  SELECT "issn/isbn", publication_venue  FROM Proceedings WHERE publisher="{0}";'''.format(id)
            df_sql = read_sql(query, con)
            return df_sql
    
    """
    getVenuesByPublisherId: It returns a data frame with all the venues (i.e. the rows) that have been published by the organisation having the identifier specified as input (e.g. "crossref:78").
    """

    def getPublicationInVenue(self, venueId):
        with connect(self.dbPath) as con:
            query = '''SELECT  DISTINCT id, title, publication_year, orcid, "issn/isbn" FROM JournalArticles WHERE "issn/isbn" like  "%'{0}'%"  UNION SELECT  DISTINCT id, title, publication_year, orcid, "issn/isbn" FROM BookChapter WHERE "issn/isbn" like  "%'{0}'%"  UNION SELECT  DISTINCT id, title, publication_year, orcid, "issn/isbn" FROM ProceedingsPaper WHERE "issn/isbn" like  "%'{0}'%" ;'''.format(venueId)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationInVenue: It returns a data frame with all the publications (i.e. the rows) that have been included in the venue having the identifier specified as input (e.g. "issn:0944-1344").
    """

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        with connect(self.dbPath) as con:
            query = '''SELECT id, title, publication_year, orcid, "issn/isbn" FROM JournalArticles WHERE issue = '{0}' AND volume = '{1}' AND "issn/isbn" like  "%'{2}'%";'''.format(issue, volume, journalId)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getJournalArticlesInIssue: It returns a data frame with all the journal articles (i.e. the rows) that have been included in the input issue (e.g. "9") of the input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getJournalArticlesInVolume(self, volume, journalId):
        with connect(self.dbPath) as con:
            query = '''SELECT id, title, publication_year, orcid, "issn/isbn" FROM JournalArticles WHERE volume = '{0}' AND "issn/isbn" like  "%'{1}'%";'''.format(volume, journalId)
            df_sql = read_sql(query, con)
            return df_sql
        
    """
    getJournalArticlesInVolume: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue, in input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getJournalArticlesInJournal(self, journalId):
        with connect(self.dbPath) as con:
            query = '''SELECT id, title, publication_year, orcid, "issn/isbn" FROM JournalArticles WHERE "issn/isbn" like  "%'{0}'%";'''.format(journalId)
            df_sql = read_sql(query, con)
            return df_sql
        
    """
    getJournalArticlesInJournal: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue and the volume, in the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getProceedingsByEvent(self, eventPartialName):
        with connect(self.dbPath) as con:
            query = '''SELECT  publication_venue, "issn/isbn", publisher, event FROM Proceedings WHERE event COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%';'''.format(eventPartialName)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getProceedingsByEvent: It returns a data frame with all the proceedings (i.e. the rows) that refer to the events that match (in lowercase), even partially, with the name specified as input (e.g. "web").
    """

    def getPublicationAuthors(self, publicationId):
        with connect(self.dbPath) as con:
            query = '''SELECT orcid FROM JournalArticles WHERE id = '{0}' UNION  SELECT orcid FROM BookChapter WHERE id = '{0}' UNION SELECT orcid FROM ProceedingsPaper WHERE id = '{0}';'''.format(publicationId)
            coauthors = read_sql(query, con)
            output = DataFrame()
            for label, content in coauthors.iteritems():
                for el in content:
                    authorIds = el
                    listOrcid = authorIds.strip('}{').split(', ')
            for item in listOrcid:
                query = "SELECT DISTINCT* FROM Author WHERE orcid = {0}".format(item)
                autInfo = read_sql(query, con)
                output = pd.concat([output, autInfo])
            return output

    """
    getPublicationAuthors: It returns a data frame with all the authors (i.e. the rows) of the publication with the identifier specified as input (e.g. "doi:10.1080/21645515.2021.1910000").
    """

    def getPublicationsByAuthorName(self, authorPartialName):
        with connect(self.dbPath) as con: 
            query = "SELECT DISTINCT orcid FROM Author WHERE family COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%' OR given COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%';".format(authorPartialName)
            authorId = read_sql(query, con)
            output = DataFrame()
            for label, content in authorId.iteritems():
                for el in content:    
                    query = '''SELECT id, title, publication_year, orcid, "issn/isbn" FROM JournalArticles  WHERE orcid like "%{0}%" UNION SELECT id, title, publication_year, orcid, "issn/isbn" FROM BookChapter  WHERE orcid like  "%{0}%" UNION SELECT id, title, publication_year, orcid, "issn/isbn" FROM ProceedingsPaper  WHERE orcid like  "%{0}%";'''.format(el)
                    df_sql = read_sql(query, con)
                    output = pd.concat([output, df_sql])
            return output

    """
    getPublicationsByAuthorName: It returns a data frame with all the publications (i.e. the rows) that have been authored by the people having their name matching (in lowercase), even partially, with the name specified as input (e.g. "doe").
    """

    def getDistinctPublisherOfPublications(self, pubIdList):
        with connect(self.dbPath) as con:
            publisherId = DataFrame(); output = DataFrame()
            for el in pubIdList:
                query = '''SELECT Journal.publisher FROM  JournalArticles LEFT JOIN Journal ON JournalArticles."issn/isbn" == Journal."issn/isbn" WHERE JournalArticles.id = "{0}" UNION SELECT Book.publisher FROM  BookChapter LEFT JOIN Book ON BookChapter."issn/isbn" == Book."issn/isbn" WHERE BookChapter.id = "{0}" UNION SELECT Proceedings.publisher FROM  ProceedingsPaper LEFT JOIN Proceedings ON ProceedingsPaper."issn/isbn" == Proceedings."issn/isbn" WHERE ProceedingsPaper.id = "{0}";'''.format(el)
                df_sql = read_sql(query, con)
                publisherId = pd.concat([publisherId, df_sql])
            for label, content in publisherId.iteritems():
                for el in content:  
                    query ="""SELECT * FROM Publisher WHERE id = '{0}'""".format(el)
                    df_sql = read_sql(query, con)
                    output = pd.concat([output, df_sql])
            return output

    """
    getDistinctPublisherOfPublications: It returns a data frame with all the distinct publishers (i.e. the rows) that have published the venues of the publications with identifiers those specified as input (e.g. [ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]).
    """

rel_path = "vAMOSotraVez.db"
rel_dp = RelationalDataProcessor(rel_path)
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("data/relational_publications.csv")
rel_dp.uploadData("data/relationalJSON.json")
print(rel_dp)
"""
rel_qp = RelationalQueryProcessor(rel_path)
rel_qp.setDbPath(rel_path)
question = rel_qp.getPublicationsByAuthorId("0000-0001-9857-1511")
print(question)
"""