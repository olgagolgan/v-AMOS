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
            if  path.endswith(".json"):           
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
                        infoList = authors[doi]
                        #pprint(infoList)
                        setInfo = set()
                        setOrcid = set()
                        for dict in infoList:
                            personalData = []
                            setOrcid.add(dict["orcid"])
                            for key in dict:
                                personalData.append(dict[key])
                            setInfo.add(str(personalData))
                        datum.append([doi, str(setOrcid), str(setInfo)])
                    autOfPub = pd.DataFrame(datum, columns=["doi","orcid","info"])

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
                        autOfPub.to_sql("AuthorAndPublication", con, if_exists="replace", index=False)        
                        publisherDf.to_sql("Publisher", con, if_exists="replace", index=False)
                        bibliographydDf.to_sql("CitationsListed", con, if_exists="replace", index=False)
                        citeDf.to_sql("CitationsCondensed", con, if_exists="replace", index=False)
                        venuePub.to_sql("Venues", con, if_exists="replace", index=False)
                        con.commit()
                    return True
            if  path.endswith(".csv"):                
                csvData = pd.read_csv(path)

                # ========= JOURNAL ARTICLE ==============
                journal_articles = csvData.query("type == 'journal-article'")
                journal_articles = journal_articles[["id","title","publication_year","issue","volume", "publication_venue", "publisher"]]
                journal_articles.rename(columns = {'id':'doi'}, inplace = True)

                # ========= BOOK CHAPTER ============== 
                book_chapter = csvData.query("type == 'book-chapter'")
                book_chapter = book_chapter[["id","title","publication_year","chapter", "publication_venue", "publisher"]]
                book_chapter.rename(columns = {'id':'doi'}, inplace = True)

                # ========= PROCEEDINGS-PAPER ============== 
                proceedings_paper = csvData.query("type == 'proceedings-paper'")
                proceedings_paper = proceedings_paper[["id","title","publication_year", "publication_venue", "publisher", "event"]]
                proceedings_paper.rename(columns = {'id':'doi'}, inplace = True)

                with connect(self.dbPath) as con:
                    csvData.to_sql("GeneralPublication", con, if_exists="replace", index=False)
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
            query = """ SELECT JournalArticles.doi, title, publication_year, orcid, "issn/isbn", publisher 
            FROM JournalArticles 
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == JournalArticles.doi
            LEFT JOIN Venues ON Venues.doi == JournalArticles.doi
            WHERE publication_year = '{0}' 
            UNION  SELECT BookChapter.doi, title, publication_year, orcid, "issn/isbn", publisher 
            FROM BookChapter
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == BookChapter.doi
            LEFT JOIN Venues ON Venues.doi == BookChapter.doi
            WHERE publication_year ='{0}' 
            UNION SELECT ProceedingsPaper.doi, title, publication_year, orcid, "issn/isbn", publisher 
            FROM ProceedingsPaper
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == ProceedingsPaper.doi
            LEFT JOIN Venues ON Venues.doi == ProceedingsPaper.doi
            WHERE publication_year ='{0}'; """.format(year)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsPublishedInYear: It returns a data frame with all the publications (i.e. the rows) that have been published in the input year (e.g. 2020).
    """

    def getPublicationsByAuthorId(self, id):
        with connect(self.dbPath) as con:  
            query = """
            SELECT JournalArticles.doi, title, publication_year, orcid, "issn/isbn", publisher 
            FROM JournalArticles 
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == JournalArticles.doi
            LEFT JOIN Venues ON Venues.doi == JournalArticles.doi
            WHERE orcid like  '%{0}%'
            UNION  SELECT BookChapter.doi, title, publication_year, orcid, "issn/isbn", publisher 
            FROM BookChapter
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == BookChapter.doi
            LEFT JOIN Venues ON Venues.doi == BookChapter.doi
            WHERE orcid like  '%{0}%'
            UNION SELECT ProceedingsPaper.doi, title, publication_year, orcid, "issn/isbn", publisher 
            FROM ProceedingsPaper
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == ProceedingsPaper.doi
            LEFT JOIN Venues ON Venues.doi == ProceedingsPaper.doi
            WHERE orcid like  '%{0}%' """.format(id)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getPublicationsByAuthorId: It returns a data frame with all the publications (i.e. the rows) that have been authored by the person having the identifier specified as input (e.g. "0000-0001-9857-1511").
    """

    def getMostCitedPublication(self):
        with connect(self.dbPath) as con:  
            query = """
            SELECT CitationsListed."doi mention", COUNT(CitationsListed."doi mention") as MostCited
            FROM CitationsCondensed
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == CitationsCondensed.doi
            LEFT JOIN CitationsListed ON CitationsListed.doi == AuthorAndPublication.doi
            GROUP BY CitationsListed."doi mention"
            ORDER BY MostCited DESC
            LIMIT 1"""
            freqMat = read_sql(query, con)
            #freqMat = freqMat["doi mention"] With this instruction the method doesn't return the MostCited column
        return freqMat

    """
    getMostCitedPublication: It returns a data frame with all the publications (i.e. the rows) that have received the most number of citations by other publications.
    """

    def getMostCitedVenue(self): 
        mostCitPub = RelationalQueryProcessor.getMostCitedPublication(self)
        with connect(self.dbPath) as con: 
            for label, content in mostCitPub.iteritems(): #this works
                query = """ 
                SELECT Venues."issn/isbn", COUNT(CitationsListed."doi mention") as MostCited
                FROM BookChapter 
                LEFT JOIN AuthorAndPublication ON BookChapter.doi = AuthorAndPublication.doi
                LEFT JOIN Venues ON AuthorAndPublication.doi = Venues.doi
                LEFT JOIN CitationsListed ON Venues.doi = CitationsListed."doi mention"
                LEFT JOIN JournalArticles ON JournalArticles.doi = BookChapter.doi
                LEFT JOIN ProceedingsPaper ON ProceedingsPaper.doi = Venues.doi
                GROUP BY Venues."issn/isbn"
                ORDER BY MostCited DESC
                LIMIT 1              
                """
                df_sql = read_sql(query, con)
                return df_sql

    """
    getMostCitedVenue: It returns a data frame with all the venues (i.e. the rows) containing the publications that, overall, have received the most number of citations by other publications.
    """

    def getVenuesByPublisherId(self, id):
        with connect(self.dbPath) as con:
            query = """SELECT "issn/isbn", publication_venue, publisher 
            FROM JournalArticles
            LEFT JOIN Venues ON Venues.doi == JournalArticles.doi
            WHERE publisher="{0}" 
            UNION SELECT   "issn/isbn", publication_venue, publisher 
            FROM BookChapter 
            LEFT JOIN Venues ON Venues.doi == BookChapter.doi
            WHERE publisher="{0}" 
            UNION  SELECT  "issn/isbn", publication_venue, publisher  
            FROM ProceedingsPaper 
            LEFT JOIN Venues ON Venues.doi == ProceedingsPaper.doi
            WHERE publisher="{0}";""".format(id)
            df_sql = read_sql(query, con)
        return df_sql
    
    """
    getVenuesByPublisherId: It returns a data frame with all the venues (i.e. the rows) that have been published by the organisation having the identifier specified as input (e.g. "crossref:78").
    """

    def getPublicationInVenue(self, venueId):
        with connect(self.dbPath) as con:
            query = """ SELECT JournalArticles.doi, title, publication_year, orcid, "issn/isbn", publisher 
            FROM JournalArticles
            LEFT JOIN Venues ON Venues.doi == JournalArticles.doi
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == JournalArticles.doi
            WHERE "issn/isbn" like  "%'{0}'%"
            UNION SELECT   BookChapter.doi, title, publication_year, orcid, "issn/isbn", publisher 
            FROM BookChapter 
            LEFT JOIN Venues ON Venues.doi == BookChapter.doi
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == BookChapter.doi
            WHERE "issn/isbn" like "%'{0}'%"
            UNION  SELECT  ProceedingsPaper.doi, title, publication_year, orcid, "issn/isbn", publisher 
            FROM ProceedingsPaper 
            LEFT JOIN Venues ON Venues.doi == ProceedingsPaper.doi
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == ProceedingsPaper.doi
            WHERE "issn/isbn" like  "%'{0}'%";
            """.format(venueId)
            df_sql = read_sql(query, con)
        return df_sql

    """
    getPublicationInVenue: It returns a data frame with all the publications (i.e. the rows) that have been included in the venue having the identifier specified as input (e.g. "issn:0944-1344").
    """

    def getJournalArticlesInIssue(self, issue, volume, journalId):
        with connect(self.dbPath) as con:
            query = """ SELECT JournalArticles.doi, title, publication_year, issue, volume,  "issn/isbn", orcid,publisher 
            FROM JournalArticles
            LEFT JOIN Venues ON Venues.doi == JournalArticles.doi
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == JournalArticles.doi
            WHERE issue = '{0}' AND volume = '{1}' AND "issn/isbn" like  "%'{2}'%";
            """.format(issue, volume, journalId)
            df_sql = read_sql(query, con)
        return df_sql

    """
    getJournalArticlesInIssue: It returns a data frame with all the journal articles (i.e. the rows) that have been included in the input issue (e.g. "9") of the input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getJournalArticlesInVolume(self, volume, journalId):
        with connect(self.dbPath) as con:
            query = """ SELECT JournalArticles.doi, title, publication_year, issue, volume,  "issn/isbn", orcid,publisher 
            FROM JournalArticles
            LEFT JOIN Venues ON Venues.doi == JournalArticles.doi
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == JournalArticles.doi
            WHERE volume = '{0}' AND "issn/isbn" like  "%'{1}'%";""".format(volume, journalId)
            df_sql = read_sql(query, con)
            return df_sql
        
    """
    getJournalArticlesInVolume: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue, in input volume (e.g. "17") of the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getJournalArticlesInJournal(self, journalId):
        with connect(self.dbPath) as con:
            query = """ SELECT JournalArticles.doi, title, publication_year, issue, volume,  "issn/isbn", orcid,publisher 
            FROM JournalArticles
            LEFT JOIN Venues ON Venues.doi == JournalArticles.doi
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == JournalArticles.doi
            WHERE "issn/isbn" like  "%'{0}'%";""".format(journalId)            
            df_sql = read_sql(query, con)
        return df_sql
        
    """
    getJournalArticlesInJournal: It returns a data frame with all the journal articles (i.e. the rows) that have been included, independently from the issue and the volume, in the journal having the identifier specified as input (e.g. "issn:2164-5515").
    """

    def getProceedingsByEvent(self, eventPartialName):
        with connect(self.dbPath) as con:
            query = """
            SELECT  publication_venue, "issn/isbn", publisher, event 
            FROM ProceedingsPaper
            LEFT JOIN Venues ON ProceedingsPaper.doi == Venues.doi 
            WHERE event COLLATE SQL_Latin1_General_CP1_CI_AS LIKE '%{0}%';""".format(eventPartialName)
            df_sql = read_sql(query, con)
            return df_sql

    """
    getProceedingsByEvent: It returns a data frame with all the proceedings (i.e. the rows) that refer to the events that match (in lowercase), even partially, with the name specified as input (e.g. "web").
    """

    def getPublicationAuthors(self, publicationId):
        with connect(self.dbPath) as con:
            query = """SELECT orcid
            FROM JournalArticles
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == JournalArticles.doi
            WHERE AuthorAndPublication.doi="{0}"
            UNION SELECT   orcid
            FROM BookChapter 
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == BookChapter.doi
            WHERE AuthorAndPublication.doi="{0}"
            UNION  SELECT  orcid 
            FROM ProceedingsPaper 
            LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == ProceedingsPaper.doi
            WHERE AuthorAndPublication.doi="{0}";""".format(publicationId)
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
                for el in content:  # WHY DUPLICATES?? 
                    query = """SELECT DISTINCT JournalArticles.doi, title, publication_year, orcid, "issn/isbn", publisher 
                    FROM JournalArticles
                    LEFT JOIN Venues ON Venues.doi == JournalArticles.doi
                    LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == JournalArticles.doi
                    WHERE orcid like  "%'{0}'%"
                    UNION SELECT  DISTINCT BookChapter.doi, title, publication_year, orcid, "issn/isbn", publisher 
                    FROM BookChapter 
                    LEFT JOIN Venues ON Venues.doi == BookChapter.doi
                    LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == BookChapter.doi
                    WHERE orcid like  "%'{0}'%"
                    UNION  SELECT DISTINCT ProceedingsPaper.doi, title, publication_year, orcid, "issn/isbn", publisher 
                    FROM ProceedingsPaper 
                    LEFT JOIN Venues ON Venues.doi == ProceedingsPaper.doi
                    LEFT JOIN AuthorAndPublication ON AuthorAndPublication.doi == ProceedingsPaper.doi
                    WHERE orcid like  "%'{0}'%";""".format(el)
                    
                    #'''SELECT id, title, publication_year, orcid, "issn/isbn" FROM JournalArticles  WHERE orcid like "%{0}%" UNION SELECT id, title, publication_year, orcid, "issn/isbn" FROM BookChapter  WHERE orcid like  "%{0}%" UNION SELECT id, title, publication_year, orcid, "issn/isbn" FROM ProceedingsPaper  WHERE orcid like  "%{0}%";'''.format(el)
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
                query = """SELECT JournalArticles.publisher
                FROM JournalArticles
                WHERE doi == "{0}"
                UNION SELECT BookChapter.publisher
                FROM BookChapter
                WHERE doi == "{0}"
                UNION SELECT ProceedingsPaper.publisher
                FROM ProceedingsPaper
                WHERE doi == "{0}";""".format(el)
                df_sql = read_sql(query, con) #doi:10.1080/21645515.2021.1910000
                publisherId = pd.concat([publisherId, df_sql])
            for label, content in publisherId.iteritems():
                for el in content:   
                    query ="""SELECT * FROM Publisher WHERE id = '{0}'""".format(el)
                    df_sql = read_sql(query, con)
                    output = pd.concat([output, df_sql])
            output["doi"] = pubIdList
            return output

    """
    getDistinctPublisherOfPublications: It returns a data frame with all the distinct publishers (i.e. the rows) that have published the venues of the publications with identifiers those specified as input (e.g. [ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]).
    """

rel_path = "vAMOSotraVez.db"
rel_dp = RelationalDataProcessor(rel_path)
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("data/relational_publications.csv")
rel_dp.uploadData("data/relationalJSON.json")
rel_qp = RelationalQueryProcessor(rel_path)
rel_qp.setDbPath(rel_path)
"""
print("1) getPublicationsPublishedInYear:\n",rel_qp.getPublicationsPublishedInYear(2020))
print("-----------------")
print("2) getPublicationsByAuthorId:\n",rel_qp.getPublicationsByAuthorId("0000-0001-9857-1511"))
print("-----------------")
print("3) getMostCitedPublication:\n", rel_qp.getMostCitedPublication())
print("-----------------")
print("4) getMostCitedVenue:\n", rel_qp.getMostCitedVenue())
print("-----------------")
print("5) getVenuesByPublisherId:\n", rel_qp.getVenuesByPublisherId("crossref:78"))
print("-----------------")
print("6) getPublicationInVenue:\n", rel_qp.getPublicationInVenue("issn:0944-1344"))
print("-----------------")
print("7) getJournalArticlesInIssue:\n", rel_qp.getJournalArticlesInIssue(9, 17, "issn:2164-5515"))
print("-----------------")
print("8) getJournalArticlesInVolume:\n", rel_qp.getJournalArticlesInVolume(17, "issn:2164-5515"))
print("-----------------")
print("9) getJournalArticlesInJournal:\n", rel_qp.getJournalArticlesInJournal("issn:2164-5515"))
print("-----------------")
print("10) getProceedingsByEvent:\n", rel_qp.getProceedingsByEvent("web"))
print("-----------------")
print("11) getPublicationAuthors:\n", rel_qp.getPublicationAuthors("doi:10.1080/21645515.2021.1910000"))
print("-----------------")
print("12) getPublicationsByAuthorName:\n", rel_qp.getPublicationsByAuthorName("iv"))
print("-----------------")
print("13) getDistinctPublisherOfPublications:\n", rel_qp.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ]))
"""