from json import load
from sqlite3 import connect
from black import out
from pandas import read_sql, DataFrame
import pandas as pd

def testUpload(format):
    if format == "json": 
        with open("data/relationalJSON.json", "r", encoding="utf-8") as f:
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

        with connect("data/publicationsRelTest2.db") as con:
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

        with connect("data/publicationsRelTest2.db") as con:
            journal_articles.to_sql("JournalArticles", con, if_exists="replace", index=False)
            book_chapter.to_sql("BookChapter", con, if_exists="replace", index=False)
            proceedings_paper.to_sql("ProceedingsPaper", con, if_exists="replace", index=False)
            con.commit()
        return True
    return "Check again file type"

print(testUpload("json"))
print(testUpload("csv"))

"""
authorDf.to_csv(r'/Users/manuele/Desktop/export/authorDf.csv')
publisherDf.to_csv(r'/Users/manuele/Desktop/export/publisherDf.csv')
journal_articles.to_csv(r'/Users/manuele/Desktop/export/journal_articles.csv')
book_chapter.to_csv(r'/Users/manuele/Desktop/export/book_chapter.csv')
proceedings_paper.to_csv(r'/Users/manuele/Desktop/export/proceedings_paper.csv')
journal.to_csv(r'/Users/manuele/Desktop/export/journal.csv')
book.to_csv(r'/Users/manuele/Desktop/export/book.csv')
proceedings.to_csv(r'/Users/manuele/Desktop/export/proceedings.csv')
refDf.to_csv(r'/Users/manuele/Desktop/export/citations.csv')


def createDB():
    with connect("data/publicationsRelTest2.db") as con:
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
temp = createDB()
"""