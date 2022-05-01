import csv
from json import load
from sqlite3 import connect
from csv import reader
import pandas as pd
import sqlite3
from pprint import pprint

with open("TempRelational/relationalJSON.json", "r", encoding="utf-8") as f:
    jsonData = load(f)

csvData = pd.read_csv("TempRelational/relational_publications.csv")

# ========== AUTHOR =======================
authors = jsonData['authors']
datum = []
for doi in authors:
    data_row = authors[doi]
    for row in data_row:
        datum.append(row)
authorDf = pd.DataFrame(datum)
 
# ========== PUBLISHERS ===================
publishers = jsonData['publishers']
datum = []
for cross_ref in publishers:
    datum.append([publishers[cross_ref]['id'], publishers[cross_ref]['name']])
publisherDf = pd.DataFrame(datum, columns=['id','name'])

# ========= JOURNAL ARTICLE ==============
journal_articles = csvData.query("type == 'journal-article'")
journal_articles.drop(["type", "chapter", "venue_type", "publisher", "event"], axis = 1, inplace=True)

# Add venuesId
venues = jsonData['venues_id']
datum = []
for doi in venues:
    datum.append([doi, venues[doi]])
venuePub = pd.DataFrame(datum, columns=["doi","issn/isbn"])
journal_articles = journal_articles.merge(venuePub, how='left', left_on="id", right_on="doi") 
journal_articles.drop(["doi", "publication_venue"], axis = 1, inplace=True)

#Add authors
authors = jsonData['authors']
datum = []
for doi in authors:
    setOrcid = set()
    for item in authors[doi]:
        setOrcid.add(item['orcid'])
    datum.append([doi, setOrcid])   
autOfPub = pd.DataFrame(datum, columns=["doi","orcid"])
journal_articles = journal_articles.merge(autOfPub, how='left', left_on="id", right_on="doi") 
journal_articles.drop(["doi"], axis = 1, inplace=True) #ALL THE QUERY: id included 

#Add citation
references = jsonData['references']
datum = []
for doi in references:
    datum.append([doi,references[doi]])
citeDf = pd.DataFrame(datum, columns=["doi","cite"])
journal_articles = journal_articles.merge(citeDf, how='left', left_on="id", right_on="doi") 
journal_articles.drop(["doi"], axis = 1, inplace=True)
journal_articles.to_csv(r'/Users/manuele/Desktop/journal_articles.csv')

# ========= BOOK CHAPTER ============== 
book_chapter = csvData.query("type == 'book-chapter'")
book_chapter.drop(["type", "issue", "volume", "venue_type", "publisher", "event"], axis = 1, inplace=True)

# Add venuesId
venues = jsonData['venues_id']
datum = []
for doi in venues:
    datum.append([doi, venues[doi]])
venuePub = pd.DataFrame(datum, columns=["doi","issn/isbn"])
book_chapter = book_chapter.merge(venuePub, how='left', left_on="id", right_on="doi") 
book_chapter.drop(["doi", "publication_venue"], axis = 1, inplace=True)

#Add authors
authors = jsonData['authors']
datum = []
for doi in authors:
    setOrcid = set()
    for item in authors[doi]:
        setOrcid.add(item['orcid'])
    datum.append([doi, setOrcid])    
autOfPub = pd.DataFrame(datum, columns=["doi","orcid"])
book_chapter = book_chapter.merge(autOfPub, how='left', left_on="id", right_on="doi") 
book_chapter.drop(["doi"], axis = 1, inplace=True) #ALL THE QUERY: id included 

#Add citation
references = jsonData['references']
datum = []
for doi in references:
    datum.append([doi,references[doi]])
citeDf = pd.DataFrame(datum, columns=["doi","cite"])
book_chapter = book_chapter.merge(citeDf, how='left', left_on="id", right_on="doi") 
book_chapter.drop(["doi"], axis = 1, inplace=True)

# ========= PROCEEDINGS-PAPER ============== 
proceedings_paper = csvData.query("type == 'proceedings-paper'")
proceedings_paper.drop(["type", "issue", "volume", "chapter", "venue_type", "publisher", "event"], axis = 1, inplace=True)

# Add venuesId
venues = jsonData['venues_id']
datum = []
for doi in venues:
    datum.append([doi, venues[doi]])
venuePub = pd.DataFrame(datum, columns=["doi","issn/isbn"])
proceedings_paper = proceedings_paper.merge(venuePub, how='left', left_on="id", right_on="doi") 
proceedings_paper.drop(["doi", "publication_venue"], axis = 1, inplace=True)

#Add authors
authors = jsonData['authors']
datum = []
for doi in authors:
    setOrcid = set()
    for item in authors[doi]:
        setOrcid.add(item['orcid'])
    datum.append([doi, setOrcid])      
autOfPub = pd.DataFrame(datum, columns=["doi","orcid"])
proceedings_paper = proceedings_paper.merge(autOfPub, how='left', left_on="id", right_on="doi") 
proceedings_paper.drop(["doi"], axis = 1, inplace=True) #ALL THE QUERY: id included 

#Add citation
references = jsonData['references']
datum = []
for doi in references:
    datum.append([doi,references[doi]])
citeDf = pd.DataFrame(datum, columns=["doi","cite"])
proceedings_paper = proceedings_paper.merge(citeDf, how='left', left_on="id", right_on="doi") 
proceedings_paper.drop(["doi"], axis = 1, inplace=True)

# ============= JOURNAL ===========
journal = csvData.query("venue_type == 'journal'")
journal.drop(["title","type","publication_year","issue","volume","chapter","venue_type", "event"], axis = 1, inplace=True)

journal = journal.merge(venuePub, how='left', left_on="id", right_on="doi") # Add venuesId
journal.drop(["id","doi"], axis = 1, inplace=True)

# ============= BOOK ===========
book = csvData.query("venue_type == 'book'")
book.drop(["title","type","publication_year","issue","volume","chapter","venue_type", "event"], axis = 1, inplace=True)

book = book.merge(venuePub, how='left', left_on="id", right_on="doi") # Add venuesId
book.drop(["id","doi"], axis = 1, inplace=True)

# ============= PROCEEDINGS ===========
proceedings = csvData.query("venue_type == 'proceedings'")
proceedings.drop(["title","type","publication_year","issue","volume","chapter","venue_type"], axis = 1, inplace=True)

proceedings = proceedings.merge(venuePub, how='left', left_on="id", right_on="doi") # Add venuesId
proceedings.drop(["id","doi"], axis = 1, inplace=True)

# ========== REFERENCES ===================
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
refDf = df2.join(df1)

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
"""

def createDB():
    with sqlite3.connect("TempRelational/publications.db") as con:
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

