from json import load
import pandas as pd
import sqlite3

with open("JSONParser/relationalJSON.json", "r", encoding="utf-8") as f:
    json_doc = load(f)

# ========== GENERAL =======
csvDf = pd.read_csv("JSONParser/relational_publications.csv")

# ========== AUTHOR and PUBLICATION =======
authors = json_doc['authors']
rows_author = []
rows_first = []
for doi in authors:
    data_row = authors[doi]
    for row in data_row:
        rows_author.append(row)
    for id in range(len(authors[doi])):
        row = [doi, id+1]
        rows_first.append(row)
df1 = pd.DataFrame(rows_author)
df2 = pd.DataFrame(rows_first); df2.columns = ["doi", "coauthor no."]
author_pubDf = df2.join(df1)

""" 
EXPORT & VISUALIZATION: 
author_pubDf.to_csv(r'author_pubDf.csv')
print(author_pubDf)

=== only author (no doi) ===
authorDf.to_csv(r'authorDf.csv')
authorDf = df1.drop_duplicates(keep='first')
print(authorDf)
"""

# ========== VENUES =======================  
venues = json_doc['venues_id']
rows_ven = []
rows_first = []
for doi in venues:
    data_row = venues[doi]
    #**** non è molto ma è un lavoro onesto, rr. 40-41
    if len(data_row) != 2: 
        data_row.append("NaN") 
    for row in data_row: 
        rows_ven.append(row)
    for id in range(len(venues[doi])):
        row = [doi, id]
        rows_first.append(row)
df1 = pd.DataFrame(rows_ven); df1.columns = ["id"]
df2 = pd.DataFrame(rows_first); df2.columns = ["doi", "id no."]
venueDf = df2.join(df1)

""" 
EXPORT & VISUALIZATION: 
venueDf.to_csv(r'venueDf.csv')
print(venueDf)
"""

# ========== REFERENCES ===================
references = json_doc['references']
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
EXPORT & VISUALIZATION: 
refDf.to_csv(r'refDf.csv')
print(refDf)
"""

# ========== PUBLISHERS ===================
publishers = json_doc['publishers']
rowsID = []
rowsName = []
for cross_ref in publishers:
    #**** non è molto ma è un lavoro onesto, rr. 85-87
    data_row = publishers[cross_ref]
    rowsID.append(data_row["id"])
    rowsName.append(data_row["name"])
data_tuples = list(zip(rowsID,rowsName))
publisherDf = pd.DataFrame(data_tuples, columns=['id','name'])

"""
EXPORT & VISUALIZATION: 
refDf.to_csv(r'refDf.csv')
print(refDf)
"""

def createDB():
    with sqlite3.connect("publications.db") as con:
        author_pubDf.to_sql("Author", con, if_exists="replace", index=False)
        csvDf.to_sql("General", con, if_exists="replace", index=False)
        venueDf.to_sql("Venues", con, if_exists="replace", index=False)
        refDf.to_sql("References", con, if_exists="replace", index=False)
        publisherDf.to_sql("Publishers", con, if_exists="replace", index=False)
        con.commit

temp = createDB()