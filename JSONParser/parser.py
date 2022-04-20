from json import load
from csv import reader
import pandas as pd

with open("relational_publications.csv", "r", encoding="utf-8") as f:
    publications = reader(f)

with open("relationalJSON.json", "r", encoding="utf-8") as f:
    json_doc = load(f)

# ========== AUTHOR and PUBLICATION =======
authors = json_doc['authors']
rows_author = []
rows_first = []
for doi in authors:
    data_row = authors[doi]
    for row in data_row:
        rows_author.append(row)
    for id in range(len(authors[doi])):
        row = [doi, id]
        rows_first.append(row)
df1 = pd.DataFrame(rows_author)
df2 = pd.DataFrame(rows_first); df2.columns = ["doi", "coauthor no."]
author_pubDB = df2.join(df1)

""" 
EXPORT & VISUALIZATION: 
author_pubDB.to_csv(r'author_pubDB.csv')
print(author_pubDB)

=== only author (no doi) ===
authorDB.to_csv(r'authorDB.csv')
authorDB = df1.drop_duplicates(keep='first')
print(authorDB)
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
venueDB = df2.join(df1)

""" 
EXPORT & VISUALIZATION: 
venueDB.to_csv(r'venueDB.csv')
print(venueDB)
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
refDB = df2.join(df1)

"""
EXPORT & VISUALIZATION: 
refDB.to_csv(r'refDB.csv')
print(refDB)
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
refDB = pd.DataFrame(data_tuples, columns=['id','name'])

"""
EXPORT & VISUALIZATION: 
refDB.to_csv(r'refDB.csv')
print(refDB)
"""