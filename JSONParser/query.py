from parser import *
from pandas import read_sql
import sqlite3

print("=== General === \n", csvDf.info)
print("=== Author === \n", author_pubDf.info)
print("=== Venues === \n", venueDf.info)
print("=== References === \n", refDf.info)
print("=== Publishers === \n", publisherDf.info)

"""
def getPublicationsPublishedInYear(year):
    with connect("publications.db") as con:  
        query = "SELECT * FROM General WHERE publication_year = '{0}'".format(year)
        df_sql = read_sql(query, con)
        return df_sql

print(getPublicationsPublishedInYear("2020"))
"""