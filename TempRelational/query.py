from commitDB import *
from pandas import read_sql
from sqlite3 import connect

database = createDB()

print("=== General === \n", csvDf.info)
print("=== Author === \n", author_pubDf.info)
print("=== Venues === \n", venueDf.info)
print("=== References === \n", refDf.info)
print("=== Publishers === \n", publisherDf.info)


def getPublicationsPublishedInYear(year):
    with connect("publications.db") as con:  
        query = "SELECT * FROM General WHERE publication_year = '{0}'".format(year)
        df_sql = read_sql(query, con)
        return df_sql

def getVenuesByPublisherId(id):
    with connect("publications.db") as con:
        query = "SELECT * FROM General WHERE publisher = '{0}'".format(id) 
        df_sql = read_sql(query, con)
        return df_sql

print("1° query \n", getPublicationsPublishedInYear("2020"))
print("2° query \n", getVenuesByPublisherId("crossref:78"))