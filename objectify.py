from distutils.log import info
from pandas import DataFrame
from relationalProcessorClasses import *

def PubToAuthor(doi): #doi:10.1162/qss_a_00023
    with connect(rel_path) as con:
        query = "SELECT info FROM AuthorAndPublication WHERE doi = '{0}'".format(doi)
        df_sql = read_sql(query, con)
        return df_sql

sampleDf = rel_qp.getPublicationsPublishedInYear(2020)
output = DataFrame()
for label, content in sampleDf["doi"].iteritems():
    authorInfo = PubToAuthor(content)
    output = pd.concat([output, authorInfo])
for label, content in output["info"].iteritems():
    print(content)