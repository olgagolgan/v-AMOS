from triplestoreProcessorClasses import *
from relationalProcessorClasses import *
from pandas import concat
from dataModelClasses import *

class GenericQueryProcessor:
    def __init__(self, queryProcessor):
        self.queryProcessor = list(queryProcessor)

    def cleanQueryProcessor(self):
        self.queryProcessor.clear()
        if len(self.queryProcessor) == 0:
            return True
        else:
            return False

    def addQueryProcessor(self, QueryProcessor):
        length = len(self.queryProcessor)
        self.queryProcessor.append(QueryProcessor)
        if len(self.queryProcessor) == (length + 1):
            return True
        else:
            return False

    def getPublicationsPublishedInYear(self, year):
        graph_year = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getPublicationsPublishedInYear(year)
        rel_year = RelationalQueryProcessor("publicationsRelTest.db").getPublicationsPublishedInYear(year)  # gp back to the correct format
        df_union = concat([graph_year, rel_year], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_union_sorted = df_union_no_dupl.sort_values("publicationYear")
        final_list = list()

        for row_idx, row in df_union_sorted.iterrows():
           identifier = row["id"]
           publicationYear = row["publication_year"]
           title = row["title"]
           publisher = row["publisher"]
           graph_au = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getPublicationAuthors(identifier)
           rel_au = RelationalQueryProcessor("publicationsRelTest.db").getPublicationAuthors(identifier)  # gp back to the correct format
           df_au = concat([graph_au, rel_au], ignore_index=True)
           df_au_no_dupl = df_au.drop_duplicates()
           authors_list = list()
           for row_idx, row in df_au_no_dupl.iterrows():
               orcid = row["orcid"]
               givenName = row["given"]
               familyName = row["family"]
               author = Person(orcid, givenName, familyName)
               authors_list.append(author)
               

           
           pub = Publication(identifier, publicationYear, title, cited, authors_list, publicationVenue)
           final_list.append(pub)

        # return df_union_sorted

# def getPublicationsByAuthorId(self, year):
#         graph_year = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getPublicationsPublishedInYear(year)
#         rel_year = getPublicationsPublishedInYear(year)  # gp back to the correct format
#         df_union = concat([graph_year, rel_year], ignore_index=True)
#         df_union_no_dupl = df_union.drop_duplicates()
#         df_union_sorted = df_union_no_dupl.sort_values("publicationYear")  