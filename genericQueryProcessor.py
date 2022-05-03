from tripleStoreQueryProcessor import *
from pandas import concat
from dfCreator import *

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
        rel_year = getPublicationsPublishedInYear(year)  # gp back to the correct format
        df_union = concat([graph_year, rel_year], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_union_sorted = df_union_no_dupl.sort_values("publicationYear")
        print(df_union_sorted)
