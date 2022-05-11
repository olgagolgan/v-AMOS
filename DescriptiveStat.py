from relationalProcessorClasses import *
from triplestoreProcessorClasses import *
from genericQueryProcessor import *
from pandas import concat

def getStatByAuthorId(AuthorID):
        graph_df = trp_qp.getPublicationsByAuthorId(AuthorID)
        rel_df = rel_qp.getPublicationsByAuthorId(AuthorID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        median = df_union_no_dupl["publication_year"].median()
        mean = df_union_no_dupl["publication_year"].mean()
        most_recent = df_union_no_dupl["publication_year"].max()
        less_recent = df_union_no_dupl["publication_year"].min()

        return 'The median of the publication year of the author with id ' + str(AuthorID) + ' is: ' + str(
                median) + '. The mean is: ' + str(mean) + '. The most recent year of publication is: ' + str(
                most_recent) + ' and the less recent is: ' + str(less_recent)

def getPublicationsStatInYear(year):
        graph_year = trp_qp.getPublicationsPublishedInYear(year)
        rel_year = rel_qp.getPublicationsPublishedInYear(year)  
        df_union = concat([graph_year, rel_year], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        pub_number = df_union_no_dupl["title"].value_counts()

        return "Title of publications and number of times they appear in data related to the year " + str(year), pub_number

def getBestVenuesInYear(year):
        graph_year = trp_qp.getPublicationsPublishedInYear(year)
        rel_year = rel_qp.getPublicationsPublishedInYear(year)
        df_union = concat([graph_year, rel_year], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        final_df = df_union_no_dupl["publication_venue"].value_counts()[:3]

        return "Top three venues with related number of publications in year " + str(year), final_df


def getBestAuthorsInYear(year):
        graph_year = trp_qp.getPublicationsPublishedInYear(year)
        rel_year = rel_qp.getPublicationsPublishedInYear(year)
        df_union = concat([graph_year, rel_year], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        final_df = df_union_no_dupl["family"].value_counts()[:3]

        return "Top three authors (surname) with related number of publications in year " + str(year), final_df


## STATISTICS ##

# my_m12 = getStatByAuthorId("0000-0001-9857-1511")
# print(my_m12)

# my_m13 = getPublicationsStatInYear(2020)
# print("The output of the method getPublicationsStatInYear with 2020 as input will be:")
# print(my_m13)
# print("-------")

# my_m14 = getBestAuthorsInYear(2020)
# print("The output of the method getBestAuthorsInYear with 2020 as input will be:")
# print(my_m14)
# print("-------")

# my_m15 = getBestVenuesInYear(2020)
# print("The output of the method getBestVenuesInYear with 2020 as input will be:")
# print(my_m15)
