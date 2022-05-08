from relationalProcessorClasses import *
from triplestoreProcessorClasses import *
from pandas import concat


## METHODS FOR DESCRIPTIVE STATISTICS PURPOSES ##

def getPublicationsPublishedInYear(year):
        graph_year = trp_qp.getPublicationsPublishedInYear(year)
        rel_year = rel_qp.getPublicationsPublishedInYear(year)  
        df_union = concat([graph_year, rel_year], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getPublicationsByAuthorId(AuthorID):
        graph_df = trp_qp.getPublicationsByAuthorId(AuthorID)
        rel_df = rel_qp.getPublicationsByAuthorId(AuthorID)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getMostCitedPublication():
        graph_df = trp_qp.getMostCitedPublication()
        rel_df = rel_qp.getMostCitedPublication()  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getVenuesByPublisherId(publisherID):
        graph_df = trp_qp.getVenuesByPublisherId(publisherID)
        rel_df = rel_qp.getVenuesByPublisherId(publisherID) 
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getPublicationInVenue(venueID):
        graph_df = trp_qp.getPublicationInVenue(venueID)
        rel_df = rel_qp.getPublicationInVenue(venueID)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getJournalArticlesInIssue(self, issue, volume, journalID):
        graph_df = trp_qp.getJournalArticlesInIssue(issue, volume, journalID)
        rel_df = rel_qp.getJournalArticlesInIssue(issue, volume, journalID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getJournalArticlesInVolume(volume, journalID):
        graph_df = trp_qp.getJournalArticlesInVolume(volume, journalID)
        rel_df = rel_qp.getJournalArticlesInVolume(volume, journalID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getJournalArticlesInJournal(journalID):
        graph_df = trp_qp.getJournalArticlesInJournal(journalID)
        rel_df = rel_qp.getJournalArticlesInJournal(journalID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getMostCitedVenue():
        graph_df = trp_qp.getMostCitedVenue()
        rel_df = rel_qp.getMostCitedVenue() 
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getProceedingsByEvent(eventPartialName):
        graph_df = trp_qp.getProceedingsByEvent(eventPartialName)
        rel_df = rel_qp.getProceedingsByEvent(eventPartialName)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

def getPublicationAuthors(publicationID):
        graph_df = trp_qp.getPublicationAuthors(publicationID)
        rel_df = rel_qp.getPublicationAuthors(publicationID)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        final_df = df_union_no_dupl.sort_values("family")
        return final_df

def getPublicationsByAuthorName(partialAuthorName):
        graph_df = trp_qp.getPublicationsByAuthorName(partialAuthorName)
        rel_df = rel_qp.getPublicationsByAuthorName(partialAuthorName)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        final_df = df_union.drop_duplicates()
        return final_df

#################################

# my_df = getPublicationsByAuthorId("0000-0001-9773-4008")
# print('The median of the publication year filtered by an author id:')
# print(my_df["publication_year"].median())
# print('--------')

# my_df1 = getPublicationsPublishedInYear(2020)
# print('The titles' count of the publications published in a selected year:')
# print(my_df1["title"].value_counts())
# #my_df1.plot(kind="bar")
# print('--------')

# my_df2 = getPublicationsByAuthorId("0000-0003-0530-4305")
# print('The mode of the publication for a selected author is:')
# print(my_df2.mode(axis='columns', dropna=False))
# print('--------')

# my_df3 = getJournalArticlesInJournal("issn:1066-8888")
# print('The authors' count of the journal article of a specified journal :')
# print(my_df3["family"].value_counts())
# #my_df1.plot(kind="bar")