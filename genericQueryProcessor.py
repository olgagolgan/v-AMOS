from triplestoreProcessorClasses import *
from relationalProcessorClasses import *
from pandas import concat
from dataModelClasses import *

class GenericQueryProcessor:
    def __init__(self, queryProcessor):
        self.queryProcessor = list(queryProcessor)

    def cleanQueryProcessors(self):
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
    
    #####################################
    #ADDITIONAL METHODS FOR A BETTER CODE

    def getCitation(identifier): 
        graph_cite = trp_qp.getCitedOfPublication(identifier)
        rel_cite = rel_qp.getCitedOfPublication(identifier)  
        df_cite = concat([graph_cite, rel_cite], ignore_index=True)
        df_cite_no_dupl = df_cite.drop_duplicates()
        cites_list = []
        for row_idx, row in rel_cite.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)           
            cited_pub = str(identifier) + '; publication year: ' + str(publicationYear) + '; title: ' + str(title) + '; authors: ' + str(authors) + '; publication venue: '  + str(publicationVenue) + '.'
            cites_list.append(cited_pub)
        return cites_list

    def getAuthors(identifier):
        graph_au = trp_qp.getPublicationAuthors(identifier)
        rel_au = rel_qp.getPublicationAuthors(identifier) 
        df_au = concat([graph_au, rel_au], ignore_index=True)
        df_au_no_dupl = df_au.drop_duplicates()
        authors = set()

        for row_idx, row in df_au_no_dupl.iterrows():
            orcid = row["orcid"]
            givenName = row["given"]
            familyName = row["family"]
            author = str(orcid) + ", " + str(givenName) + ", " + str(familyName)
            authors.add(author)

        return authors
    
    #######################################

    def getPublicationsPublishedInYear(self, year):
        graph_year = trp_qp.getPublicationsPublishedInYear(year)
        rel_year = rel_qp.getPublicationsPublishedInYear(year)  
        df_union = concat([graph_year, rel_year], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_union_sorted = df_union_no_dupl.sort_values("publication_year")
        pub_list = list()

        for row_idx, row in df_union_sorted.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)           
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            pub_list.append(pub.__str__())

        return '\n'.join(pub_list)

####################################################################

    def getPublicationsByAuthorId(self, AuthorID):
        graph_df = trp_qp.getPublicationsByAuthorId(AuthorID)
        rel_df = rel_qp.getPublicationsByAuthorId(AuthorID)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        pub_list = list()  

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)           
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)  
            pub_list.append(pub.__str__())

        return '\n'.join(pub_list)

####################################################################

    def getMostCitedPublication(self):
        graph_df = trp_qp.getMostCitedPublication()
        rel_df = rel_qp.getMostCitedPublication()  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)           
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue) 

        return pub.__str__()

####################################################################

    def getMostCitedVenue(self):
        graph_df = trp_qp.getMostCitedVenue()
        rel_df = rel_qp.getMostCitedVenue() 
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["venue_id"]
            title = row["title"]
            publisher = row["publisher"]

            venue = Venue(identifier, title, publisher)

        return venue.__str__()

####################################################################

    def getVenuesByPublisherId(self, publisherID):
        graph_df = trp_qp.getVenuesByPublisherId(publisherID)
        rel_df = rel_qp.getVenuesByPublisherId(publisherID) 
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        venues_list = list()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["venue_id"]
            title = row["title"]

            venue = Venue(identifier, title, publisherID)
            venues_list.append(venue.__str__())

        return '\n'.join(venues_list)        

####################################################################

    def getPublicationInVenue(self, venueID):
        graph_df = trp_qp.getPublicationInVenue(venueID)
        rel_df = rel_qp.getPublicationInVenue(venueID)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        pub_list = list()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)           
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            pub_list.append(pub.__str__())

        return '\n'.join(pub_list)

####################################################################

    def getJournalArticlesInIssue(self, issue, volume, journalID):
        graph_df = trp_qp.getJournalArticlesInIssue(issue, volume, journalID)
        rel_df = rel_qp.getJournalArticlesInIssue(issue, volume, journalID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        journal_list = list()

        for row_idx, row in df_union_no_dupl.iterrows():
            issue = row["issue"]
            volume = row["volume"]
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)           
            cites_list = GenericQueryProcessor.getCitation(identifier)
            journalArticle = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            journal_list.append(journalArticle.__str__())

        return '\n'.join(journal_list)

####################################################################

    def getJournalArticlesInVolume(self, volume, journalID):
        graph_df = trp_qp.getJournalArticlesInVolume(volume, journalID)
        rel_df = rel_qp.getJournalArticlesInVolume(volume, journalID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        journal_list = list()

        for row_idx, row in df_union_no_dupl.iterrows():
            volume = row["volume"]
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            issue = row["issue"]
            authors = GenericQueryProcessor.getAuthors(identifier)           
            cites_list = GenericQueryProcessor.getCitation(identifier)
            journalArticle = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            journal_list.append(journalArticle.__str__())

        return '\n'.join(journal_list)

####################################################################

    def getJournalArticlesInJournal(self, journalID):
        graph_df = trp_qp.getJournalArticlesInJournal(journalID)
        rel_df = rel_qp.getJournalArticlesInJournal(journalID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        journal_list = list()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            issue = row["issue"]
            volume = row["volume"]
            authors = GenericQueryProcessor.getAuthors(identifier)           
            cites_list = GenericQueryProcessor.getCitation(identifier)
            journalArticle = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            journal_list.append(journalArticle.__str__())

        return '\n'.join(journal_list)

####################################################################

    def getProceedingsByEvent(self, eventPartialName):
        graph_df = trp_qp.getProceedingsByEvent(eventPartialName)
        rel_df = rel_qp.getProceedingsByEvent(eventPartialName)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        proceedings_list = list()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["venue_id"]
            title = row["publication_venue"]
            publisher = row["publisher"]
            event = eventPartialName
            proceeding = Proceedings(identifier, title, publisher, event)
            proceedings_list.append(proceeding.__str__())

        return '\n'.join(proceedings_list)

####################################################################

    def getPublicationAuthors(self, publicationID):
        graph_df = trp_qp.getPublicationAuthors(publicationID)
        rel_df = rel_qp.getPublicationAuthors(publicationID)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_union_sorted = df_union_no_dupl.sort_values("family")
        authors = list()

        for row_idx, row in df_union_sorted.iterrows():
            orcid = row["orcid"]
            givenName = row["given"]
            familyName = row["family"]
            author = Person(orcid, givenName, familyName)
            authors.append(author.__str__())
            
        return '\n'.join(authors)

####################################################################

    def getPublicationsByAuthorName(self, partialAuthorName):
        graph_df = trp_qp.getPublicationsByAuthorName(partialAuthorName)
        rel_df = rel_qp.getPublicationsByAuthorName(partialAuthorName)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        pub_list = list()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)           
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            pub_list.append(pub.__str__())

        return '\n'.join(pub_list)

####################################################################

    def getDistinctPublisherOfPublications(self, listOfPublication):
        graph_df = trp_qp.getDistinctPublisherOfPublications(listOfPublication)
        rel_df = rel_qp.getDistinctPublisherOfPublications(listOfPublication)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        organizations_list = list()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["id"]
            publisher_name = row["name"]
            organization = Organization(identifier, publisher_name)
            organizations_list.append(organization.__str__())

        return '\n'.join(organizations_list)

generic = GenericQueryProcessor([rel_qp, trp_qp])     

#TESTER

#1st query
# my_m1 = generic.getPublicationsPublishedInYear(2020)
# print(my_m1)

#2nd query
# my_m2 = generic.getPublicationsByAuthorId("0000-0001-7553-6916")
# print(my_m2)

#3rd query
# my_m3 = generic.getMostCitedPublication()
# print(my_m3)

#4th query
# my_m4 = generic.getMostCitedVenue()
# print(my_m4)

#5th query
# my_m5 = generic.getVenuesByPublisherId("crossref:78")
# print(my_m5)

#6th query 
# my_m6 = generic.getPublicationInVenue("issn:0944-1344")
# print(my_m6)

#7th query
# my_m7 = generic.getJournalArticlesInIssue(3, 28, "issn:1066-8888")
# print(my_m7)

#8th query
# my_m8 = generic.getJournalArticlesInVolume(28, "issn:1066-8888")
# print(my_m8)

#9th query
# my_m9 = generic.getProceedingsByEvent("") 
# print(my_m9) 

#10th query
# my_m10 = generic.getPublicationAuthors("doi:10.1007/s11192-019-03311-9")
# print(my_m10)

#11th query
# my_m11 = generic.getPublicationsByAuthorName("Peroni")
# print(my_m11)

#12th query
# my_m12 = generic.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ])
# print(my_m12)
