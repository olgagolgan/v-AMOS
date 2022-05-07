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
    
    #####################################
    
    #ADDITIONAL METHODS FOR A BETTER CODE
    def getCitation(identifier): 
        graph_cite = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getCitedOfPublication(identifier)
        rel_cite = RelationalQueryProcessor("relationalDatabase.db").getCitedOfPublication(identifier)  
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
        graph_au = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getPublicationAuthors(identifier)
        rel_au = RelationalQueryProcessor("relationalDatabase.db").getPublicationAuthors(identifier) 
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
        graph_year = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getPublicationsPublishedInYear(year)
        rel_year = RelationalQueryProcessor("relationalDatabase.db").getPublicationsPublishedInYear(year)  
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getPublicationsByAuthorId(AuthorID)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getPublicationsByAuthorId(AuthorID)  
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getMostCitedPublication()
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getMostCitedPublication()  
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getMostCitedVenue()
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getMostCitedVenue() 
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getVenuesByPublisherId(publisherID)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getVenuesByPublisherId(publisherID) 
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getPublicationInVenue(venueID)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getPublicationInVenue(venueID)  
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getJournalArticlesInIssue(issue, volume, journalID)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getJournalArticlesInIssue(issue, volume, journalID)
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getJournalArticlesInVolume(volume, journalID)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getJournalArticlesInVolume(volume, journalID)
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getJournalArticlesInJournal(journalID)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getJournalArticlesInJournal(journalID)
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getProceedingsByEvent(eventPartialName)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getProceedingsByEvent(eventPartialName)
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getPublicationAuthors(publicationID)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getPublicationAuthors(publicationID)  
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getPublicationsByAuthorName(partialAuthorName)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getPublicationsByAuthorName(partialAuthorName)  
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
        graph_df = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph").getDistinctPublisherOfPublications(listOfPublication)
        rel_df = RelationalQueryProcessor("relationalDatabase.db").getDistinctPublisherOfPublications(listOfPublication)  
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        organizations_list = list()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["id"]
            publisher_name = row["name"]
            organization = Organization(identifier, publisher_name)
            organizations_list.append(organization.__str__())

        return '\n'.join(organizations_list)

#TESTER

#1st query
my_m1 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getPublicationsPublishedInYear(2020)
print(my_m1)

#2nd query
# my_m2 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getPublicationsByAuthorId("0000-0001-9773-4008")
# print(my_m2)

#3rd query
# my_m3 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getMostCitedPublication()
# print(my_m3)

#4th query
# my_m4 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getMostCitedVenue()
# print(my_m4)

#5th query
# my_m5 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getVenuesByPublisherId("crossref:78")
# print(my_m5)

#6th query 
# my_m6 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getPublicationInVenue("issn:0944-1344")
# print(my_m6)

#7th query
# my_m7 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getJournalArticlesInIssue(3, 28, "issn:1066-8888")
# print(my_m7)

#8th query
# my_m8 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getJournalArticlesInVolume(28, "issn:1066-8888")
# print(my_m8)

#9th query
# my_m9 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getProceedingsByEvent("") 
# print(my_m9) 

#10th query
# my_m10 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getPublicationAuthors("doi:10.1007/s11192-019-03311-9")
# print(my_m10)

#11th query
# my_m11 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getPublicationsByAuthorName("Peroni")
# print(my_m11)

#12th query
# my_m12 = GenericQueryProcessor([TriplestoreQueryProcessor, RelationalQueryProcessor]).getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ])
# print(my_m12)
