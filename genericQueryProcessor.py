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

    # ADDITIONAL METHODS FOR A BETTER CODE

    def getCitation(identifier):
        graph_cite = trp_qp.getCitedOfPublication(identifier)
        rel_cite = rel_qp.getCitedOfPublication(identifier)
        df_cite = concat([graph_cite, rel_cite], ignore_index=True)
        df_cite_no_dupl = df_cite.drop_duplicates()
        cites_list = []
        for row_idx, row in df_cite_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)
            cited_pub = Publication(identifier, publicationYear, title, "", authors, publicationVenue)
            cites_list.append(cited_pub.__str__())
        return cites_list

    def getPublishers(self, identifier):
        graph_df = trp_qp.getVenuesByPublisherId(identifier)
        rel_df = rel_qp.getVenuesByPublisherId(identifier) 
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        organizations_list = list()
        organization = None
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["id"]
            publisher_name = row["name"]
            organization = Organization(identifier, publisher_name)
        organizations_list.append(organization.__str__())
        return organizations_list

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



    # METHODS

    def getPublicationsPublishedInYear(self, year):
        graph_year = trp_qp.getPublicationsPublishedInYear(year)
        rel_year = rel_qp.getPublicationsPublishedInYear(year)
        df_union = concat([graph_year, rel_year], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_union_sorted = df_union_no_dupl.sort_values("publication_year")
        pub_list = list()
        pub_list_object = list()
        for row_idx, row in df_union_sorted.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            pub_list.append(pub.__str__())
            pub_list_object.append(pub)
            return pub_list, pub_list_object

    def getPublicationsByAuthorId(self, AuthorID):
        graph_df = trp_qp.getPublicationsByAuthorId(AuthorID)
        rel_df = rel_qp.getPublicationsByAuthorId(AuthorID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        pub_list = list()
        pub_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            pub_list.append(pub.__str__())
            pub_list_object.append(pub)
            return pub_list, pub_list_object

    def getMostCitedPublication(self):
        graph_df = trp_qp.getMostCitedPublication()
        rel_df = rel_qp.getMostCitedPublication()
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        pub = None
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
        return pub.__str__(), pub

    def getMostCitedVenue(self):
        graph_df = trp_qp.getMostCitedVenue()
        rel_df = rel_qp.getMostCitedVenue()
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        venue = None
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["venue_id"]
            title = row["title"]
            publishers = generic.getPublishers(publisher)
            venue = Venue(identifier, title, publishers)
        return venue.__str__(), venue

    def getVenuesByPublisherId(self, publisherID):
        graph_df = trp_qp.getVenuesByPublisherId(publisherID)
        rel_df = rel_qp.getVenuesByPublisherId(publisherID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        venues_list = list()
        venue_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["venue_id"]
            title = row["title"]
            publishers = generic.getPublishers(publisherID)
            venue = Venue(identifier, title, publishers)
            venues_list.append(venue.__str__())
            venue_list_object.append(venue)
            return venues_list, venue_list_object

    def getPublicationInVenue(self, venueID):
        graph_df = trp_qp.getPublicationInVenue(venueID)
        rel_df = rel_qp.getPublicationInVenue(venueID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        pub_list = list()
        pub_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            pub_list.append(pub.__str__())
            pub_list_object.append(pub)
            return pub_list, pub_list_object


    def getJournalArticlesInIssue(self, issue, volume, journalID):
        graph_df = trp_qp.getJournalArticlesInIssue(issue, volume, journalID)
        rel_df = rel_qp.getJournalArticlesInIssue(issue, volume, journalID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        journal_list = list()
        journal_list_object = list()
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
            journal_list_object.append(journalArticle)
        return journal_list, journal_list_object

    def getJournalArticlesInVolume(self, volume, journalID):
        graph_df = trp_qp.getJournalArticlesInVolume(volume, journalID)
        rel_df = rel_qp.getJournalArticlesInVolume(volume, journalID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        journal_list = list()
        journal_list_object = list()
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
            journal_list_object.append(journalArticle)
        return journal_list, journal_list_object

    def getJournalArticlesInJournal(self, journalID):
        graph_df = trp_qp.getJournalArticlesInJournal(journalID)
        rel_df = rel_qp.getJournalArticlesInJournal(journalID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        journal_list = list()
        journal_list_object = list()
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
            journal_list_object.append(journalArticle)
        return journal_list, journal_list_object

    def getProceedingsByEvent(self, eventPartialName):
        graph_df = trp_qp.getProceedingsByEvent(eventPartialName)
        rel_df = rel_qp.getProceedingsByEvent(eventPartialName)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        proceedings_list = list()
        proceedings_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["venue_id"]
            title = row["publication_venue"]
            publisher = row["publisher"]
            event = eventPartialName
            proceeding = Proceedings(identifier, title, publisher, event)
            proceedings_list.append(proceeding.__str__())
            proceedings_list_object.append(proceeding)
        return proceedings_list, proceedings_list_object

    def getPublicationAuthors(self, publicationID):
        graph_df = trp_qp.getPublicationAuthors(publicationID)
        rel_df = rel_qp.getPublicationAuthors(publicationID)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_union_sorted = df_union_no_dupl.sort_values("family")
        authors = list()
        authors_list_object = list()
        for row_idx, row in df_union_sorted.iterrows():
            orcid = row["orcid"]
            givenName = row["given"]
            familyName = row["family"]
            author = Person(orcid, givenName, familyName)
            authors.append(author.__str__())
            authors_list_object.append(author)
        return authors, authors_list_object

    def getPublicationsByAuthorName(self, partialAuthorName):
        graph_df = trp_qp.getPublicationsByAuthorName(partialAuthorName)
        rel_df = rel_qp.getPublicationsByAuthorName(partialAuthorName)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        pub_list = list()
        pub_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = row["publication_venue"]
            authors = GenericQueryProcessor.getAuthors(identifier)
            cites_list = GenericQueryProcessor.getCitation(identifier)
            pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            pub_list.append(pub.__str__())
            pub_list_object.append(pub)
        return pub_list, pub_list_object

    def getDistinctPublisherOfPublications(self, listOfPublication):
        graph_df = trp_qp.getDistinctPublisherOfPublications(listOfPublication)
        rel_df = rel_qp.getDistinctPublisherOfPublications(listOfPublication)
        df_union = concat([graph_df, rel_df], ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        organizations_list = list()
        organizations_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["id"]
            publisher_name = row["name"]
            organization = Organization(identifier, publisher_name)
            organizations_list.append(organization.__str__())
            organizations_list_object.append(organization)
        return organizations_list, organizations_list_object


# setting the environment for testing, the commands have been commented in order to allow the user to set them freely


# triple_uri = "http://tester"
# trp_dp = TriplestoreDataProcessor(triple_uri)
# trp_dp.setEndpointUri(triple_uri)
# trp_dp.getEndPoint()
# trp_qp = TriplestoreQueryProcessor(triple_uri)

# rel_path = "tester.db"
# rel_dp = RelationalDataProcessor(rel_path)
# rel_dp.setDbPath(rel_path)
# rel_dp.getDbPath()
# rel_qp = RelationalQueryProcessor(rel_path)

#
# generic = GenericQueryProcessor([trp_qp, rel_qp])

#TESTER

#1st query
# my_m1 = generic.getPublicationsPublishedInYear(2020)
# print(my_m1)
# print("-----------------------------------")
# print(my_m1[1][0].getTitle())
# print("-----------------------------------")


#2nd query
# my_m2 = generic.getPublicationsByAuthorId("0000-0001-7553-6916")
# print(my_m2)
# print("-----------------------------------")
# print(my_m2[1][0].getTitle())
# print("-----------------------------------")

#3rd query
# my_m3 = generic.getMostCitedPublication()
# print(my_m3)
# print("-----------------------------------")
# myObj = my_m3[1]
# print(myObj.getCitedPublications())
# print("-----------------------------------")

#4th query
# my_m4 = generic.getMostCitedVenue()
# print(my_m4)
# print("-----------------------------------")
# print(my_m4[1].getTitle())
# print("-----------------------------------")

#5th query
# my_m5 = generic.getVenuesByPublisherId("crossref:78")
# print(my_m5)
# print("-----------------------------------")
# myObj = my_m5[1][0]
# print(myObj.getPublisher())
# print("-----------------------------------")

#6th query
# my_m6 = generic.getPublicationInVenue("issn:0944-1344")
# print(my_m6)
# print("-----------------------------------")
# print(my_m6[1][0].getCitedPublications())
# print("-----------------------------------")

#7th query
# my_m7 = generic.getJournalArticlesInIssue(3, 28, "issn:1066-8888")
# print(my_m7)
# print("-----------------------------------")
# print(my_m7[1][0].getIssue())
# print("-----------------------------------")


#8th query
# my_m8 = generic.getJournalArticlesInVolume(28, "issn:1066-8888")
# print(my_m8)
# print("-----------------------------------")
# print (my_m8[1][0].getTitle())
# print("-----------------------------------")

#9th query
# my_m9 = generic.getProceedingsByEvent("")
# print(my_m9)
# print("-----------------------------------")
# print(my_m9[1][0].getEvent())
# print("-----------------------------------")

#10th query
# my_m10 = generic.getPublicationAuthors("doi:10.1007/s11192-019-03311-9")
# print(my_m10)
# print("-----------------------------------")
# print(my_m10[1][0].getGivenName())
# print("-----------------------------------")

#11th query
# my_m11 = generic.getPublicationsByAuthorName("Peroni")
# print(my_m11)
# print("-----------------------------------")
# print(my_m11[1][0].getCitedPublications())
# print("-----------------------------------")

#12th query
# my_m12 = generic.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ])
# print(my_m12)
# print("-----------------------------------")
# print(my_m12[1][0].getName())
