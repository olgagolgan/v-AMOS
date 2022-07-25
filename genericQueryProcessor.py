from triplestoreProcessorClasses import *
from relationalProcessorClasses import *
from pandas import concat
from dataModelClasses import *

# Added 03-07-22
def bestRow(df, colName):
    maxVal = df[colName].max()
    contentCol = []
    for idx, val in df[colName].iteritems():
        if val == maxVal:
            contentCol.append(idx)
    return df.loc[contentCol]
# ---------------

class GenericQueryProcessor:
    def __init__(self, queryProcessor=None):
        queryProcessor = list()
        self.queryProcessor = queryProcessor

    def cleanQueryProcessors(self):
        self.queryProcessor.clear()
        if len(self.queryProcessor) == 0:
            return True
        else:
            return False

    def addQueryProcessor(self, Processor):
        self.queryProcessor.append(Processor)

    # ADDITIONAL METHODS FOR A BETTER CODE

    def getCitation(self, identifier):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getCitedOfPublication(identifier))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        cites_list = []
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            chapter = row["chapter"]
            issue= row["issue"]
            volume= row["volume"]
            type = row["type"]
            publicationVenue = self.getVenueByPublicationId(identifier)
            authors = self.getAuthors(identifier)
            if(type == "journal-article"):
                cited_pub = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            elif(type == "book-chapter"):
                cited_pub = BookChapter(identifier, publicationYear, title, cites_list, authors, publicationVenue, chapter)
            else:
                cited_pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            cites_list.append(cited_pub)
        return cites_list

    def getPublishers(self, identifier):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getVenuesByPublisherId(identifier))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        
        organizations_list = list()
        organization = None
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["id"]
            publisher_name = row["name"]
            organization = Organization(identifier, publisher_name)
        return organization

    def getAuthors(self, identifier):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getPublicationAuthors(identifier))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        authors = set()
        for row_idx, row in df_union_no_dupl.iterrows():
            orcid = row["orcid"]
            givenName = row["given"]
            familyName = row["family"]
            author = str(orcid) + ", " + str(givenName) + ", " + str(familyName)
            authors.add(author)
        return authors

    def getVenueByPublicationId(self, publication):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getVenueByPublicationId(publication))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["venue_id"]
            title = row["title"]
            publisher = row["id"]
            publishers = self.getPublishers(publisher)
            venue = Venue(identifier, title, publishers)
            return venue


    # METHODS
    
    def getPublicationsPublishedInYear(self, year):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getPublicationsPublishedInYear(year))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_union_sorted = df_union_no_dupl.sort_values("publication_year")

        pub_list_object = list()
        for row_idx, row in df_union_sorted.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            type = row["type"]
            issue = row["issue"]
            volume = row["volume"]
            chapter = row["chapter"]
            publicationVenue = self.getVenueByPublicationId(identifier)
            authors = self.getAuthors(identifier)
            cites_list = self.getCitation(identifier)
            if(type == "journal-article"):
                pub = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            elif(type == "book-chapter"):
                pub = BookChapter(identifier, publicationYear, title, cites_list, authors, publicationVenue, chapter)
            else:
                pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            
            pub_list_object.append(pub)

        return pub_list_object

    def getPublicationsByAuthorId(self, AuthorID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getPublicationsByAuthorId(AuthorID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        pub_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            type = row["type"]
            issue = row["issue"]
            volume = row["volume"]
            chapter = row["chapter"]
            publicationVenue = self.getVenueByPublicationId(identifier)
            authors = self.getAuthors(identifier)
            cites_list = self.getCitation(identifier)
            if(type == "journal-article"):
                pub = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            elif(type == "book-chapter"):
                pub = BookChapter(identifier, publicationYear, title, cites_list, authors, publicationVenue, chapter)
            else:
                pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)

            pub_list_object.append(pub)

        return pub_list_object

    def getMostCitedPublication(self):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getMostCitedPublication())
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_union_no_dupl = bestRow(df_union_no_dupl, "MostCited") #Added 03-07-2022

        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            type = row["type"]
            issue = row["issue"]
            volume = row["volume"]
            chapter = row["chapter"]
            publicationVenue = self.getVenueByPublicationId(identifier)
            authors = self.getAuthors(identifier)
            cites_list = self.getCitation(identifier)
            if(type == "journal-article"):
                pub = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            elif(type == "book-chapter"):
                pub = BookChapter(identifier, publicationYear, title, cites_list, authors, publicationVenue, chapter)
            else:
                pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
        return pub
        
    def getMostCitedVenue(self): # Replaced 03-07-22
        mostCitedPublication = GenericQueryProcessor.getMostCitedPublication(self) 
        return mostCitedPublication.getPublicationVenue()

    def getVenuesByPublisherId(self, publisherID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getVenuesByPublisherId(publisherID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        venue_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["venue_id"]
            title = row["publication_venue"]
            publishers = self.getPublishers(publisherID)
            venue = Venue(identifier, title, publishers)
            venue_list_object.append(venue)

        return venue_list_object

    def getPublicationInVenue(self, venueID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getPublicationInVenue(venueID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        pub_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            type = row["type"]
            issue = row["issue"]
            volume = row["volume"]
            chapter = row["chapter"]
            publicationVenue = self.getVenueByPublicationId(identifier)
            authors = self.getAuthors(identifier)
            cites_list = self.getCitation(identifier)
            if(type == "journal-article"):
                pub = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            elif(type == "book-chapter"):
                pub = BookChapter(identifier, publicationYear, title, cites_list, authors, publicationVenue, chapter)
            else:
                pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            pub_list_object.append(pub)

        return pub_list_object


    def getJournalArticlesInIssue(self, issue, volume, journalID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getJournalArticlesInIssue(issue, volume, journalID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        journal_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            issue = row["issue"]
            volume = row["volume"]
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = self.getVenueByPublicationId(identifier)
            authors = self.getAuthors(identifier)
            cites_list = self.getCitation(identifier)
            journalArticle = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            journal_list_object.append(journalArticle)

        return journal_list_object

    def getJournalArticlesInVolume(self, volume, journalID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getJournalArticlesInVolume(volume, journalID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        journal_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            volume = row["volume"]
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = self.getVenueByPublicationId(identifier)
            issue = row["issue"]
            authors = self.getAuthors(identifier)
            cites_list = self.getCitation(identifier)
            journalArticle = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            journal_list_object.append(journalArticle)

        return journal_list_object

    def getJournalArticlesInJournal(self, journalID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getJournalArticlesInJournal(journalID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        journal_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = self.getVenueByPublicationId(identifier)
            issue = row["issue"]
            volume = row["volume"]
            authors = self.getAuthors(identifier)
            cites_list = self.getCitation(identifier)
            journalArticle = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            journal_list_object.append(journalArticle)

        return journal_list_object

    def getProceedingsByEvent(self, eventPartialName):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getProceedingsByEvent(eventPartialName))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        proceedings_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["venue_id"]
            title = row["publication_venue"]
            publisher = row["publisher"]
            event = eventPartialName
            proceeding = Proceedings(identifier, title, publisher, event)
            proceedings_list_object.append(proceeding)

        return proceedings_list_object

    def getPublicationAuthors(self, publicationID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getPublicationAuthors(publicationID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_union_sorted = df_union_no_dupl.sort_values("family")

        authors_list_object = list()
        for row_idx, row in df_union_sorted.iterrows():
            orcid = row["orcid"]
            givenName = row["given"]
            familyName = row["family"]
            author = Person(orcid, givenName, familyName)
            authors_list_object.append(author)

        return authors_list_object

    def getPublicationsByAuthorName(self, partialAuthorName):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getPublicationsByAuthorName(partialAuthorName))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        pub_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            type = row["type"]
            issue = row["issue"]
            volume = row["volume"]
            chapter = row["chapter"]
            publicationVenue = self.getVenueByPublicationId(identifier)
            authors = self.getAuthors(identifier)
            cites_list = self.getCitation(identifier)
            if(type == "journal-article"):
                pub = JournalArticle(identifier, publicationYear, title, cites_list, authors, publicationVenue, issue, volume)
            elif(type == "book-chapter"):
                pub = BookChapter(identifier, publicationYear, title, cites_list, authors, publicationVenue, chapter)
            else:
                pub = Publication(identifier, publicationYear, title, cites_list, authors, publicationVenue)
            pub_list_object.append(pub)

        return pub_list_object

    def getDistinctPublisherOfPublications(self, listOfPublication):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getDistinctPublisherOfPublications(listOfPublication))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()

        organizations_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            identifier = row["id"]
            publisher_name = row["name"]
            organization = Organization(identifier, publisher_name)
            organizations_list_object.append(organization)

        return organizations_list_object


# setting the environment for testing, the commands have been commented in order to allow the user to set them freely


triple_uri = "http://127.0.0.1:9999/blazegraph/sparql"
trp_dp = TriplestoreDataProcessor()
trp_dp.setEndpointUrl(triple_uri)
# print(trp_dp.uploadData("data/graph_publications.csv"))
# print(trp_dp.uploadData("data/graph_other_data.json"))
trp_qp = TriplestoreQueryProcessor()
trp_qp.setEndpointUrl(triple_uri)

rel_path = "tester.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
# rel_dp.uploadData("data/relational_publications.csv")
# rel_dp.uploadData("data/relationalJSON.json")
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

generic = GenericQueryProcessor()
generic.addQueryProcessor(trp_qp)
generic.addQueryProcessor(rel_qp)



#TESTER



#1st query
# my_m1 = generic.getPublicationsPublishedInYear(2020)
# print(my_m1)
# print("-----------------------------------")
# print(my_m1[0].getPublicationVenue())
# print("-----------------------------------")


#2nd query
# my_m2 = generic.getPublicationsByAuthorId("0000-0002-2440-3993")
# print(my_m2)
# print("-----------------------------------")
# print(my_m2[0].getIds())
# print("-----------------------------------")

#3rd query
# my_m3 = generic.getMostCitedPublication()
# print(my_m3)
# print("-----------------------------------")
# print(my_m3.getTitle())
# print(my_m3.getIds())
# print(my_m3.getCitedPublications())
# myObj = my_m3.getPublicationVenue()
# print(myObj)
# print("-----------------------------")
# print(myObj.getTitle())
# print(myObj.getIds())
# print(myObj.getPublisher())
# print("-----------------------------------")

# 4th query
# my_m4 = generic.getMostCitedVenue()
# print(my_m4)
# print("-----------------------------------")
# print(my_m4.getTitle())
# print(my_m4.getIds())
# print(my_m4.getPublisher())
# print("-----------------------------------")

#5th query
# my_m5 = generic.getVenuesByPublisherId("crossref:78")
# # print(my_m5)
# print("-----------------------------------")
# myObj = my_m5[1]
# print(myObj)
# print(myObj.getIds())
# print(myObj.getPublisher())
# print(myObj.getTitle())
# print("-----------------------------------")

#6th query
# my_m6 = generic.getPublicationInVenue("issn:0944-1344")
# print(my_m6)
# print("-----------------------------------")
# myO = my_m6[1].getPublicationVenue()
# print(myO)
# print("-----------------------------------")
# myX = myO.getPublisher()
# print(myX)

#7th query
# my_m7 = generic.getJournalArticlesInIssue("1", "126", "issn:1588-2861")
# print(my_m7)
# print("-----------------------------------")
# print(my_m7[0].getIssue())
# print("-----------------------------------")

#TEST GRAPH
# my_m7_2 = generic.getJournalArticlesInIssue("7", "14", "issn:1996-1073")
# print(my_m7_2)
# print("-----------------------------------")
#  print(my_m7_2[0].getIssue())
# print("-----------------------------------")

#TEST REL
# my_m7_3 = generic.getJournalArticlesInIssue("9", "126", "issn:0138-9130")
# print(my_m7_3)


#8th query
# my_m8 = generic.getJournalArticlesInVolume("28", "issn:1066-8888")
# print(my_m8)
# print("-----------------------------------")
# my_m8_2 = generic.getJournalArticlesInVolume("7","issn:2304-6775")
# print(my_m8_2)
# print (my_m8[1][0].getTitle())
# print("-----------------------------------")

#9th query
# my_m9 = generic.getJournalArticlesInJournal("issn:1066-8888")
# print(my_m9)
# print("-----------------------------------")
# print (my_m9[0].getPublicationVenue())
# print("-----------------------------------")


#10th query
# my_m10 = generic.getProceedingsByEvent("")
# print(my_m10)
# print("-----------------------------------")
# print(my_m10[0].getEvent())
# print("-----------------------------------")

#10th query
# my_m11 = generic.getPublicationAuthors("doi:10.1007/s11192-019-03311-9")
# print(my_m11)
# print("-----------------------------------")
# print(my_m11[0].getGivenName())
# print("-----------------------------------")

#12th query
my_m12 = generic.getPublicationsByAuthorName("Peroni")
print(my_m12)
# print("-----------------------------------")
# print(my_m12[0].getCitedPublications())
# print("-----------------------------------")

#13th query
# my_m13 = generic.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ])
# print(my_m13)
# print("-----------------------------------")
# print(my_m13[0].getName())
