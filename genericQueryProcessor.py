from nntplib import ArticleInfo
from triplestoreProcessorClasses import *
from relationalProcessorClasses import *
from pandas import concat, DataFrame
from dataModelClasses import *

def bestFreqVen(mostCitedVenue):
    for index, row in mostCitedVenue.iterrows():
        if index < len(mostCitedVenue)-1:
            if mostCitedVenue.iloc[index]["id_no."] < mostCitedVenue.iloc[index+1]["id_no."]:
                firstVenueID = mostCitedVenue.iloc[index]["venue_id"]
                secondVenueID = mostCitedVenue.iloc[index+1]["venue_id"]
                mostCitedVenue.at[index, "venue_id"] = frozenset({firstVenueID, secondVenueID})
                mostCitedVenue.at[index+1, "venue_id"] = "already"
    mostCitedVenue = mostCitedVenue.query("venue_id != 'already'").reset_index(drop=True)
    venuesRank = mostCitedVenue["venue_id"].value_counts()
    venuesRank = DataFrame(venuesRank.to_frame().reset_index()).rename(columns={"index": "venue_id", "venue_id": "frequency"})
    mostCitedVenue = mostCitedVenue.astype({"venue_id": str}, errors='raise')    
    venuesRank = venuesRank.astype({"venue_id": str}, errors='raise')
    mostCitedVenue = mostCitedVenue[["title",  "venue_id",  "publisher", "name", "venue_type", "event"]]
    bestVenue = venuesRank.loc[0]['venue_id']
    bestVenueInfo = DataFrame(mostCitedVenue.query('''venue_id == "{0}"'''.format(bestVenue)).iloc[0]).transpose().reset_index(drop=True)
    if bestVenueInfo.loc[0]['venue_id'].startswith("frozenset("):
        bestVenueInfo.loc[0]['venue_id'] = bestVenueInfo.loc[0]['venue_id'].lstrip("frozenset(").rstrip(")").strip('}{').split(', ')
        for i in range(len(bestVenueInfo.loc[0]['venue_id'])):
            id = bestVenueInfo.loc[0]['venue_id'][i]
            id = id.replace("'", "")
            bestVenueInfo.loc[0]['venue_id'][i] = id
    else:
        bestVenueInfo.loc[0]['venue_id'] = bestVenueInfo.loc[0]['venue_id'].replace("'", "").split(" ")
    return bestVenueInfo

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

    def checkTypePublication(self, type, doi, publicationYear, title, issue, volume, chapter, publicationVenue, authors, cites_list=None):
        if (type == "journal-article"):
                pub = JournalArticle(doi, int(publicationYear), title, cites_list, authors, publicationVenue,
                                     issue, volume)
        elif (type == "book-chapter"):
            pub = BookChapter(doi, int(publicationYear), title, cites_list, authors, publicationVenue,
                                int(chapter))
        elif (type == "proceedings-paper"):
            pub = ProceedingsPaper(doi, int(publicationYear), title, cites_list, authors,
                                        publicationVenue, issue, volume)
        else:
            pub = Publication(doi, int(publicationYear), title, cites_list, authors, publicationVenue)
        pub
        return pub
    
    def checkTypeVenue(self, v_type, identifier, title, publishers, event):
        if (v_type == "journal"):
            venue = Journal(identifier, title, publishers)
        elif (v_type == "book"):
            venue = Book(identifier, title, publishers)
        elif (v_type == "proceedings"):
            venue = Proceedings(identifier, title, publishers, event)
        else:
            venue = Venue(identifier, title, publishers)
        return venue

    def getPubInfo(self, identifier):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getPubInfo(identifier))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        return df_union_no_dupl


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
            cited_pub = self.checkTypePublication(type, identifier, publicationYear, title, issue, volume, chapter, publicationVenue, authors, cites_list)
            cites_list.append(cited_pub)
        return cites_list

    def getPublishers(self, identifier):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getVenuesByPublisherId(identifier))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        df_last = df_union_no_dupl.drop_duplicates( subset=['id'], keep='last').reset_index(drop=True)
        organization = None
        for row_idx, row in df_last.iterrows():
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
        df_last = df_union_no_dupl.drop_duplicates( subset=['orcid'], keep='last').reset_index(drop=True)
        authors = set()
        for row_idx, row in df_last.iterrows():
            orcid = row["orcid"]
            givenName = row["given"]
            familyName = row["family"]
            author = Person(orcid, givenName, familyName)
            authors.add(author)
        return authors

    def getVenueByPublicationId(self, publication):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getVenueByPublicationId(publication))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        if len(df_union_no_dupl.index) > 1:
            identifier = [df_union_no_dupl.iloc[0]["venue_id"], df_union_no_dupl.iloc[1]["venue_id"]]
        else: 
            identifier = df_union_no_dupl.iloc[0]["venue_id"]
        title = df_union_no_dupl.iloc[0]["title"]
        publisher = df_union_no_dupl.iloc[0]["id"]
        venue_type = df_union_no_dupl.iloc[0]["venue_type"]
        event = df_union_no_dupl.iloc[0]["event"]
        publishers = self.getPublishers(publisher)
        if (venue_type == "journal"):
            venue = Journal(identifier, title, publishers)
        elif (venue_type == "book"):
            venue = Book(identifier, title, publishers)
        elif (venue_type == "proceedings"):
            venue = Proceedings(identifier, title, publishers, event)
        else:
            venue = Venue(identifier, title, publishers)
        return venue


    # METHODS
    
    def getPublicationsPublishedInYear(self, year):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getPublicationsPublishedInYear(year))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates().drop_duplicates(subset=['doi'])
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
            pub = self.checkTypePublication(type, identifier, publicationYear, title, issue, volume, chapter, publicationVenue, authors, cites_list)
            pub_list_object.append(pub)

        return pub_list_object

    def getPublicationsByAuthorId(self, AuthorID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getPublicationsByAuthorId(AuthorID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates().drop_duplicates(subset=['doi'])

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
            pub = self.checkTypePublication(type, identifier, publicationYear, title, issue, volume, chapter, publicationVenue, authors, cites_list)
            pub_list_object.append(pub)

        return pub_list_object

    
    def getMostCitedPublication(self):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getMostCitedPublication())
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        doi_dict = dict()
        doi_set = df_union_no_dupl["doi"].unique()
        for it in doi_set:
            my_doi = df_union_no_dupl.loc[df_union_no_dupl["doi_mention"] == it]
            doi_dict[it] = my_doi.__len__()
        doi_df = DataFrame.from_dict(doi_dict, orient="index")
        identifier = doi_df.sort_values(0, ascending=False).first_valid_index()
        pub_info = self.getPubInfo(identifier)
        publicationYear = pub_info.loc[pub_info["doi"] == identifier]["publication_year"].iloc[0]
        title = pub_info.loc[pub_info["doi"] == identifier]["title"].iloc[0]
        type = pub_info.loc[pub_info["doi"] == identifier]["type"].iloc[0]
        issue = pub_info.loc[pub_info["doi"] == identifier]["issue"].iloc[0]
        volume = pub_info.loc[pub_info["doi"] == identifier]["volume"].iloc[0]
        chapter = pub_info.loc[pub_info["doi"] == identifier]["chapter"].iloc[0]
        publicationVenue = self.getVenueByPublicationId(identifier)
        authors = self.getAuthors(identifier)
        cites_list = self.getCitation(identifier)
        pub = self.checkTypePublication(type, identifier, publicationYear, title, issue, volume, chapter, publicationVenue, authors, cites_list)
        return pub

    def getMostCitedVenue(self):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getMostCitedVenue())
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        info_venue = bestFreqVen(df_union_no_dupl)
        for row_idx, row in info_venue.iterrows():
            identifier = row["venue_id"]
            title = row["title"]
            venue_type = row["venue_type"]
            publishers = Organization(row["publisher"], row["name"])
            venue = self.checkTypeVenue(venue_type, identifier, title, publishers, event)      
        return venue


    def getVenuesByPublisherId(self, publisherID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getVenuesByPublisherId(publisherID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates()
        venue_list_object = list()
        for row_idx, row in df_union_no_dupl.iterrows():
            if (row_idx) < len(df_union_no_dupl)-1:
                if row["publication_venue"] == df_union_no_dupl.iloc[row_idx+1]["publication_venue"]:
                    df_union_no_dupl.at[row_idx,"venue_id"] = [row["venue_id"], df_union_no_dupl.iloc[row_idx+1]["venue_id"]]

        final_df = df_union_no_dupl.drop_duplicates(subset = ["publication_venue"])
        for row_idx, row in final_df.iterrows():
            identifier = row["venue_id"]
            title = row["publication_venue"]
            venue_type = row["venue_type"]
            publishers = self.getPublishers(publisherID)
            venue = self.checkTypeVenue(venue_type, identifier, title, publishers, event)
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
            pub = self.checkTypePublication(type, identifier, publicationYear, title, issue, volume, chapter, publicationVenue, authors, cites_list)
            pub_list_object.append(pub)

        return pub_list_object


    def getJournalArticlesInIssue(self, issue, volume, journalID):
        dataframes = []
        for qprocessor in self.queryProcessor:
            dataframes.append(qprocessor.getJournalArticlesInIssue(issue, volume, journalID))
        df_union = concat(dataframes, ignore_index=True)
        df_union_no_dupl = df_union.drop_duplicates().drop_duplicates(subset=['doi'])

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
        df_union_no_dupl = df_union.drop_duplicates().drop_duplicates(subset=['doi'])

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
        df_union_no_dupl = df_union.drop_duplicates().drop_duplicates(subset=['doi'])

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
        for row_idx, row in df_union_no_dupl.iterrows():
            if (row_idx) < len(df_union_no_dupl)-1:
                if row["publication_venue"] == df_union_no_dupl.iloc[row_idx+1]["publication_venue"]:
                    df_union_no_dupl.at[row_idx,"venue_id"] = [row["venue_id"], df_union_no_dupl.iloc[row_idx+1]["venue_id"]]

        final_df = df_union_no_dupl.drop_duplicates(subset = ["publication_venue"])

        proceedings_list_object = list()
        for row_idx, row in final_df.iterrows():
            identifier = row["venue_id"]
            title = row["publication_venue"]
            publisher = row["publisher"]
            event = row["event"]
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
        df_union_no_dupl = df_union.drop_duplicates().drop_duplicates(subset=['doi'])

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
            pub = self.checkTypePublication(type, identifier, publicationYear, title, issue, volume, chapter, publicationVenue, authors, cites_list)
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
# print(trp_dp.uploadData("data/graph_publications2.csv"))
# print(trp_dp.uploadData("data/graph_other_data2.json"))
trp_qp = TriplestoreQueryProcessor()
trp_qp.setEndpointUrl(triple_uri)

rel_path = "tester.db"
rel_dp = RelationalDataProcessor()
rel_dp.setDbPath(rel_path)
# print(rel_dp.uploadData("data/relational_publications2.csv"))
# print(rel_dp.uploadData("data/relationalJSON2.json"))
rel_qp = RelationalQueryProcessor()
rel_qp.setDbPath(rel_path)

generic = GenericQueryProcessor()
generic.addQueryProcessor(trp_qp)
generic.addQueryProcessor(rel_qp)



#TESTER



#1st query
# my_m1 = generic.getPublicationsPublishedInYear(2015)
# print(my_m1)
# print(len(my_m1))
# for x in my_m1:
#     print(x.getIds(), x.getTitle())
#     for info in x.getAuthors():
#         print(info.getIds())
#     print("-----------------------------------")
    
# print(my_m1[0].getIds())
# print(my_m1[0].getTitle())
# print(my_m1[0].getCitedPublications())
# print(my_m1[0].getPublicationYear())
# print(my_m1[0].getAuthors())
# print("-----------------------------------")
# a = my_m1[0].getPublicationVenue()
# print(a.getTitle())
# print(a.getIds())
# print(a.getPublisher())


#2nd query
# my_m2 = generic.getPublicationsByAuthorId("0000-0002-2440-3993")
# print(my_m2)
# for x in my_m2:
#     print(x.getIds(), x.getTitle())
#     for info in x.getAuthors():
#         print(info.getIds())
#     print("-----------------------------------")
# print("-----------------------------------")
# print(my_m2[0].getIds())
# print(my_m2[0].getTitle())
# print(my_m2[0].getIds())
# print(my_m2[0].getCitedPublications())
# print(my_m2[0].getPublicationVenue())
# print(my_m2[0].getPublicationYear())
# print(my_m2[0].getAuthors())



# print("-----------------------------------")

#3rd query
# my_m3 = generic.getMostCitedPublication()
# print(my_m3)
# print("-----------------------------------")
# print(my_m3.getTitle())
# print(my_m3.getIds())
# print("-----------------------------------")
# print(my_m3.getCitedPublications())
# print("-----------------------------------")
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
# my_m5 = generic.getVenuesByPublisherId("crossref:735")
# print(len(my_m5))
# for x in my_m5:
#     print(x.getIds(), x.getTitle())
# print("-----------------------------------")
# myObj = my_m5[1]
# print(myObj)
# print(myObj.getEvent())
# print(myObj.getPublisher())
# print(myObj.getTitle())
# print("-----------------------------------")

#6th query
# my_m6 = generic.getPublicationInVenue("issn:0944-1344")
# print(len(my_m6))
# print(my_m6[0].getTitle())
# print(my_m6[0].getIds())
# print("-----------------------------------")
# myO = my_m6[1].getPublicationVenue()
# print(myO.getIds())
# print(myO)
# print("-----------------------------------")
# myX = myO.getPublisher()
# print(myX)
# print(myX.getIds())
# print("--------")
# for article in my_m6:
#     print(article.getIds())

#7th query
# my_m7 = generic.getJournalArticlesInIssue("1", "126", "issn:1588-2861")
# print(len(my_m7))
# for article in my_m7:
#    print(article.getIds())
# print("-----------------------------------")

# print(my_m7[0].getIssue())
# print("-----------------------------------")

#TEST GRAPH
# my_m7_2 = generic.getJournalArticlesInIssue("7", "14", "issn:1996-1073")
# print(len(my_m7_2))
# for article in my_m7_2:
#     print(article.getIds())
# print("-----------------------------------")
#  print(my_m7_2[0].getIssue())
# print("-----------------------------------")

#TEST REL
# my_m7_3 = generic.getJournalArticlesInIssue("9", "126", "issn:0138-9130")
# print(len(my_m7_3))
# for article in my_m7_3:
#     print(article.getIds())
# print("-----------------------------------")


#8th query
# my_m8 = generic.getJournalArticlesInVolume("28", "issn:1066-8888")
# print(len(my_m8))
# for article in my_m8:
#     print(article.getIds())
# print("-----------------------------------")
# my_m8_2 = generic.getJournalArticlesInVolume("7","issn:2304-6775")
# print(my_m8_2)
# print (my_m8[1][0].getTitle())
# print("-----------------------------------")

#9th query
# my_m9 = generic.getJournalArticlesInJournal("issn:1066-8888")
# print(len(my_m9))
# for article in my_m9:
#     print(article.getIds())
# print("-----------------------------------")
# print (my_m9[0].getPublicationVenue())
# print("-----------------------------------")


#10th query
# my_m10 = generic.getProceedingsByEvent("meet")
# print(len(my_m10))
# print(my_m10[0].getIds())
# print("-----------------------------------")
# print(my_m10[0].getEvent())
# print("-----------------------------------")

#11th query
# my_m11 = generic.getPublicationAuthors("doi:10.1007/s11192-019-03217-6")
# print(len(my_m11))
# for aut in my_m11:
#     print(aut.getIds())
# print("-----------------------------------")
# print(my_m11[0].getGivenName())
# print("-----------------------------------")

#12th query
# my_m12 = generic.getPublicationsByAuthorName("Peroni")
# print(len(my_m12))
# for x in my_m12:
#     print(x.getIds(), x.getTitle())
# print("-----------------------------------")
# print(my_m12[0].getIds())
# print(my_m12[0].getTitle())
# print(my_m12[0].getCitedPublications())
# print(my_m12[0].getPublicationYear())
# print(my_m12[0].getAuthors())
# print("-----------------------------------")

#13th query
# my_m13 = generic.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ])
# print(len(my_m13))
# for x in my_m13:
#     print(x.getIds())
# print("-----------------------------------")
# print(my_m13[0].getName())

#generic.getCitation("doi:10.1016/j.websem.2021.100655")
