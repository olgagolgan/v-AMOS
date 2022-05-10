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
            cited_pub = [identifier, publicationYear, title, authors, publicationVenue]   
            cites_list.append(cited_pub)
        return cites_list

    def getAuthors(identifier):
        graph_au = trp_qp.getPublicationAuthors(identifier)
        rel_au = rel_qp.getPublicationAuthors(identifier) 
        df_au = concat([graph_au, rel_au], ignore_index=True)
        df_au_no_dupl = df_au.drop_duplicates()
        authors = list()
        for row_idx, row in df_au_no_dupl.iterrows():
            orcid = row["orcid"]
            givenName = row["given"]
            familyName = row["family"]
            author = str(orcid) + ", " + str(givenName) + ", " + str(familyName)
            authors.append(author)
        return authors
    
    def getInfoVenuePub(identifier):
        graph_venpub = trp_qp.getVenuesInfoByDoi(identifier)
        graph_venpub.columns = ['venue_id', 'publication_venue', 'id', 'name']
        rel_venpub = rel_qp.getVenuesInfoByDoi(identifier)
        df_venpub = concat([graph_venpub, rel_venpub], ignore_index=True)
        df_venpub = df_venpub.drop_duplicates()
        venuesIdList = []
        for el in df_venpub["venue_id"]:
            venuesIdList.append(el)
        venuesId = ", ".join(venuesIdList)
        venueName = df_venpub["publication_venue"][0]
        pubId = df_venpub["id"][0]
        pubName = df_venpub["name"][0]
        listInfoVen = [venuesId, venueName, [pubId, pubName]]
        return listInfoVen

    def getInfoPublisher(identifier):
        graph_pub = trp_qp.getPubNameById(identifier)
        rel_pub = rel_qp.getPubNameById(identifier)
        df_pub = concat([graph_pub, rel_pub], ignore_index=True)
        df_pub = df_pub.drop_duplicates()
        df_pub = df_pub.dropna()
        publisherList = [df_pub["id"][0], df_pub["name"][0]]
        return publisherList

    # METHODS

    def getPublicationsPublishedInYear(self, year):
        graph_year = trp_qp.getPublicationsPublishedInYear(year)
        rel_year = rel_qp.getPublicationsPublishedInYear(year)  
        df_union = concat([graph_year, rel_year], ignore_index=True)
        df_union = df_union.drop_duplicates()
        #df_union_sorted = df_union_no_dupl.sort_values("publication_year")
        pub_list = list()
        pub_list_object = list()
        for row_idx, row in df_union.iterrows():
            identifier = row["doi"]
            publicationYear = row["publication_year"]
            title = row["title"]
            publicationVenue = GenericQueryProcessor.getInfoVenuePub(identifier)
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
            publicationVenue = GenericQueryProcessor.getInfoVenuePub(identifier)
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
            publicationVenue = GenericQueryProcessor.getInfoVenuePub(identifier)
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
            publisher = GenericQueryProcessor.getInfoPublisher(row["publisher"])
            venue = Venue(identifier, title, publisher)
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
            publisherOfVen = GenericQueryProcessor.getInfoPublisher(publisherID)
            venue = Venue(identifier, title, publisherOfVen)
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
            publicationVenue = GenericQueryProcessor.getInfoVenuePub(identifier)
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
            publicationVenue = GenericQueryProcessor.getInfoVenuePub(identifier)
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
            publicationVenue = GenericQueryProcessor.getInfoVenuePub(identifier)
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
            publicationVenue = GenericQueryProcessor.getInfoVenuePub(identifier)
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
            publisher = GenericQueryProcessor.getInfoPublisher(row["publisher"])
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
            publicationVenue = GenericQueryProcessor.getInfoVenuePub(identifier)
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

# ========== SET RELDb

rel_path = "relationalDatabase.db"
rel_dp = RelationalDataProcessor(rel_path)
rel_dp.setDbPath(rel_path)
rel_dp.uploadData("data/relational_publications.csv")
rel_dp.uploadData("data/relationalJSON.json")
rel_qp = RelationalQueryProcessor(rel_path)
rel_qp.setDbPath(rel_path)

# ========== SET TRIDb

graph1 = TriplestoreProcessor("http://127.0.0.1:9999/blazegraph")
graph2 = TriplestoreDataProcessor("http://127.0.0.1:9999/blazegraph")
graph2.uploadData("data/graph_publications.csv")
graph2.uploadData("data/graph_other_data.json")
trp_qp = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph")

# ========= SET GENERIC

generic = GenericQueryProcessor([rel_qp, trp_qp])     
