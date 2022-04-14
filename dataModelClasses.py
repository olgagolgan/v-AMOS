
# import json
# import csv
#
# file_csv = "graph_publications.csv"
# file_json = "graph_other_data.json"
#
# jsonfile = open(file_json, mode="r", encoding="utf-8")
# datajson = json.load(jsonfile)
#
# dataCsv = list()
#
# with open(file_csv, "r", encoding="utf-8") as file:
#     csvReader = csv.reader(file)
#     next(csvReader)
#     for line in csvReader:
#         dataCsv.append(tuple(line))


class IdentifiableEntity:
    def __init__(self, identifier):
        self.identifier = identifier

    def getIds(self):
        return list(self.identifier)


class Publication(IdentifiableEntity):
    def __init__(self, identifier, publicationYear, title, cited, authors, publicationVenue):
        super().__init__(identifier)
        self.publicationYear = publicationYear
        self.title = title
        self.cited = cited
        self.authors = authors
        self.publicationVenue = publicationVenue

    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getCitedPublications(self):
        return self.cited  # cited must be a list

    def getPublicationVenue(self):
        return self.publicationVenue

    def getAuthors(self):
        return self.authors  # authors must be a set


# sara's classes
class JournalArticle(Publication):
    def __init__(self, identifier, publicationYear, title, cited, authors, publicationVenue, issue, volume):
        super().__init__(identifier, publicationYear, title, cited, authors, publicationVenue)
        self.issue = issue
        self.volume = volume

    def getIssue(self):
        return self.issue

    def getVolume(self):
        return self.volume


class BookChapter(Publication):
    def __init__(self, identifier, publicationYear, title, cited, authors, publicationVenue, chapterNumber):
        super().__init__(identifier, publicationYear, title, cited, authors, publicationVenue)
        self.chapterNumber = chapterNumber

    def getChapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    pass


# manu's classes
class Venue(IdentifiableEntity):
    def __init__(self, identifier, title, publisher):
        super().__init__(identifier)
        self.title = title
        self.publisher = publisher

    def getTitle(self):
        return self.title

    def getPublisher(self):
        return self.publisher


class Journal(Venue):
    pass


class Book(Venue):
    pass


class Proceedings(Venue):
    def __init__(self, identifier, title, publisher, event):
        super().__init__(identifier, title, publisher)
        self.event = event

    def getEvent(self):
        return self.event


# amelia's classes
class Person(IdentifiableEntity):
    def __init__(self, identifier, givenName, familyName):
        super().__init__(identifier)
        self.givenName = givenName
        self.familyName = familyName

    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName


class Organization(IdentifiableEntity):
    def __init__(self, identifier, name):
        super().__init__(identifier)
        self.name = name

    def getName(self):
        return self.name