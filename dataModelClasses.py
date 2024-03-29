class IdentifiableEntity:
    def __init__(self, identifier):
        if type(identifier) == list:
            myList = []
            for el in identifier:
                myList.append(el)
            self.identifier = myList
        self.identifier = identifier

    def getIds(self):
        if type(self.identifier) == list:
            return self.identifier
        my_list = list()
        my_list.append(self.identifier)
        return my_list


class Publication(IdentifiableEntity):
    def __init__(self, identifier, publicationYear, title, cited, authors, publicationVenue):
        super().__init__(identifier)
        self.publicationYear = publicationYear
        self.title = title
        self.cited = cited
        self.authors = authors
        self.publicationVenue = publicationVenue

    # def __str__(self):
    #     result = list()
    #     result.append('Publication:')
    #     result.append(str(self.identifier))
    #     result.append('publication year: ' + str(self.publicationYear))
    #     result.append('title: ' + str(self.title))
    #     result.append('authors: ' + str(self.authors))
    #     result.append('publication venue: ' + str(self.publicationVenue))
    #     if len(self.cited) > 0:
    #         result.append('cited publications:' + str(self.cited))
    #     else:
    #         result.append('cited publications: None')
    #     return result

    def getPublicationYear(self):
        return self.publicationYear

    def getTitle(self):
        return self.title

    def getCitedPublications(self):
        return self.cited

    def getPublicationVenue(self):
        return self.publicationVenue

    def getAuthors(self):
        return self.authors


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
