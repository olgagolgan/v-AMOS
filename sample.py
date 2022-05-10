class IdentifiableEntity:
    def __init__(self, identifier):
        self.identifier = identifier

    def getIds(self):
        idList = self.identifier.split(", ")
        return idList
        
class Venue(IdentifiableEntity):
    def __init__(self, identifier, title, publisher):
        super().__init__(identifier)
        self.title = title
        self.publisher = publisher
    
    def __str__(self):
        result = list()
        result.append('Venue:')
        result.append('identifier: ' + str(self.identifier))
        result.append('title: ' + str(self.title))
        result.append('publisher: ' + str(self.publisher))
        return str(result)
        
    def getTitle(self):
        return self.title

    def getPublisher(self):
        publisher = Organization(self.publisher[0], self.publisher[1])
        return publisher

class Organization(IdentifiableEntity):
    def __init__(self, identifier, name):
        super().__init__(identifier)
        self.name = name
    
    def __str__(self):
        result = list()
        result.append('Organization:')
        result.append('identifier: ' + str(self.identifier))
        result.append('name: ' + str(self.name))
        return str(result)

    def getName(self):
        return self.name

venueSamp = ["issn:2641-3337, issn:1588-2861", "Quantitative Science Studies", ["crossref:281", "MIT Press - Journals"]]

def printInfo(venue):
    my_venue = Venue(venueSamp[0],venueSamp[1],venueSamp[2])
    my_id = my_venue.getIds()
    my_name = my_venue.getTitle()
    my_pub = my_venue.getPublisher()
    pubId = my_pub.getIds()
    pubName = my_pub.getName()
    info = """
    The object is: {0}
    with id: {1}
    with name: {2}
    published by: {3}, which is an object
    with id: {4}
    with name: {5}""".format(my_venue, my_id, my_name, my_pub, pubId, pubName)
    return info

print(printInfo(venueSamp))