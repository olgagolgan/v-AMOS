#Class: Venue

class Venue(IdentifiableEntity):
    def __init__(self, title, publisher):
        self.title = title
        self.publisher = publisher
        super().__init__(self, identifiers)
    
    def getTitle(self):
        return self.title
    
    def getPublisher(self):
        return self.publisher

#Subclass: 1) Journal; 2) Book; 3) Proceedings

class Journal(Venue):
    pass

class Book(Venue):
    pass
    
class Proceedings(Venue):
    def __init__(self, event):
        self.event = event
        super().__init__(self, title, publisher)  #Non devo fare quello per prendere il getId da Identifiable Entity, visto l'ho già richiamata in Venue (r. 12)?

    def getEvent(self):
        return self.event