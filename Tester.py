from dataModelClasses import *
from relationalProcessorClasses import *
from triplestoreProcessorClasses import *
from genericQueryProcessor import *

# ================ GENERIC

#1st query
# my_m1 = generic.getPublicationsPublishedInYear(2020)
# print(my_m1)
# print("-----------------------------------")
# print(my_m1[1][0].getAuthor())
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
# print(my_m3[1].getAuthors())
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
# print(my_m5[1][0].getPublisher())
# print("-----------------------------------")

#6th query 
# my_m6 = generic.getPublicationInVenue("issn:0944-1344")
# print(my_m6)
# print("-----------------------------------")
# print(my_m6[1][0].getTitle())
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

# 12th query
# my_m12 = generic.getDistinctPublisherOfPublications([ "doi:10.1080/21645515.2021.1910000", "doi:10.3390/ijfs9030035" ])
# print(my_m12)
# print("-----------------------------------")
# print(my_m12[1][0].getName())

