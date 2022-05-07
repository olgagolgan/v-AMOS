from dataModelClasses import *
from triplestoreProcessorClasses import *
graph1 = TriplestoreProcessor("http://127.0.0.1:9999/blazegraph")
print(graph1.getEndPoint())
graph2 = TriplestoreDataProcessor("http://127.0.0.1:9999/blazegraph")
print(graph2.uploadData("graph_publications.csv"))
print(graph2.uploadData("graph_other_data.json"))
from importlib import reload
reload(TriplestoreQueryProcessor)
graph3 = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph")
print(graph3.getPublicationsPublishedInYear(2020))