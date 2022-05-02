from AdditionalClasses import *
graph1 = TriplestoreProcessor("http://127.0.0.1:9999/blazegraph")
print(graph1.getEndPoint())
graph2 = TriplestoreDataProcessor("http://127.0.0.1:9999/blazegraph")
print(graph2.uploadData("graph_publications2.csv"))
print(graph2.uploadData("graph_other_data.json"))
# from importlib import reload
# import tripleStoreQueryProcessor
# reload(tripleStoreQueryProcessor)
# from tripleStoreQueryProcessor import *
# graph3 = TriplestoreQueryProcessor("http://127.0.0.1:9999/blazegraph")
# print(graph3.getPublicationsPublishedInYear())