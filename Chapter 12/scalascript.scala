spark-shell --packages graphframes:graphframes:0.5.0-spark2.1-s_2.11

import org.graphframes._

val vertex = spark.read.option("header","true").csv("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter12/graph1_vertex.csv")

val edges = spark.read.option("header","true").csv("/Users/romeokienzler/Documents/romeo/Dropbox/arbeit/spark/sparkbuch/mywork/chapter12/graph1_edges.csv")

val graph = GraphFrame(vertex, edges)

graph.vertices.count
graph.edges.count

graph.vertices.filter("attr > 40").show

graph.find("(A)-[edge:Mother]->(B)").show

GraphFrame(vertex, graph.edges.filter("attr=='Mother'")).show

//Pagerank
val results = graph.pageRank.resetProbability(0.15).tol(0.01).run()

results.vertices.orderBy($"pagerank".desc).show

//Triangle count
val results = graph.triangleCount.run()
results.select("id", "count").show()

//Connected components
//this directory needs to be created before
//sc.setCheckpointDir("/tmp/spark2.11checkpointdir")
sc.setCheckpointDir("/Users/romeokienzler/Downloads/spark2.11checkpointdir")
val result = graph.connectedComponents.run()
result.select("id", "component").orderBy("component").show()

//Strongly connected components

val result = graph.stronglyConnectedComponents.maxIter(10).run()
result.select("id", "component").orderBy("component").show()