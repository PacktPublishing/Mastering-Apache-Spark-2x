
// import packages

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

// define the application 

object graph2 extends App
{

  // set up hdfs server and configuration

  val hdfsServer = "hdfs://hc2nn.semtech-solutions.co.nz:8020"
  val hdfsPath   = "/data/spark/graphx/"

  val vertexFile = hdfsServer + hdfsPath + "graph1_vertex.csv"
  val edgeFile   = hdfsServer + hdfsPath + "graph1_edges.csv"

  val sparkMaster = "spark://hc2nn.semtech-solutions.co.nz:7077"
  val appName = "Graph 2"
  val conf = new SparkConf()

  conf.setMaster(sparkMaster)
  conf.setAppName(appName)

  // create the spark context

  val sparkCxt = new SparkContext(conf)

  // load the edges and vertices files 

  val vertices: RDD[(VertexId, (String, String))] =
      sparkCxt.textFile(vertexFile).map { line =>
        val fields = line.split(",")
        ( fields(0).toLong, ( fields(1), fields(2) ) )
  }

  val edges: RDD[Edge[String]] =
      sparkCxt.textFile(edgeFile).map { line =>
        val fields = line.split(",")
        Edge(fields(0).toLong, fields(1).toLong, fields(2))
  }

  // create graph from edges and vertices

  val default = ("Unknown", "Missing")

  val graph = Graph(vertices, edges, default)

  // example filtering 

  val c1 = graph.vertices.filter { case (id, (name, age)) => age.toLong > 40 }.count

  val c2 = graph.edges.filter { case Edge(from, to, property) 
    => property == "Father" | property == "Mother" }.count

  println( "Vertices count : " + c1 )
  println( "Edges    count : " + c2 )


} // end application
