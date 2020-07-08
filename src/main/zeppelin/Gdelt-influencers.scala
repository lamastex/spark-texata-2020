val inputGkg = "s3://texata-round2/gdelt/gkg"
val inputEvent = "s3://texata-round2/gdelt/events"

val eventDS = spark.read.gdeltEVENT(inputEvent)
val gkgDS = spark.read.gdeltGKG(inputGkg)

import com.aamend.texata._
val personRDD = gkgDS
    .filter(c => {
        c.themes.contains("ENV_GAS") || c.themes.contains("ENV_OIL")
    })
    .flatMap(f => {
        f
        .persons
        .flatMap(p1 => {
            f
            .persons
            .filter(p2 => p2 != p1)
            .map(p2 => {
                ((p1, p2), f.numArticles.toLong)
            })
        })
    })
    .rdd
    .reduceByKey(_+_)
    .cache()

val graph = personRDD.toGraph()
graph.cache

graph.vertices.count
graph.edges.count
graph.degrees.values.sum / graph.vertices.count

import org.apache.spark.graphx.{EdgeTriplet, Graph}
val subgraph = graph
    .subgraph(
        (et: EdgeTriplet[String, Long]) => et.attr > 1000,(_, vData: String) => true
    )
    
val subGraphDeg = subgraph.outerJoinVertices(subgraph.degrees)((vId, vData, vDeg) => {
        (vData, vDeg.getOrElse(0))
    })
    .subgraph(
        (et: EdgeTriplet[(String, Int), Long]) => et.srcAttr._2 > 0 && et.dstAttr._2 > 0,
        (_, vData: (String, Int)) => vData._2 > 0
    )
    .mapVertices({ 
        case (vId, (vData, vDeg)) =>
            vData
    })
    .cache()
    
subGraphDeg.vertices.count
subGraphDeg.edges.count
subGraphDeg.degrees.values.sum / subGraphDeg.vertices.count

val wccGraph = subGraphDeg.louvain().cache()

val prVertices = wccGraph.pageRank(0.15).vertices
val prGraph = wccGraph.outerJoinVertices(prVertices)((_, vData, vPr) => {
    (vData._1, vData._2, vPr.getOrElse(0.15))
})

prVertices.toDF().show

prGraph.vertices.values.toDF("person", "community", "pageRank").cache()

val vertices = prGraph.vertices.values.toDF("person", "community", "pageRank").cache()
vertices.createOrReplaceTempView("gkg")
sqlContext.cacheTable("gkg")

vertices.filter(col("community") === -2209714333319565875L).select("person").show

vertices.orderBy(desc("pageRank")).filter(col("")).select("person").show(1000)

def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
}
    
printToFile(new java.io.File("/tmp/edges.csv")) { p =>
        prGraph.edges.map(e => {
            Array(e.srcId, e.dstId, e.attr).mkString(",")
        })
        .collect()
        .foreach(p.println)
    }

printToFile(new java.io.File("/tmp/nodes.csv")) { p =>
        prGraph.vertices.map(e => {
            Array(e._1, e._2._1, e._2._2, e._2._3).mkString(",")
        })
        .collect()
        .foreach(p.println)
    }