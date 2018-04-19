package main.scala



import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.graphx.lib.PageRank
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object SimRank {

  def pr(graph: Graph[String, Double], sourceVertices: Set[VertexId], tol: Double, resetProb: Double
  = 0.15, maxStep: Int = -1)
  : Graph[Double, Double] = {
    // Initialize the pagerankGraph with each edge attribute
    // having weight 1/outDegree and each vertex with attribute 1.0.
    val pagerankGraph: Graph[(Double, Double, Int, Boolean), Double] = graph
      // Associate the degree with each vertex
      .outerJoinVertices(graph.outDegrees) {
      (vid, vdata, deg) => deg.getOrElse(0)
    }
      // Set the weight on the edges based on the degree
      .mapTriplets( e => 1.0 / e.srcAttr )
      // Set the vertex attributes to (initialPR, delta = 0, step = 0, isSrc)
      .mapVertices { (id, attr) =>
      if (sourceVertices contains id) (0.0, 0.0, 0, true) else (0.0, 0.0, 0, false)
    }
      .cache()

    val isPersonalized = !sourceVertices.isEmpty
    val N = graph.numVertices
    val numSrc = (if (isPersonalized) sourceVertices.size else N).toDouble

    def personalizedVertexProgram(id: VertexId, attr: (Double, Double, Int, Boolean),
                                  msgSum: Double): (Double, Double, Int, Boolean) = {
      val (oldPR, lastDelta, step, isSrc) = attr

      val newPR = if (step == 0) {
        1.0 / N.toDouble
      } else if (isSrc) {
        (1.0 - resetProb) * msgSum + resetProb / numSrc
      } else {
        (1.0 - resetProb) * msgSum
      }
      (newPR, newPR - oldPR, step + 1, isSrc)
    }

    def vertexProgram(id: VertexId, attr: (Double, Double, Int, Boolean),
                      msgSum: Double): (Double, Double, Int, Boolean) = {
      val (oldPR, lastDelta, step, isSrc) = attr

      val newPR = if (step == 0) {
        1.0 / N.toDouble
      } else {
        (1.0 - resetProb) * msgSum + resetProb / numSrc
      }
      (newPR, newPR - oldPR, step + 1, isSrc)
    }

    def sendMessage(edge: EdgeTriplet[(Double, Double, Int, Boolean), Double]) = {
      if (edge.srcAttr._2 <= tol ||(maxStep != -1 && edge.srcAttr._3 < maxStep)) {
        Iterator.empty
      } else {
        Iterator((edge.dstId, edge.srcAttr._1 * edge.attr))
      }
    }

    def messageCombiner(a: Double, b: Double): Double = a + b

    // The initial message received by all vertices in PageRank
    val initialMessage: Double = 1.0 / N.toDouble

    // Execute a dynamic version of Pregel.
    val vp = if (isPersonalized) {
      (id: VertexId, attr: (Double, Double, Int, Boolean), msgSum: Double) =>
        personalizedVertexProgram(id, attr, msgSum)
    } else {
      (id: VertexId, attr: (Double, Double, Int, Boolean), msgSum: Double) =>
        vertexProgram(id, attr, msgSum)
    }

    val rankGraph = Pregel(pagerankGraph, initialMessage, activeDirection = EdgeDirection.Out)(
      vp, sendMessage, messageCombiner)
      .mapVertices((vid, attr) => attr._1)
    rankGraph
  }

  def dfZipWithIndex(
                      df: DataFrame,
                      offset: Long,
                      colName: String
                    ) : DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          Seq()
            ++ ln._1.toSeq ++
            Seq(ln._2 + offset)
        )
      ),
      StructType(
        Array[StructField]()
          ++ df.schema.fields
          ++ Array(StructField(colName,LongType, false))
      )
    )
  }

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val sparkSession = SparkSession.builder.
      master("local[1]")
      .appName("SimRank")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext

    val songs = sql.read.format("csv").option("header", "true")
      .load("file:///Users/louis/VietSongAnalysis/csv/songs.csv")
    val performers = sql.read.format("csv").option("header", "true")
      .load("file:///Users/louis/VietSongAnalysis/csv/performers.csv")
    //    val genres = sql.read.format("csv").option("header", "true")
    //      .load("file:///Users/louis/VietSongAnalysis/csv/genres.csv")


    val songsWithIndex = dfZipWithIndex(songs.select("name"), offset = 1L, colName = "songVertexId")
      .select("name", "songVertexId")

    var maxOffsetSoFar = songsWithIndex.select(max(col("songVertexId"))).first().getAs[Long](0)

    val artistWithIndex = dfZipWithIndex(songs.select("artist").distinct(), offset = maxOffsetSoFar,
      colName = "artistVertexId")

    maxOffsetSoFar = artistWithIndex.select(max(col("artistVertexId"))).first().getAs[Long](0)

    val performerWithIndex = dfZipWithIndex(performers.select("performer").distinct(), offset =
      maxOffsetSoFar,
      colName = "performerVertexId")

    //    maxOffsetSoFar = performerWithIndex.select(max(col("performerVertexId"))).first().getAs[Long](0)
    //
    //    val genreWithIndex = dfZipWithIndex(genres.select("genre").distinct(), offset =
    //      maxOffsetSoFar,
    //      colName = "genreVertexId")

    // Vertices

    val toVertices: DataFrame => RDD[(VertexId, String)] = df => df
      .rdd.map(row => (row(1).asInstanceOf[Number].longValue, row(0).asInstanceOf[String]))
    val songVertices = toVertices(songsWithIndex)
    val artistVertices = toVertices(artistWithIndex)
    val performerVertices = toVertices(performerWithIndex)

    val vertices: RDD[(VertexId, String)] = Seq(songVertices, artistVertices, performerVertices)
      .reduce((a, b) => a ++ b)


    val songArtistRelationships = songs.join(songsWithIndex, "name")
      .join(artistWithIndex, "artist").select("songVertexId", "artistVertexId").distinct()


    val songPerformerRelationship = songs.join(songsWithIndex, "name")
      .join(performers, "song_id")
      .join(performerWithIndex, "performer").select("songVertexId", "performerVertexId")
      .distinct()

    // Edges
    val edges: RDD[Edge[Double]] = Seq(songArtistRelationships, songPerformerRelationship)
      .map(df => df.rdd.flatMap(row => Seq(
        Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue, 1.0),
        Edge(row(1).asInstanceOf[Number].longValue, row(0).asInstanceOf[Number].longValue, 1.0))))
      .reduce((a, b) => a ++ b)

    val defaultNode = ("Missing Node")
    val graph = Graph(vertices, edges, defaultNode)
    graph.cache()

    //    val pageranks = pr(graph, Set.empty, 0.0001).vertices
    val pageranks = graph.pageRank(0.001).vertices
    pageranks
      .join(artistVertices)
      .sortBy(_._2._1, ascending=false) // sort by the rank
      .toJavaRDD()
      .saveAsTextFile("file:///Users/louis/VietSongAnalysis/composer.csv")


    val composers = Array("khắc việt")
    val sourceVertices = artistWithIndex.filter(col("artist").isin(composers:_*)).rdd
      .map(row => row(1).asInstanceOf[Number].longValue).collect().toArray

    val ranks = graph.personalizedPageRank(sourceVertices.head, 0.0001).vertices
    ranks
      .join(artistVertices)
      .sortBy(_._2._1, ascending=false) // sort by the rank
      .toJavaRDD()
      .saveAsTextFile("file:///Users/louis/VietSongAnalysis/p_khacviet")

    val sourceSongs = Array("yêu lại từ đầu")
    val songsVertices = songsWithIndex.filter(col("name").isin(sourceSongs:_*)).rdd
      .map(row => row(1).asInstanceOf[Number].longValue).collect().toArray

    val songranks = graph.personalizedPageRank(songsVertices.head, 0.0001).vertices
    songranks
      .join(songVertices)
      .sortBy(_._2._1, ascending=false) // sort by the rank
      .toJavaRDD()
      .saveAsTextFile("file:///Users/louis/VietSongAnalysis/p_yeulaitudau")

    //    def mean(xs: Iterable[Double]) = xs.sum / xs.size
    //    val ranks = graph.staticParallelPersonalizedPageRank(sourceVertices.toArray, 5).vertices
    //        ranks
    //          .join(artistVertices)
    //          .sortBy(mean(_._2._1.values), ascending=false) // sort by the rank
    //          .take(10) // get the top 10
    //          .foreach(x => println(x._2._2, x._1))
    // Reset prob 0.15
    // Top 10 page rank
    //    various artists
    //      nhạc ngoại
    //      đàm vĩnh hưng
    //    nguyễn hồng thuận
    //    hoài an
    //      viễn châu
    //      trịnh công sơn
    //    nguyễn văn chung
    //    khánh đơn
    //      nhạc hoa


    //
    //    val ranks = pr(graph, sourceVertices, 0.001).vertices
    //    ranks
    //        .join(artistVertices)
    //        .sortBy(_._2._1, ascending=false) // sort by the rank
    //        .take(10) // get the top 10
    //        .foreach(x => println(x._2._2, x._1, x._2._1))
  }
}
