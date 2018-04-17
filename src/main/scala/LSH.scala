package main.scala



import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.types.DataTypes

import scala.collection.mutable.WrappedArray
import scala.collection.mutable.ListBuffer

object LSH {

  def jaccard(x: WrappedArray[Int], yArr: Array[WrappedArray[Int]]): Double = {
    val N = yArr.length.toFloat
    val total = yArr.map(y => x.intersect(y).distinct.length.toFloat / x.union(y).distinct.length
      .toFloat).sum
    total / N
  }

  def main(args: Array[String]) {

    //Create a SparkContext to initialize Spark
    val sparkSession = SparkSession.builder.
      master("local[1]")
      .appName("LSH")
      .getOrCreate()
    val sc = sparkSession.sparkContext
    val sql = sparkSession.sqlContext

    val songs = sql.read.format("csv").option("header", "true")
      .load("file:///Users/louis/VietSongAnalysis/csv/songs.csv")

    val tokenizer = new Tokenizer().setInputCol("lyric").setOutputCol("words")
    val wordDf = tokenizer.transform(songs).drop("lyric")


    val twogram = new NGram().setN(2).setInputCol("words").setOutputCol("twograms")
    val twogramDf = twogram.transform(wordDf)
    val twogramcv = new CountVectorizer().setInputCol("twograms").setOutputCol("features_2gram")
    val twogramcvModel = twogramcv.fit(twogramDf)

    // Buggy song: 11396, 8387
    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)
    val twogramcvDf = twogramcvModel.transform(twogramDf).drop("twograms")
      .filter(isNoneZeroVector(col("features_2gram")))

    val onegram = new NGram().setN(1).setInputCol("words").setOutputCol("onegrams")
    val onegramDf = onegram.transform(twogramcvDf).drop("words")

    val onegramcv = new CountVectorizer().setInputCol("onegrams").setOutputCol("features_1gram")
    val onegramcvModel = onegramcv.fit(onegramDf)
    val onegramcvDf = onegramcvModel.transform(onegramDf).drop("onegrams")
      .filter(isNoneZeroVector(col("features_1gram")))

    // Assemble 1-gram and 2-gram
    val assembler = new VectorAssembler()
      .setInputCols(Array("features_1gram", "features_2gram"))
      .setOutputCol("features")

    val cvDf = assembler.transform(onegramcvDf)
    cvDf.show()

    val numRow = 100
    val _hashFunctions = ListBuffer[Hasher]()
    for (i <- 0 until numRow)
      _hashFunctions += Hasher.create()
    val hashFunctions : List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex
    val minHashUdf = udf((x: Vector) => hashFunctions.map(_._1.minhash(x.toSparse)))
    val mhDf = cvDf.withColumn("hashes", minHashUdf(col("features"))).drop("features")

    val songIds = Array("bức thư tình đầu tiên", "bức thư tình thứ hai")
    val searchSongs = mhDf.filter(col("name").isin(songIds:_*)).select(col("hashes")).collect()
      .map(row => row.getAs[WrappedArray[Int]](0))

    val jaccardSearchSongsUdf = udf((x: WrappedArray[Int]) => jaccard(x, searchSongs))

    val jacDf = mhDf.filter(!col("name").isin(songIds:_*))
      .withColumn("jaccard", jaccardSearchSongsUdf(col("hashes")))

    jacDf.orderBy(col("jaccard").desc).select(col("name")).show()

    val composers = Array("trịnh công sơn")
    val composerSongs =  mhDf.filter(col("artist").isin(composers:_*)).select(col("hashes")).collect()
      .map(row => row.getAs[WrappedArray[Int]](0))

    val jaccardComposerSongsUdf = udf((x: WrappedArray[Int]) => jaccard(x, composerSongs))
    val artistResultDf = mhDf.filter(!col("artist").isin(composers:_*))
      .withColumn("jaccard", jaccardComposerSongsUdf(col("hashes")))
      .groupBy("artist").agg((sum("jaccard") / count("*")).alias("mean_jaccard"))
      .orderBy(col("mean_jaccard").desc).show()


  }
}
