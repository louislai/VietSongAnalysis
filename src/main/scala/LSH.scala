package main.scala



import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
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
    val nStart = 1
    val nEnd = 2
    val nRange = Range(nStart, nEnd + 1)
    val multipleCvDf = nRange.foldLeft[DataFrame](wordDf)((prev, i) => {
      val nGram = new NGram().setN(i).setInputCol("words").setOutputCol(s"${i}_grams")
      val nGramDf = nGram.transform(prev)
      val ngramCv = new CountVectorizer().setInputCol(s"${i}_grams").setOutputCol(s"${i}_features")
      val ngramCvModel = ngramCv.fit(nGramDf)
      ngramCvModel.transform(nGramDf)
    })

    val isNoneZeroVector = udf({v: Vector => v.numNonzeros > 0}, DataTypes.BooleanType)

    // Assemble n-gram count vectorizer
    val assembler = new VectorAssembler()
      .setInputCols(nRange.map(i => s"${i}_features").toArray)
      .setOutputCol("features")

    val cvDf = assembler.transform(multipleCvDf)
      .filter(isNoneZeroVector(col("features")))
      .select("song_id", "name", "artist", "features")
    cvDf.show()
    // 274572 features

    val numRow = 1000
    val _hashFunctions = ListBuffer[Hasher]()
    for (i <- 0 until numRow)
      _hashFunctions += Hasher.create()
    val hashFunctions : List[(Hasher, Int)] = _hashFunctions.toList.zipWithIndex
    val minHashUdf = udf((x: Vector) => hashFunctions.map(_._1.minhash(x.toSparse)))
    val mhDf = cvDf.withColumn("hashes", minHashUdf(col("features"))).drop("features")

    val songIds = Array("yêu lại từ đầu")
    val searchSongs = mhDf.filter(col("name").isin(songIds:_*)).select(col("hashes")).collect()
      .map(row => row.getAs[WrappedArray[Int]](0))

    val jaccardSearchSongsUdf = udf((x: WrappedArray[Int]) => jaccard(x, searchSongs))

    val jacDf = mhDf.filter(!col("name").isin(songIds:_*))
      .withColumn("jaccard", jaccardSearchSongsUdf(col("hashes")))

    jacDf.orderBy(col("jaccard").desc)
      .select("name", "artist", "jaccard")
      .toJavaRDD.saveAsTextFile("file:///Users/louis/VietSongAnalysis/yeulaitudau")

    val composers = Array("khắc việt")
    val composerSongs =  mhDf.filter(col("artist").isin(composers:_*)).select(col("hashes")).collect()
      .map(row => row.getAs[WrappedArray[Int]](0))

    val jaccardComposerSongsUdf = udf((x: WrappedArray[Int]) => jaccard(x, composerSongs))
    val artistResultDf = mhDf.filter(!col("artist").isin(composers:_*))
      .withColumn("jaccard", jaccardComposerSongsUdf(col("hashes")))
      .groupBy("artist").agg((sum("jaccard") / count("*")).alias("mean_jaccard"))
      .orderBy(col("mean_jaccard").desc)
      .toJavaRDD.saveAsTextFile("file:///Users/louis/VietSongAnalysis/khacviet")


  }
}
