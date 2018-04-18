import org.apache.spark.ml.feature.{NGram, RegexTokenizer, Tokenizer}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.StopWordsRemover

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

  // Change this dir
  val OUTPUT_DIR = "/home/hadoop/spark-data/output"

  // df is DF with columns "content" as the paragraph we want to analyse
  def countTopWords(df: DataFrame, stopWords: Array[String], topCount: Int, outDir: String): Unit = {
    val remover = new StopWordsRemover()
      .setStopWords(stopWords)
      .setInputCol("words")
      .setOutputCol("filteredWords")

    val tokenizer = new Tokenizer().setInputCol("content").setOutputCol("words")
    var oneGramDF = tokenizer.transform(df).select("words")
    oneGramDF = remover.transform(oneGramDF).select(col("filteredWords").alias("words"))

    val twogram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")
    var twoGramDF = twogram.transform(oneGramDF).select(col("ngrams").alias("words"))

    twoGramDF = remover.transform(twoGramDF).select(col("filteredWords").alias("words"))

    val threengram = new NGram().setN(3).setInputCol("words").setOutputCol("ngrams")
    var threeGramDF = threengram.transform(oneGramDF).select(col("ngrams").alias("words"))

    threeGramDF = remover.transform(threeGramDF).select(col("filteredWords").alias("words"))


    val l1 = oneGramDF.withColumn("words", explode(oneGramDF("words")))
      .groupBy("words")
      .count()
      .sort(desc("count"))

    val l2 = twoGramDF.withColumn("words", explode(twoGramDF("words")))
      .groupBy("words")
      .count()
      .sort(desc("count"))

    val l3 = threeGramDF.withColumn("words", explode(threeGramDF("words")))
      .groupBy("words")
      .count()
      .sort(desc("count"))

    l1.show(topCount)
    l2.show(topCount)
    l3.show(topCount)

    l1.limit(topCount)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
//      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(s"$outDir/1_word.csv")
    l2.limit(topCount)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
//      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(s"$outDir/2_word.csv")
    l3.limit(topCount)
      .write
      .format("com.databricks.spark.csv")
      .option("header", "true")
//      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save(s"$outDir/3_word.csv")
  }

  def countLyricWords(songDF: DataFrame, stopWords: Array[String]): Unit = {
    val df = songDF.select(col("lyric").alias("content"))
    val outDir = s"$OUTPUT_DIR/lyric"
    countTopWords(df, stopWords, 50, outDir)
  }

  def countGenreLyricWords(genresDF: DataFrame, songsDF: DataFrame, stopWords: Array[String]) {
    val topGenres =
      genresDF
        .groupBy("genre")
        .count()
        .sort(desc("count"))
        .collect()
        .map(g => g.get(0))
        .filter(x => x != null)

    topGenres.foreach(x => {
      val genre = x.toString()
      val df = genresDF
        .filter(col("genre") === genre)
        .join(songsDF, "song_id")
        .select(col("lyric").alias("content"))
      val outDir = s"$OUTPUT_DIR/genre/$genre"
      countTopWords(df, stopWords, 50, outDir)
    })
  }

  def topPerformers(df: DataFrame, outDir: String) {
    val data = df.groupBy("performer")
      .count()
      .sort(desc("count"))
      .select("performer", "count")
      .limit(50)

    data.show()

    data.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save(outDir)
  }

  def topAllPerformers(performersDF: DataFrame): Unit = {
    val outDir = s"$OUTPUT_DIR/performer/top-performer.csv"
    topPerformers(performersDF, outDir)
  }

  def topPerformersForGenres(performersDF: DataFrame, genresDF: DataFrame) {
    val topGenres =
      genresDF
        .groupBy("genre")
        .count()
        .sort(desc("count"))
        .take(10)
        .map(g => g.get(0))
        .filter(x => x != null)

    topGenres.foreach(x => {
      val genre = x.toString()
      val df = genresDF
        .filter(col("genre") === genre)
        .join(performersDF, "song_id")
        .select("performer")
      val outDir = s"$OUTPUT_DIR/performer/top-performer-per-genre/$genre.csv"
      topPerformers(df, outDir)
    })
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("song lyrics")
      .getOrCreate()

    val songsDF = spark.read.option("header", true).csv("/home/hadoop/spark-data/dataset/songs.csv")
    val performersDF = spark.read.option("header", true).csv("/home/hadoop/spark-data/dataset/performers.csv")
    val genresDF = spark.read.option("header", true).csv("/home/hadoop/spark-data/dataset/genres.csv")

    val stopWords = spark.sparkContext.textFile("/home/hadoop/spark-data/dataset/stopwords.txt").collect()

//    countLyricWords(songsDF, stopWords)
//    countGenreLyricWords(genresDF, songsDF, stopWords)
//    topAllPerformers(performersDF)
//    topPerformersForGenres(performersDF, genresDF)
  }
}
