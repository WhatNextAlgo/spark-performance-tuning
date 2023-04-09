package part4Partitioning

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}
import org.apache.spark.sql.SparkSession
import scala.util.Random
object Partitioners extends App {
  val spark = SparkSession.builder()
    .appName("Partitioners")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val sc = spark.sparkContext

  val numbers = sc.parallelize(1 to 10000)
  println(numbers.partitioner) // None

  val numbers3 = numbers.repartition(3) // random data redistribution
  println(numbers3.partitioner) // None

  // keep track of the partitioner
  // KV RDDs can control the partitioner scheme
  val keyedNumbers = numbers.map(n => (n % 10,n)) // RDD[(Int,Int)]
  val hashedNumbers = keyedNumbers.partitionBy(new HashPartitioner(4))
  hashedNumbers.groupByKey().mapValues(s => s.map(_.toInt).sum).collect.foreach(println)
  /*
      keys with the same hash stay on the same partition
      Prerequisite for
        - combineByKey
        - groupByKey
        - aggregateByKey
        - foldByKey
        - reduceByKey
      Prereq for joins, when neither RDD has a known partitioner.
    */
  val rangedNumbers = keyedNumbers.partitionBy(new RangePartitioner(5, keyedNumbers))
  /*
    Keys within the same range will be on the same partitioner.
    For a spectrum 0-1000
    keys between Int.MinValue-200 => partition 0
    keys between 200-400 => partition 1
    keys between 400-600 => partition 2
    keys between 600-800 => partition 3
    keys between 800-Int.MaxValue => partition 4
    RangePartitioner is a prerequisite for a SORT.
   */
  rangedNumbers.sortByKey() // NOT incur a shuffle
  println("---------------------------")
  rangedNumbers.groupByKey().mapValues(s => s.map(_.toInt).sum).collect.foreach(println)
  // define your own partitioner

  def generateRandomWords(nWords:Int,maxLength:Int) = {
    val r = new Random
    (1 to nWords).map( _ => r.alphanumeric.take(r.nextInt(maxLength)).mkString(""))
  }

  val randomWordsRDD = sc.parallelize(generateRandomWords(1000, 100))
  // repartition this RDD by the words length == two words of the same length will be on the same partition
  // custom computation = counting the occurrences of 'z' in every word
  val zWordsRDD = randomWordsRDD.map(word => (word,word.count(_ == 'z')))
  zWordsRDD.collect.foreach(println)

  class ByLengthPartitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = {
      key.toString.length % numPartitions
    }
  }

  val byLengthZWords = zWordsRDD.partitionBy(new ByLengthPartitioner(100))

  byLengthZWords.foreachPartition(_.foreach(println))

}
