package part2foundations

import org.apache.spark.sql.SparkSession
object ReadingDAGs extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Reading DAGs")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

}
