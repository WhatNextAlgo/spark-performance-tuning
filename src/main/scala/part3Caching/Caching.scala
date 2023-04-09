package part3Caching

import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
object Caching extends App {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", 10000000)
    .appName("Caching")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val flightsDF = spark.read.format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/flights")

  println(flightsDF.count())

  Thread.sleep(10000)

  // simulate an "expensive" operation
  val orderedFlightsDF = flightsDF.orderBy("dist")

  // scenario: use this DF multiple times
  orderedFlightsDF.persist(
    // no argument = MEMORY_AND_DISK
    // StorageLevel.MEMORY_ONLY // cache the DF in memory EXACTLY - CPU efficient, memory expensive
    // StorageLevel.DISK_ONLY // cache the DF to DISK - CPU efficient and mem efficient, but slower
    // StorageLevel.MEMORY_AND_DISK // cache this DF to both the heap AND the disk - first caches to memory, but if the DF is EVICTED, will be written to disk

    /* modifiers: */
    // StorageLevel.MEMORY_ONLY_SER // memory only, serialized - more CPU intensive, memory saving - more impactful for RDDs
    // StorageLevel.MEMORY_ONLY_2 // memory only, replicated twice - for resiliency, 2x memory usage
    // StorageLevel.MEMORY_ONLY_SER_2 // memory only, serialized, replicated 2x

    /* off-heap */
    StorageLevel.OFF_HEAP // cache outside the JVM, done with Tungsten, still stored on the machine RAM, needs to be configured, CPU efficient and memory efficient
  )
  println(orderedFlightsDF.count())
  println(orderedFlightsDF.count())
  Thread.sleep(10000)
  // remove from cache
  orderedFlightsDF.unpersist() // remove this DF from cache

  // change cache name
  orderedFlightsDF.createOrReplaceTempView("orderedFlights")
  spark.catalog.cacheTable("orderedFlights")
  orderedFlightsDF.count()

  // RDDs
  val flightsRDD = orderedFlightsDF.rdd
  flightsRDD.persist(StorageLevel.MEMORY_ONLY_SER)
  flightsRDD.count()

}
