package part2foundations
import org.apache.spark.sql.SparkSession

object SparkJobAnatomy extends App{

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Spark Job Anatomy")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val sc = spark.sparkContext

  // job 1 - a count
  val rdd1 = sc.parallelize(1 to 1000000)
  rdd1.count
  // inspect the UI, one stage with 1 tasks
  // task = a unit of computation applied to a unit of data (a partition)
  Thread.sleep(10000)
  // job 2 - a count with a small transformation
  rdd1.map(_ * 2).count
  // inspect the UI, another job with (still) one stage, 1 tasks
  // all parallelizable computations (like maps) are done in a single stage
  println(rdd1.toDebugString)
  // job 3 - a count with a shuffle
  rdd1.repartition(23).count
  // UI: 2 stages, one with 1 tasks, one with 23 tasks
  // each stage is delimited by shuffles

  println(rdd1.toDebugString)

  // job 4, a more complex computation: load a file and compute the average salary of the employees by department
  val employees = sc.textFile("src/main/resources/data/employees/employees.txt")
  employees.collect.foreach(println)
  // process the lines
  val empTokens = employees.map(line => line.split(","))
  empTokens.collect.foreach(println)
  // extract relevant data
  val empDetails = empTokens.map(tokens => (tokens(4),tokens(7)))
  empDetails.collect.foreach(println)
  // group the elements
  val empGroups = empDetails.groupByKey(2)
  empGroups.collect.foreach(println)

  Thread.sleep(10000)
  // process the values associated to each group
  val avgSalaries = empGroups.mapValues(salaries => salaries.map(_.toInt).sum / salaries.size)
  // show the result
  Thread.sleep(10000)
  avgSalaries
    .collect() // this is an action
    .foreach(println)

  Thread.sleep(10000)
  // look at the Spark UI: one job, 2 stages
  // the groupByKey triggers a shuffle, and thus the beginning of another stage
  // all other computations (maps, mapValues) are done in their respective stage
  // the number of tasks = the number of partitions processed in a given stage
}
