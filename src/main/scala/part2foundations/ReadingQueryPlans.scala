package part2foundations
import org.apache.spark.sql.SparkSession

object ReadingQueryPlans extends App{
  ///////////////////////////////////////////////////////////////////// Boilerplate
  // you don't need this code in the Spark shell
  // this code is needed if you want to run it locally in IntelliJ

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Reading Query Plans")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val sc = spark.sparkContext

  ///////////////////////////////////////////////////////////////////// Boilerplate

  // plan 1 - a simple transformation
  val simpleNumbers = spark.range(1,1000000).toDF("id")
  val times5 = simpleNumbers.selectExpr("id * 5 as id")
  times5.explain()//this is how you show query plan
  /*
    == Physical Plan ==
    *(1) Project [(id#0L * 5) AS id#4L]
    +- *(1) Range (1, 1000000, step=1, splits=1)
   */

  // plan 2 - a shuffle
  val moreNumbers = spark.range(1, 1000000, 2)
  val split7 = moreNumbers.repartition(7)

  split7.explain()
  /*
== Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=14]
     +- Range (1, 1000000, step=2, splits=1)
   */

  // plan 3 - shuffle + transformation
  split7.selectExpr("id * 5 as id").explain()
  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- Project [(id#6L * 5) AS id#10L]
     +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=25]
        +- Range (1, 1000000, step=2, splits=1)
   */

  // plan 4: a more complex job with a join
  val ds1 = spark.range(1,10000000).toDF("id")
  val ds2 = spark.range(1,20000000,2).toDF("id")
  val ds3 = ds1.repartition(7)
  val ds4 = ds2.repartition(9)
  val ds5 = ds3.selectExpr("id * 3 as id")
  val joined = ds5.join(ds4,"id")
  val sum = joined.selectExpr("sum(id)")
  sum.explain()

  /*
  == Physical Plan ==
  AdaptiveSparkPlan isFinalPlan=false
  +- HashAggregate(keys=[], functions=[sum(id#20L)])
     +- Exchange SinglePartition, ENSURE_REQUIREMENTS, [plan_id=72]
        +- HashAggregate(keys=[], functions=[partial_sum(id#20L)])
           +- Project [id#20L]
              +- SortMergeJoin [id#20L], [id#16L], Inner
                 :- Sort [id#20L ASC NULLS FIRST], false, 0
                 :  +- Exchange hashpartitioning(id#20L, 200), ENSURE_REQUIREMENTS, [plan_id=64]
                 :     +- Project [(id#12L * 3) AS id#20L]
                 :        +- Exchange RoundRobinPartitioning(7), REPARTITION_BY_NUM, [plan_id=54]
                 :           +- Range (1, 10000000, step=1, splits=1)
                 +- Sort [id#16L ASC NULLS FIRST], false, 0
                    +- Exchange hashpartitioning(id#16L, 200), ENSURE_REQUIREMENTS, [plan_id=65]
                       +- Exchange RoundRobinPartitioning(9), REPARTITION_BY_NUM, [plan_id=57]
                          +- Range (1, 20000000, step=2, splits=1)


   */
  /**
   * Exercises - read the Query Plans and try to understand the code that generated them.
   */

  // exercise 1
  /*
    == Physical Plan ==
    *(1) Project [firstName#153, lastName#155, (cast(salary#159 as double) / 1.1) AS salary_EUR#168]
    +- *(1) FileScan csv [firstName#153,lastName#155,salary#159] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/tmp/employees_headers.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<firstName:string,lastName:string,salary:string>
   */
  val employeesDF = spark.read.option("header", true).csv("/tmp/employees_headers.csv")
  val empEur = employeesDF.selectExpr("firstName", "lastName", "salary / 1.1 as salary_EUR")

  // exercise 2
  /*
  == Physical Plan ==
  *(2) HashAggregate(keys=[dept#156], functions=[avg(cast(salary#181 as bigint))])
    +- Exchange hashpartitioning(dept#156, 200)
      +- *(1) HashAggregate(keys=[dept#156], functions=[partial_avg(cast(salary#181 as bigint))])
        +- *(1) Project [dept#156, cast(salary#159 as int) AS salary#181]
          +- *(1) FileScan csv [dept#156,salary#159] Batched: false, Format: CSV, Location: InMemoryFileIndex[file:/tmp/employees_headers.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<dept:string,salary:string>
   */
  val avgSals = employeesDF
    .selectExpr("dept", "cast(salary as int) as salary")
    .groupBy("dept")
    .avg("salary")


  // exercise 3
  /*
  == Physical Plan ==
  *(5) Project [id#195L]
    +- *(5) SortMergeJoin [id#195L], [id#197L], Inner
      :- *(2) Sort [id#195L ASC NULLS FIRST], false, 0
      :  +- Exchange hashpartitioning(id#195L, 200)
      :     +- *(1) Range (1, 10000000, step=3, splits=6)
      +- *(4) Sort [id#197L ASC NULLS FIRST], false, 0
        +- Exchange hashpartitioning(id#197L, 200)
          +- *(3) Range (1, 10000000, step=5, splits=6)
   */
  val d1 = spark.range(1, 10000000, 3)
  val d2 = spark.range(1, 10000000, 5)
  val j1 = d1.join(d2, "id")


}
