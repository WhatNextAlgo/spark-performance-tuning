package part5Boost

import common._
import org.apache.spark.sql.{Dataset,SparkSession}
import org.apache.spark.sql.functions._

object FixingDataSkews{

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName(" Fixing Data Skews")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  // deactivate the broadcast joins
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
  import spark.implicits._

  val guitars: Dataset[Guitar] = Seq.fill(40000)(DataGenerator.randomGuitar()).toDS()
  val guitarSales: Dataset[GuitarSale] = Seq.fill(200000)(DataGenerator.randomGuitarSale()).toDS

  guitars.take(5).foreach(println)
  guitarSales.take(5).foreach(println)

  /*
      A Guitar is similar to a GuitarSale if
      - same make and model
      - abs(guitar.soundScore - guitarSale.soundScore) <= 0.1
      Problem:
      - for every Guitar, avg(sale prices of ALL SIMILAR GuitarSales)
      - Gibson L-00, config "sadfhja", sound 4.3,
        compute avg(sale prices of ALL GuitarSales of Gibson L-00 with sound quality between 4.2 and 4.4
     */

  def naiveSolution() = {
    val joined = guitars.join(guitarSales,Seq("make","model"))
      .where(abs(guitarSales("soundScore") - guitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    joined.count()
    joined.explain(true)
    /*
    == Parsed Logical Plan ==
    'Aggregate [configurationId#4], [configurationId#4, avg('salePrice) AS averagePrice#55]
    +- Filter (abs((soundScore#21 - soundScore#7)) <= 0.1)
       +- Project [make#5, model#6, configurationId#4, soundScore#7, registration#18, soundScore#21, salePrice#22]
          +- Join Inner, ((make#5 = make#19) AND (model#6 = model#20))
             :- LocalRelation [configurationId#4, make#5, model#6, soundScore#7]
             +- LocalRelation [registration#18, make#19, model#20, soundScore#21, salePrice#22]

    == Analyzed Logical Plan ==
    configurationId: string, averagePrice: double
    Aggregate [configurationId#4], [configurationId#4, avg(salePrice#22) AS averagePrice#55]
    +- Filter (abs((soundScore#21 - soundScore#7)) <= 0.1)
       +- Project [make#5, model#6, configurationId#4, soundScore#7, registration#18, soundScore#21, salePrice#22]
          +- Join Inner, ((make#5 = make#19) AND (model#6 = model#20))
             :- LocalRelation [configurationId#4, make#5, model#6, soundScore#7]
             +- LocalRelation [registration#18, make#19, model#20, soundScore#21, salePrice#22]

    == Optimized Logical Plan ==
    Aggregate [configurationId#4], [configurationId#4, avg(salePrice#22) AS averagePrice#55]
    +- Project [configurationId#4, salePrice#22]
       +- Join Inner, (((abs((soundScore#21 - soundScore#7)) <= 0.1) AND (make#5 = make#19)) AND (model#6 = model#20))
          :- LocalRelation [configurationId#4, make#5, model#6, soundScore#7]
          +- LocalRelation [make#19, model#20, soundScore#21, salePrice#22]

    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[configurationId#4], functions=[avg(salePrice#22)], output=[configurationId#4, averagePrice#55])
       +- Exchange hashpartitioning(configurationId#4, 200), ENSURE_REQUIREMENTS, [plan_id=282]
          +- HashAggregate(keys=[configurationId#4], functions=[partial_avg(salePrice#22)], output=[configurationId#4, sum#67, count#68L])
             +- Project [configurationId#4, salePrice#22]
                +- SortMergeJoin [make#5, model#6], [make#19, model#20], Inner, (abs((soundScore#21 - soundScore#7)) <= 0.1)
                   :- Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST], false, 0
                   :  +- Exchange hashpartitioning(make#5, model#6, 200), ENSURE_REQUIREMENTS, [plan_id=274]
                   :     +- LocalTableScan [configurationId#4, make#5, model#6, soundScore#7]
                   +- Sort [make#19 ASC NULLS FIRST, model#20 ASC NULLS FIRST], false, 0
                      +- Exchange hashpartitioning(make#19, model#20, 200), ENSURE_REQUIREMENTS, [plan_id=275]
                         +- LocalTableScan [make#19, model#20, soundScore#21, salePrice#22]

     */
  }

  def noSkewSolution() = {
    // salting interval 0-99
    val explodedGuitars =  guitars.withColumn("salt", explode(lit((0 to 99).toArray))) // multiplying the guitars DS x100
    val saltedGuitarSales = guitarSales.withColumn("salt", monotonically_increasing_id() % 100)
    explodedGuitars.take(1).foreach(println)
    saltedGuitarSales.take(1).foreach(println)
    val nonSkewedJoin = explodedGuitars.join(saltedGuitarSales, Seq("make", "model", "salt"))
      .where(abs(saltedGuitarSales("soundScore") - explodedGuitars("soundScore")) <= 0.1)
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))

    nonSkewedJoin.explain(true)
    nonSkewedJoin.count()
    /*
    == Parsed Logical Plan ==
    'Aggregate [configurationId#4], [configurationId#4, avg('salePrice) AS averagePrice#84]
    +- Filter (abs((soundScore#21 - soundScore#7)) <= 0.1)
       +- Project [make#5, model#6, salt#41, configurationId#4, soundScore#7, registration#18, soundScore#21, salePrice#22]
          +- Join Inner, (((make#5 = make#19) AND (model#6 = model#20)) AND (cast(salt#41 as bigint) = salt#47L))
             :- Project [configurationId#4, make#5, model#6, soundScore#7, salt#41]
             :  +- Generate explode([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99]), false, [salt#41]
             :     +- LocalRelation [configurationId#4, make#5, model#6, soundScore#7]
             +- Project [registration#18, make#19, model#20, soundScore#21, salePrice#22, (monotonically_increasing_id() % cast(100 as bigint)) AS salt#47L]
                +- LocalRelation [registration#18, make#19, model#20, soundScore#21, salePrice#22]

    == Analyzed Logical Plan ==
    configurationId: string, averagePrice: double
    Aggregate [configurationId#4], [configurationId#4, avg(salePrice#22) AS averagePrice#84]
    +- Filter (abs((soundScore#21 - soundScore#7)) <= 0.1)
       +- Project [make#5, model#6, salt#41, configurationId#4, soundScore#7, registration#18, soundScore#21, salePrice#22]
          +- Join Inner, (((make#5 = make#19) AND (model#6 = model#20)) AND (cast(salt#41 as bigint) = salt#47L))
             :- Project [configurationId#4, make#5, model#6, soundScore#7, salt#41]
             :  +- Generate explode([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99]), false, [salt#41]
             :     +- LocalRelation [configurationId#4, make#5, model#6, soundScore#7]
             +- Project [registration#18, make#19, model#20, soundScore#21, salePrice#22, (monotonically_increasing_id() % cast(100 as bigint)) AS salt#47L]
                +- LocalRelation [registration#18, make#19, model#20, soundScore#21, salePrice#22]

    == Optimized Logical Plan ==
    Aggregate [configurationId#4], [configurationId#4, avg(salePrice#22) AS averagePrice#84]
    +- Project [configurationId#4, salePrice#22]
       +- Join Inner, ((((abs((soundScore#21 - soundScore#7)) <= 0.1) AND (make#5 = make#19)) AND (model#6 = model#20)) AND (cast(salt#41 as bigint) = salt#47L))
          :- Filter isnotnull(salt#41)
          :  +- Generate explode([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99]), false, [salt#41]
          :     +- LocalRelation [configurationId#4, make#5, model#6, soundScore#7]
          +- LocalRelation [make#19, model#20, soundScore#21, salePrice#22, salt#47L]

    == Physical Plan ==
    AdaptiveSparkPlan isFinalPlan=false
    +- HashAggregate(keys=[configurationId#4], functions=[avg(salePrice#22)], output=[configurationId#4, averagePrice#84])
       +- Exchange hashpartitioning(configurationId#4, 200), ENSURE_REQUIREMENTS, [plan_id=61]
          +- HashAggregate(keys=[configurationId#4], functions=[partial_avg(salePrice#22)], output=[configurationId#4, sum#89, count#90L])
             +- Project [configurationId#4, salePrice#22]
                +- SortMergeJoin [make#5, model#6, cast(salt#41 as bigint)], [make#19, model#20, salt#47L], Inner, (abs((soundScore#21 - soundScore#7)) <= 0.1)
                   :- Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST, cast(salt#41 as bigint) ASC NULLS FIRST], false, 0
                   :  +- Exchange hashpartitioning(make#5, model#6, cast(salt#41 as bigint), 200), ENSURE_REQUIREMENTS, [plan_id=53]
                   :     +- Filter isnotnull(salt#41)
                   :        +- Generate explode([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99]), [configurationId#4, make#5, model#6, soundScore#7], false, [salt#41]
                   :           +- LocalTableScan [configurationId#4, make#5, model#6, soundScore#7]
                   +- Sort [make#19 ASC NULLS FIRST, model#20 ASC NULLS FIRST, salt#47L ASC NULLS FIRST], false, 0
                      +- Exchange hashpartitioning(make#19, model#20, salt#47L, 200), ENSURE_REQUIREMENTS, [plan_id=54]
                         +- LocalTableScan [make#19, model#20, soundScore#21, salePrice#22, salt#47L]


     */

  }

  def main(args:Array[String]):Unit ={
    //naiveSolution()
    //Thread.sleep(10000)
    noSkewSolution()
    Thread.sleep(10000)
  }

}
