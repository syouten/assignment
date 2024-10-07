// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.IntegerType

// COMMAND ----------

def loadData(filePaths: List[String]):List[DataFrame] = {

  val dataframes = filePaths.map{filePath => spark.read.format("csv").option("header", "true").load(filePath)}

  dataframes
}

// COMMAND ----------

def write_output(input_df: DataFrame, output_path:String):Unit = {

  input_df.repartition(1).write.format("csv").mode("overwrite").option("header","true").save(output_path)
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step one: get aggregation by month

// COMMAND ----------

def getAggByMonth(flight_df: DataFrame, output_path: String = null):DataFrame = {

  val flightsByMonth = flight_df
  .withColumn("month", month(col("date")))
  .groupBy("month")
  .count()
  .orderBy("month")
  .withColumnRenamed("month","Month")
  .withColumnRenamed("count","Number of Flights")
  
  if (output_path != null){
    write_output(flightsByMonth,output_path)
  }

  flightsByMonth
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step two: find top 100 frequent flyers

// COMMAND ----------

def getTopFrequentFlyers(flight_df: DataFrame, passenger_df: DataFrame, numTop: Int, output_path:String = null):DataFrame = {

  val topNFrequentFlyers = flight_df
    .groupBy("passengerId")
    .agg(
      count("flightId").alias("number_of_flights")
    )
    .orderBy(desc("number_of_flights"))
    .limit(numTop)

  // Use aliases for better clarity and to avoid ambiguous column references
  val joinedData = topNFrequentFlyers.alias("topN")
    .join(passenger_df.alias("passenger"), col("topN.passengerId") === col("passenger.passengerId"))
    .select(col("topN.passengerId"), col("topN.number_of_flights"),col("passenger.firstName"), col("passenger.lastName"))
    .orderBy(desc("number_of_flights"))
    .withColumnRenamed("passengerId","Passenger ID")
    .withColumnRenamed("number_of_flights","Number of Flights")
    .withColumnRenamed("firstName","First name")
    .withColumnRenamed("lastName","Last name")

  if (output_path != null){
    write_output(joinedData,output_path)
  }

  joinedData
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step three: get longest route outside UK

// COMMAND ----------

def findLongestWithoutUK(journey: String): Int = {
    val segments = journey.toLowerCase.split("uk")
    val segmentLengths = segments.map(s => s.split(",").count(_.trim.nonEmpty))
    segmentLengths.max
  }

val findLongestWithoutUK_udf = udf(findLongestWithoutUK _)


// COMMAND ----------

def getLongestRoute(flight_df: DataFrame, output_path: String = null):DataFrame = {

  val journeyAgg = flight_df.groupBy("passengerId")
  .agg(
    collect_list("from").as("from_list"),  // Collect all 'from' locations
    collect_list("to").as("to_list")       // Collect all 'to' locations
  )
  .withColumn("full_journey", concat_ws(",", $"from_list", array($"to_list"(size($"to_list") - 1))))  // Append last 'to'
  .select("passengerId", "full_journey")

val count_df = journeyAgg.withColumn("Longest Run", findLongestWithoutUK_udf(col("full_journey")))
                          .orderBy(col("Longest Run").desc)
                          .withColumnRenamed("passengerId","Passenger ID")
                          .select("Passenger ID","Longest Run")

if (output_path != null){
    write_output(count_df,output_path)
}

count_df
}

// COMMAND ----------

// MAGIC %md
// MAGIC ### Step four: passengers who have been on same flights more than 3 times

// COMMAND ----------

def getPassengerSharedFlights(flight_df: DataFrame,numShared: Int, output_path:String=null): DataFrame = {
    flight_df.createOrReplaceTempView("flight_view")

    var sql_stmt = f"""
    SELECT 
      t1.passengerId AS `Passenger 1 ID`,
      t2.passengerId AS `Passenger 2 ID`,
      COUNT(*) AS `Number of flights together` 
    FROM flight_view t1 JOIN flight_view t2 
    ON t1.flightId = t2.flightId 
    AND t1.passengerId < t2.passengerId  
    GROUP BY t1.passengerId, t2.passengerId 
    HAVING COUNT(*) >= $numShared
    """
    var resultDf = spark.sql(sql_stmt)

    if (output_path != null){
    write_output(resultDf,output_path)
    }

    resultDf
  }
