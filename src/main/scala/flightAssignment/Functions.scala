package flightAssignment


import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Functions {

  // Load Data Function
  def loadData(spark: SparkSession, filePath: String): DataFrame = {
    spark.read.option("header", "true").csv(filePath)
  }

  // Write Data to Output
  def writeOutput(df: DataFrame, outputPath: String): Unit = {
    df.repartition(1).write.mode("overwrite").option("header", "true").csv(outputPath)
  }

  // Aggregation by Month
  def getAggByMonth(flightDf: DataFrame): DataFrame = {
    flightDf
      .withColumn("month", month(col("date")))
      .groupBy("month")
      .count()
      .orderBy("month")
      .withColumnRenamed("month", "Month")
      .withColumnRenamed("count", "Number of Flights")
  }

  // Top Frequent Flyers
  def getTopFrequentFlyers(flightDf: DataFrame, passengerDf: DataFrame, numTop: Int): DataFrame = {
    val topFlyers = flightDf
      .groupBy("passengerId")
      .agg(count("flightId").alias("number_of_flights"))
      .orderBy(desc("number_of_flights"))
      .limit(numTop)

    topFlyers
      .join(passengerDf, Seq("passengerId"))
      .select("passengerId", "number_of_flights", "firstName", "lastName")
      .orderBy(desc("number_of_flights"))
      .withColumnRenamed("passengerId", "Passenger ID")
      .withColumnRenamed("number_of_flights", "Number of Flights")
      .withColumnRenamed("firstName", "First name")
      .withColumnRenamed("lastName", "Last name")
  }

  // Longest Route Outside UK
  def getLongestRoute(flightDf: DataFrame): DataFrame = {
    def findLongestWithoutUK(journey: String): Int = {
      val segments = journey.toLowerCase.split("uk")
      val segmentLengths = segments.map(_.split(",").count(_.trim.nonEmpty))
      segmentLengths.max
    }

    val findLongestWithoutUK_udf = udf(findLongestWithoutUK _)

    flightDf.groupBy("passengerId")
      .agg(collect_list("from").as("from_list"), collect_list("to").as("to_list"))
      .withColumn("full_journey", concat_ws(",", col("from_list"),element_at(col("to_list"), -1)))  // Append last 'to'
      .withColumn("Longest Run", findLongestWithoutUK_udf(col("full_journey")))
      .select("passengerId", "Longest Run")
      .orderBy(desc("Longest Run"))
      .withColumnRenamed("passengerId", "Passenger ID")
  }

  // Passengers on Same Flights More Than 3 Times
  def getPassengerSharedFlights(flightDf: DataFrame, numShared: Int): DataFrame = {
    // Self-join the DataFrame on the flightId column, filtering out self-joins (same passenger) by ensuring t1.passengerId < t2.passengerId
    val joinedFlights = flightDf.as("t1")
      .join(flightDf.as("t2"), col("t1.flightId") === col("t2.flightId") && col("t1.passengerId") < col("t2.passengerId"))

    // Group by both passenger pairs and count the number of shared flights
    val sharedFlights = joinedFlights
      .groupBy(col("t1.passengerId").as("Passenger 1 ID"), col("t2.passengerId").as("Passenger 2 ID"))
      .agg(count("*").as("Number of flights together"))

    // Filter based on the number of shared flights (at least numShared)
    val filteredSharedFlights = sharedFlights
      .filter(col("Number of flights together") >= numShared)

    filteredSharedFlights
  }
}
