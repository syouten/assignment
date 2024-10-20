package flightAssignment

import org.apache.spark.sql.SparkSession


object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Flight Data Processor")
      .master("local[*]")
      .getOrCreate()

    val rootPath = "./src"

    // Define file paths
    val flightPath = f"$rootPath/data/flightData.csv"
    val passengerPath = f"$rootPath/data/passengers.csv"

    // Load flight and passenger data
    val flightDf = Functions.loadData(spark, flightPath).cache()
    val passengerDf = Functions.loadData(spark, passengerPath).cache()

    // Processing each step
    // Q1: get agg flights by month
    val aggByMonthDf = Functions.getAggByMonth(flightDf)
    Functions.writeOutput(aggByMonthDf, f"$rootPath/output/output_aggByMonth")

    // Q2: get top flyers
    val topFlyersDf = Functions.getTopFrequentFlyers(flightDf, passengerDf, 100)
    Functions.writeOutput(topFlyersDf, f"$rootPath/output/output_topFlyers")

    // Q3: get longest run
    val longestRouteDf = Functions.getLongestRoute(flightDf)
    Functions.writeOutput(longestRouteDf, f"$rootPath/output/output_longestRoute")

    // Q4: passengers shared flights
    val passengerSharedFlightDf = Functions.getPassengerSharedFlights(flightDf, 3)
    Functions.writeOutput(passengerSharedFlightDf, f"$rootPath/output/output_passengerSharedFlight")

    spark.stop()
  }
}