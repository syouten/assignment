// Databricks notebook source
// MAGIC %run ./functions

// COMMAND ----------

var start_time = System.nanoTime()

val root_path = "file:/Workspace/Users/223142_adm@hk.fwd.com/FIB_project"

val filePaths = List(s"$root_path/flightData.csv",s"$root_path/passengers.csv")
// load data
val dataframes = loadData(filePaths)

var flight_df = dataframes(0).repartition(10,col("date")).cache()

// get aggregation by month
val aggByMonthDf = getAggByMonth(flight_df,s"$root_path/output_aggByMonth")

// find top 100 frequnty flyers
val topFlyersDf = getTopFrequentFlyers( flight_df,dataframes(1),100,s"$root_path/output_topFlyers")

// get longest route outside UK
val longestRouteDf = getLongestRoute(flight_df,s"$root_path/output_longestRoute")

// find passengers who have been on same flights for more than 3 times
val passengerSharedFlightDf = getPassengerSharedFlights(flight_df,3,s"$root_path/output_passengerSharedFlight")

var end_time = System.nanoTime()
var eclipsed_time = (end_time - start_time)/1000000000
print(s"processing time $eclipsed_time")

// COMMAND ----------


