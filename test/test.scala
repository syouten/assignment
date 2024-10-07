// Databricks notebook source
// MAGIC %run ../src/functions

// COMMAND ----------

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction



// Define mock flight data
val flightData = Seq(
  ("1", "101", "A", "B", "2024-01-01"),
  ("1", "102", "B", "C", "2024-01-05"),
  ("1", "103", "C", "UK", "2024-01-10"),
  ("2", "101", "A", "B", "2024-01-01"),
  ("2", "105", "B", "F", "2024-02-05"),
  ("3", "101", "A", "B", "2024-01-01"),
  ("3", "102", "B", "C", "2024-01-05"),
  ("3", "103", "C", "UK", "2024-01-10"),
  ("3", "104", "UK", "J", "2024-03-1")
).toDF("passengerId", "flightId", "from", "to", "date")

// Define mock passenger data
val passengerData = Seq(
  ("1", "John", "Doe"),
  ("2", "Jane", "Smith"),
  ("3", "Bob", "Johnson")
).toDF("passengerId", "firstName", "lastName")


// Test Functions
def testGetAggByMonth(): Unit = {
  println("Testing getAggByMonth:")

  // Create a DataFrame to hold the expected results
  val expectedAggByMonth = Seq(
    (1, 7), // January: 2 flights
    (2, 1), // February: 2 flights
    (3, 1)  // March: 3 flights
  ).toDF("Month", "Number of Flights")

  // Call the function to test
  val actualAggByMonth = getAggByMonth(flightData)

  // Assert the result
  assert(expectedAggByMonth.collect().toSet == actualAggByMonth.collect().toSet, "Agg By Month test failed")
  
  println("Agg By Month test passed.")
}

def testGetTopFrequentFlyers(): Unit = {
  println("Testing getTopFrequentFlyers:")

  // Create a DataFrame to hold the expected results
  val expectedTopFlyers = Seq(
    ("3", 4, "Bob", "Johnson"), // Passenger 1 with 2 flights
    ("1", 3, "John", "Doe") // Passenger 2 with 2 flights
  ).toDF("Passenger ID", "Number of Flights", "First name", "Last name")

  // Call the function to test
  val actualTopFlyers = getTopFrequentFlyers(flightData, passengerData, 2)

  // Assert the result
  assert(expectedTopFlyers.collect().toSet == actualTopFlyers.collect().toSet, "Top Frequent Flyers test failed")
  println("Top Frequent Flyers test passed.")
}

def testGetLongestRoute(): Unit = {
  println("Testing getLongestRoute:")

  // Create a DataFrame to hold the expected results
  val expectedLongestRoute = Seq(
    ("1", 3), // Passenger 1 longest run
    ("2", 3), // Passenger 2 longest run
    ("3", 3)  // Passenger 3 longest run
  ).toDF("Passenger ID", "Longest Run")

  // Call the function to test
  val actualLongestRoute = getLongestRoute(flightData)

  // Assert the result
  assert(expectedLongestRoute.collect().toSet == actualLongestRoute.collect().toSet, "Longest Route test failed")
  println("Longest Route test passed.")
}


def testGetPassengerSharedFlights(): Unit = {
  println("Testing getPassengerSharedFlights:")

  // Create a DataFrame to hold the expected results
  val expectedSharedFlights = Seq(
    ("1", "3", 3), // Passenger 1 and 3 share 3 flights
  ).toDF("Passenger 1 ID", "Passenger 2 ID", "Number of flights together")

  // Call the function to test
  val actualSharedFlights = getPassengerSharedFlights(flightData, 3)

  // Assert the result
  assert(expectedSharedFlights.collect().toSet == actualSharedFlights.collect().toSet, "Passenger Shared Flights test failed")
  println("Passenger Shared Flights test passed.")
}



// COMMAND ----------

// Run Tests
testGetAggByMonth()
testGetTopFrequentFlyers()
testGetLongestRoute()
testGetPassengerSharedFlights()
