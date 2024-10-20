package flightAssignment
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import flightAssignment.Functions._

class FlightDataFunctionsTest extends AnyFunSuite {

  // Create a Spark session for testing
  val spark = SparkSession.builder()
    .appName("Unit Testing Flight Data Functions")
    .master("local[*]")  // For local testing
    .getOrCreate()

  import spark.implicits._

  // Define mock flight data
  val flightData: DataFrame = Seq(
    ("1", "101", "A", "B", "2024-01-01"),
    ("1", "102", "B", "C", "2024-01-05"),
    ("1", "103", "C", "UK", "2024-01-10"),
    ("2", "101", "A", "B", "2024-01-01"),
    ("2", "105", "B", "F", "2024-02-05"),
    ("3", "101", "A", "B", "2024-01-01"),
    ("3", "102", "B", "C", "2024-01-05"),
    ("3", "103", "C", "UK", "2024-01-10"),
    ("3", "104", "UK", "J", "2024-03-01")
  ).toDF("passengerId", "flightId", "from", "to", "date")

  // Define mock passenger data
  val passengerData: DataFrame = Seq(
    ("1", "John", "Doe"),
    ("2", "Jane", "Smith"),
    ("3", "Bob", "Johnson")
  ).toDF("passengerId", "firstName", "lastName")

  // Test for Aggregation by Month
  test("getAggByMonth should correctly aggregate the flights by month") {
    val expectedAggByMonth = Seq(
      (1, 7), // January: 7 flights
      (2, 1), // February: 1 flight
      (3, 1)  // March: 1 flight
    ).toDF("Month", "Number of Flights")

    val actualAggByMonth = getAggByMonth(flightData)

    assert(expectedAggByMonth.collect().toSet == actualAggByMonth.collect().toSet)
  }

  // Test for Top Frequent Flyers
  test("getTopFrequentFlyers should correctly find the top 2 frequent flyers") {
    val expectedTopFlyers = Seq(
      ("3", 4, "Bob", "Johnson"), // 4 flights for Passenger 3
      ("1", 3, "John", "Doe") // 3 flights for Passenger 1
    ).toDF("Passenger ID", "Number of Flights", "First name", "Last name")

    val actualTopFlyers = getTopFrequentFlyers(flightData, passengerData, 2)

    assert(expectedTopFlyers.collect().toSet == actualTopFlyers.collect().toSet)
  }

  // Test for Longest Route
  test("getLongestRoute should correctly calculate the longest journey without UK") {
    val expectedLongestRoute = Seq(
      ("1", 3), // Longest run for Passenger 1
      ("2", 3), // Longest run for Passenger 2
      ("3", 3)  // Longest run for Passenger 3
    ).toDF("Passenger ID", "Longest Run")

    val actualLongestRoute = getLongestRoute(flightData)

    assert(expectedLongestRoute.collect().toSet == actualLongestRoute.collect().toSet)
  }

  // Test for Passengers Sharing Flights
  test("getPassengerSharedFlights should correctly find passengers who shared flights more than 3 times") {
    val expectedSharedFlights = Seq(
      ("1", "3", 3) // Passenger 1 and 3 shared 3 flights
    ).toDF("Passenger 1 ID", "Passenger 2 ID", "Number of flights together")

    val actualSharedFlights = getPassengerSharedFlights(flightData, 3)

    assert(expectedSharedFlights.collect().toSet == actualSharedFlights.collect().toSet)
  }

}
