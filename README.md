## Getting Started

### Prerequisites

- Apache Spark: 2.4
- Scala: 2.12.10
- Databricks Notebook or a compatible environment

### Running the Notebooks

1. **Run the Main scala code** (`src/main/flightAssignment/main.scala`):
   - Open the `Main.scala` notebook.
   - Execute the cells to process the flight data.
   - The output will be generated and saved in the `output/` folder.

2. **Run the Test scala code** (`src/test/flightAssignment/FlightDataFunctionsTest.scala`):
   - Run the unit test scala file.

### Testing Functions

The following functions are tested in the `test.scala` notebook:

1. **getAggByMonth**
   - Aggregates the number of flights by month.

2. **getTopFrequentFlyers**
   - Identifies the top frequent flyers based on the number of flights taken.

3. **getLongestRoute**
   - Determines the longest route for each passenger.

4. **getPassengerSharedFlights**
   - Counts the number of flights shared between passengers.

### Running Tests

- Each test function generates mock data and compares the output of the main functions against expected values using assertions. 
- You will see output messages indicating whether each test passed or failed.

### Contribution

If you want to contribute to this project, feel free to create a pull request. Make sure to run the tests to validate your changes.

### License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.