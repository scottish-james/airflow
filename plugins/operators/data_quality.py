from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Custom Airflow Operator to run data quality checks on Redshift.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 test_cases=[],
                 *args, **kwargs):
        """
        Initialize the DataQualityOperator.

        :param redshift_conn_id: Airflow connection ID for Redshift
        :param test_cases: List of test cases (dictionaries containing SQL queries and expected results)
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)

        # Store parameters as instance variables
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        """
        Execute the task: Run data quality checks based on provided test cases.

        Each test case should be a dictionary with:
        - 'sql': SQL query to run
        - 'expected_result': The expected result of the query
        - 'operator': The comparison operator (default: '==')
        """
        self.log.info('DataQualityOperator is starting')

        # If no test cases are provided, log a warning and return
        if not self.test_cases:
            self.log.warning("No test cases provided")
            return

        # Establish a connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # List to store any failing test cases
        failing_tests = []

        # Iterate over each test case
        for i, test in enumerate(self.test_cases, 1):
            sql = test.get('sql')  # SQL query to execute
            expected_result = test.get('expected_result')  # Expected result
            operator = test.get('operator', '==')  # Comparison operator

            # If SQL query is missing, log a warning and continue to next test
            if not sql:
                self.log.warning(f"Test case #{i} is missing SQL query")
                continue

            # Execute the SQL query
            self.log.info(f"Running test #{i}: {sql}")
            records = redshift.get_records(sql)

            # Check if the query returned results
            if len(records) < 1 or len(records[0]) < 1:
                failing_tests.append(f"Test #{i} failed: No results returned")
                continue

            # Extract the actual result from the query output
            actual_result = records[0][0]

            # Perform comparison based on the specified operator
            test_passed = False
            if operator == '==':
                test_passed = actual_result == expected_result
            elif operator == '>':
                test_passed = actual_result > expected_result
            elif operator == '<':
                test_passed = actual_result < expected_result
            elif operator == '>=':
                test_passed = actual_result >= expected_result
            elif operator == '<=':
                test_passed = actual_result <= expected_result
            elif operator == '!=':
                test_passed = actual_result != expected_result
            else:
                # Log an unknown operator but default to equality check
                self.log.warning(f"Unknown operator '{operator}' in test #{i}")
                test_passed = actual_result == expected_result

            # If the test failed, store the failure message
            if not test_passed:
                failing_tests.append(
                    f"Test #{i} failed: Expected {expected_result} {operator} {actual_result}"
                )
            else:
                self.log.info(f"Test #{i} passed")

        # If any tests failed, log errors and raise an exception
        if failing_tests:
            self.log.error("Data quality checks failed!")
            self.log.error("\n".join(failing_tests))
            raise ValueError("Data quality checks failed. See logs for details.")

        # Log success message if all tests pass
        self.log.info("All data quality checks passed!")