from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 test_cases=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases

    def execute(self, context):
        """
        Run data quality checks based on provided test cases
        Each test case should be a dictionary with:
        - 'sql': SQL query to run
        - 'expected_result': The expected result of the query
        - 'operator': The comparison operator (optional, defaults to '==')
        """
        self.log.info('DataQualityOperator is starting')

        if not self.test_cases:
            self.log.warning("No test cases provided")
            return

        # Create Redshift connection
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Execute each test case
        failing_tests = []
        for i, test in enumerate(self.test_cases, 1):
            sql = test.get('sql')
            expected_result = test.get('expected_result')
            operator = test.get('operator', '==')

            if not sql:
                self.log.warning(f"Test case #{i} is missing SQL query")
                continue

            self.log.info(f"Running test #{i}: {sql}")
            records = redshift.get_records(sql)

            # Check if we got any results
            if len(records) < 1 or len(records[0]) < 1:
                failing_tests.append(f"Test #{i} failed: No results returned")
                continue

            # Get the actual result from the first record
            actual_result = records[0][0]

            # Compare with expected result based on operator
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
                self.log.warning(f"Unknown operator '{operator}' in test #{i}")
                test_passed = actual_result == expected_result

            if not test_passed:
                failing_tests.append(
                    f"Test #{i} failed: Expected {expected_result} {operator} {actual_result}"
                )
            else:
                self.log.info(f"Test #{i} passed")

        # If any tests failed, raise an exception
        if failing_tests:
            self.log.error("Data quality checks failed!")
            self.log.error("\n".join(failing_tests))
            raise ValueError("Data quality checks failed. See logs for details.")

        self.log.info("All data quality checks passed!")