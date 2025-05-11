from google.cloud import bigquery
from datetime import datetime
from google.api_core.exceptions import GoogleAPICallError, RetryError


def insert_or_update_address(address_data):
    """
    Inserts or updates an employee's address in the BigQuery `address` table and updates the `updated_at` timestamp in the `employee` table.

    This function performs the following steps:
    1. Checks if an address already exists for the given employee ID in the `address` table.
    2. If the address exists:
        - Updates the existing address with the new details provided in `address_data`.
    3. If the address does not exist:
        - Inserts a new address into the `address` table.
    4. Updates the `updated_at` field in the `employee` table to the current timestamp.

    Args:
        address_data (dict): A dictionary containing the following keys:
            - emp_id (int): The employee ID.
            - addressline1 (str): The first line of the address.
            - addressline2 (str): The second line of the address.
            - city (str): The city of the address.
            - state (str): The state of the address.

    Returns:
        None

    Raises:
        GoogleAPICallError: If there is an error during the BigQuery API call.
        RetryError: If a retryable error occurs and retry attempts fail.
        Exception: For any other unexpected errors.
    """
    try:
        # Initialize bigquery client
        client = bigquery.Client()

        # Define Table
        project_id = "bigquery-458315"
        dataset_id = "practice"
        table_id = "address"
        employeetable_id = "employee"
        table_ref = f'{project_id}.{dataset_id}.{table_id}'
        table_employee = f'{project_id}.{dataset_id}.{employeetable_id}'

        # Check if address already exist
        check_query = f"""
                SELECT COUNT(*) as count FROM {table_ref} where employee_id = @emp_id
                """
        # Make query Parameters
        query_parameters =[
            bigquery.ScalarQueryParameter("emp_id","INT64",address_data["emp_id"])
            ]

        # Query Job Config
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        # Run the query
        check_job = client.query(check_query,job_config=job_config)

        # query result
        query_result = check_job.result()
        # Check if address exists
        address_exists = [row.count for row in query_result][0] > 0

        if address_exists:
            # update existing address
            update_query =f"""
            UPDATE {table_ref} SET
                    addressline1 = {address_data["addressline1"]},
                    addressline2 ={address_data["addressline2"]},
                    city = {address_data["city"]},
                    state = {address_data["state"]}
                    WHERE emp_id = {address_data["emp_id"]}
            """
        else:
            # Insert new address
            rows_to_insert =[
                {
                    "id" : int(datetime.now().strftime("%Y%m%d%H%M%S")),
                    "employee_id" : address_data["emp_id"],
                    "addressline1" : address_data["addressline1"],
                    "addressline2" : address_data["addressline2"],
                    "city" : address_data["city"],
                    "state" :  address_data["state"]
                }
                ]
            # Run the query
            errors = client.insert_rows_json(table_ref,rows_to_insert)

            # Check if there is any error returned by above query
            if not errors:
                print("New address inserted.")
            else:
                print("Error inserting address",errors)
                return

        # Update updated_at in employee table
        update_employee_query = f"""
            UPDATE {table_employee} SET updated_at = CURRENT_TIMESTAMP() WHERE id = {address_data["emp_id"]}
            """

        # Run the query
        query_result = client.query(update_employee_query)

        # Get query result
        result = query_result.result()

        # Get number of rows that were updated
        affected_rows = result.num_dml_affected_rows

        # Check if update happened
        if affected_rows:
            print(f'Employee {address_data["emp_id"]} updated_at timestamp updated.')
        else:
            print("Employee not updated...")

    # If there is error related to big query
    except (GoogleAPICallError, RetryError) as e:
        print(f"BigQuery API Error: {e}")

    # If any unexpected error occurred
    except Exception as e:
        print(f"Unexpected Error: {e}")