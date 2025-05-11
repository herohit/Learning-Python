from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, RetryError

def get_all_records():
    """
    Fetches all employee records from the BigQuery `employee` table and joins related data
    from the `department`, `designation`, and `address` tables.

    The function retrieves the following details for each employee:
        - Employee ID
        - Full Name (concatenation of first and last name)
        - Salary
        - Department Name
        - Designation Name
        - Full Address (concatenation of address lines)
        - City and State
        - Created and Updated Timestamps

    The results are printed in a tabular format.

    Raises:
        GoogleAPICallError: If there is an error during the BigQuery API call.
        RetryError: If a retryable error occurs and retry attempts fail.
        Exception: For any other unexpected errors.
    """
    try:
        # Initialize the Big query client
        client = bigquery.Client()

        # Define Table
        project_id = "bigquery-458315"
        dataset_id = "practice"

        table_employee = f'{project_id}.{dataset_id}.employee'
        table_address = f'{project_id}.{dataset_id}.address'
        table_department = f'{project_id}.{dataset_id}.department'
        table_designation = f'{project_id}.{dataset_id}.designation'

        # Query to get all records
        query = f"""
            SELECT
            e.id,
            CONCAT(e.fname, ' ', e.lname) AS full_name,
            e.salary,
            e.department_id,
            d.departmentName,
            e.designation_id,
            des.designationName,
            CONCAT(a.addressline1,' , ',a.addressline2) as full_address,
            a.city ,
            a.state ,
            e.created_at,
            e.updated_at

            FROM `{table_employee}` e
            LEFT JOIN `{table_department}` d
            ON e.department_id = d.department_id
            LEFT JOIN `{table_designation}` des
            ON e.designation_id = des.designation_id
            LEFT JOIN `{table_address}` a
            ON e.id = a.employee_id
        """

        # Run the query
        query_job = client.query(query)

        # Get the result
        result = query_job.result()

        # # store result in list
        # rows = list(result)

        # Print the results
        print("OUTPUT:")
        print("EmployeeId | FullName  | Salary | DepartmentName | DesignationName | FullAddress | City | State | CreatedAt | UpdatedAt")
        print("-" * 120)
        # Loop over the rows list
        for row in result:
            print(f"{row.id} | {row.full_name} | {row.salary} | {row.departmentName} | {row.designationName} | "
                    f"{row.full_address} | {row.city} | {row.state} | {row.created_at} | {row.updated_at}")

    # If there is errror related to big query
    except (GoogleAPICallError, RetryError) as e:
        print(f"BigQuery API Error: {e}")
    # If any unexpected error occured
    except Exception as e:
        print(f"Unexpected Error: {e}")