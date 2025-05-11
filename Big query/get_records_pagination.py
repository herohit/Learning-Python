import math
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPICallError, RetryError


def get_total_pages(page_size):
    """
       Calculates the total number of pages available for paginated results
       based on the number of rows in the employee table.

       Args:
           page_size (int): Number of records per page.

       Returns:
           int: Total number of pages.
       """

    # Initialize bigquery client
    client = bigquery.Client()

    # Query to get total rows
    count_query = "select count(*) as total from `bigquery-458315.practice.employee`"

    # Execute the query
    count_job = client.query(count_query)

    # Get result from query
    result = count_job.result()

    # Get total_row
    total_row = [row.total for row in result][0]

    # Calculate total pages
    total_pages = math.ceil(total_row / page_size)
    return total_pages


def get_records_with_pagination(page_size,page_number):
    """
    Fetches paginated employee records from BigQuery with joined address,
    department, and designation tables.

    Args:
        page_size (int): Number of records per page.
        page_number (int): Page number to fetch.

    Returns:
        google.cloud.bigquery.table.RowIterator: Result set from BigQuery.
    """

    # Initialize bigquery client
    client = bigquery.Client()

    # Define Table
    project_id = "bigquery-458315"
    dataset_id = "practice"

    # Full table references
    table_employee = f'{project_id}.{dataset_id}.employee'
    table_address = f'{project_id}.{dataset_id}.address'
    table_department = f'{project_id}.{dataset_id}.department'
    table_designation = f'{project_id}.{dataset_id}.designation'

    try:
        # Calculate offset
        offset = (page_number-1) * page_size

        # Make query to get records
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
            ORDER BY e.id ASC
            LIMIT {page_size}
            OFFSET {offset}
        """

        # Execute the query
        query_job = client.query(query)

        # Get result from query
        result = query_job.result()

        return result

    # Error related to GoogleAPI
    except (GoogleAPICallError, RetryError) as e:
        print(f"BigQuery API Error: {e}")

    # If any unexpected error occured
    except Exception as e:
        print(f"Unexpected Error: {e}")

def get_records_pagination(page_size,page_number):
    """
        Displays the paginated records from BigQuery in a formatted table.

        Args:
            page_size (int): Number of records per page.
            page_number (int): Page number to display.
        """
    # Get records
    records  = get_records_with_pagination(page_size , page_number)

    # If records exist
    if records:
        print("EmployeeId | FullName  | Salary | DepartmentName | DesignationName | FullAddress | City | State | CreatedAt | UpdatedAt")
        print("-" * 120)
        # Display each row of the result
        for row in records:
            print(f"{row.id} | {row.full_name} | {row.salary} | {row.departmentName} | {row.designationName} | "
                  f"{row.full_address} | {row.city} | {row.state} | {row.created_at} | {row.updated_at}")
    else:
        # If not records found
        print("No records found on this page.")