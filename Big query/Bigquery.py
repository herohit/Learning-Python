"""
Main entry point for interacting with Google BigQuery to perform:
1. Fetching all address records
2. Inserting or updating an address
3. Viewing paginated address records

This script assumes service account authentication via a `credentials.json` file
and uses modular functions from other files:
- insert_update.py
- get_records.py
- get_records_pagination.py
"""
import os
from insert_update import insert_or_update_address
from get_records import get_all_records
from get_records_pagination import get_records_pagination , get_total_pages

if __name__ == '__main__':
    # Set the environment variable to authenticate with Google Cloud
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'credentials.json'

    while True:
        # Main menu options
        user_choice = input("Please Choose an option. \n 1. Get all records \n 2. Insert/Update Address \n 3. Get paginated records \n 4. Exit \n Your Choice: ")

        if user_choice == '1':
            # Retrieve and display all records
            get_all_records()

        elif user_choice == '2':
            # Insert a new address or update an existing one
            insert_or_update_address()

        elif user_choice == '3':
            # Get user-defined page size, default to 5 if invalid
            page_size_input = input("Enter page size (default 5): ")
            page_size = int(page_size_input) if page_size_input.isdigit() else 5

            # Calculate total number of pages
            total_pages = get_total_pages(page_size=page_size)

            # Get starting page from user input (default to 1)
            input_page = input("Enter starting page number (default 1): ")
            current_page = int(input_page) if input_page.isdigit() and 1<= int(input_page) <= total_pages else 1

            while True:
                print(f"\n--- Showing Page {current_page} of {total_pages} ---")
                # Display paginated records for the current page
                get_records_pagination(page_size=page_size, page_number=current_page)

                # Navigation prompt
                nav = input("\nEnter 'next', 'prev', page number to jump, or 'exit' to return to main menu: ").strip().lower()

                # Navigate to next page
                if nav == 'next':
                    # Next Page
                    if current_page < total_pages:
                        current_page += 1
                    else:
                        print("You're already on the last page.")

                # Navigate to previous page
                elif nav == 'prev':
                    
                    if current_page > 0:
                        current_page -= 1
                    else:
                        print("You're already on the first page.")

                # Exit pagination loop and return to main menu
                elif nav == 'exit':
                    break

                # Jump to specific page if valid
                elif nav.isdigit():
                    nav_page = int(nav)
                    if 1 <= nav_page <= total_pages:
                        current_page = nav_page

                    # Handle invalid commands
                    else:
                        print(f"Invalid page. Please choose a number between 1 and {total_pages}.")
                else:
                    print("Invalid command. Use 'next', 'prev', a page number, or 'exit'.")

        # Exit the program
        elif user_choice == '4':
            print("Goodbye.. Exiting...")
            break

        else:
            # Handle invalid menu input
            print("Please enter a valid choice.")



