import mysql.connector
import json
import csv
import pandas as pd  # For reading data from Excel sheets

# Database connection information
db_config = {
    'host': "sem-prod-cloud-sql-proxy.service.gcp-us-west-1.consul",
    'user': "sem_gizmo_api_ro",
    'password': "v95MbUkEKuFbWYN9PVB9MFFLMxFsayAx",
    'database': "wordpress_info",
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

# Define the column headers for the output CSV file.
output_columns = ['Domain', 'Customer ID', 'Pixel ID', 'Pixel Name', 'Route', 'Parking URL']

def get_site_info_by_domain(domain):
    """
    Fetch site info from the database based on the domain.
    Returns the result as a list of dictionaries.
    """
    connection = None
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        # SQL query to match site name (domain)
        query = '''
            SELECT site_name, json_config 
            FROM wordpress_info.credentials 
            WHERE site_name LIKE %s 
            ORDER BY id DESC LIMIT 1;
        '''
        
        # Execute the query
        cursor.execute(query, (f'%{domain}%',))
        
        # Fetch the result
        result = cursor.fetchone()

        if result:
            site_name = result.get("site_name")
            json_config_str = result.get("json_config")

            # Convert json_config string to a Python dictionary
            json_config = json.loads(json_config_str)

            extracted_data = []  # To hold each row of extracted data
            
            # Extract pixel data and URLs for each source in customer's configuration
            sources = json_config.get("sources", {})

            for source_key, source_value in sources.items():
                # Extract the appropriate fields (ensure Pixel ID will be treated as text)
                customer_id = str(source_value.get("customer_id", ""))  # Convert numbers to string
                conversion_id = str(source_value.get("conversion_id", ""))  # Convert numbers to string
                conversion_label = source_value.get("conversion_label", "")
                route = source_value.get("route", "")
                pixel_id = source_value.get("pixel_id", "")

                # Handle empty pixel ID by falling back on N/A
                if pixel_id:
                    # Prefix pixel_id with a single quote `'` to force Excel to treat it as text
                    pixel_id = f"'{pixel_id}"
                else:
                    pixel_id = "N/A"  # Set N/A if pixel ID is empty

                parking_url = source_value.get("parking_crew_url", "").strip()

                # If no conversion_id, fallback to pixel_id. If none, display N/A.
                pixel_to_display = conversion_id if conversion_id else (pixel_id if pixel_id else "N/A")
                
                # If no conversion_label, display N/A.
                pixel_name = conversion_label if conversion_label else "N/A"
                
                # Ensure that if the Parking URL is missing, we display "N/A"
                parking_url = {"base_url": parking_url} if parking_url else {"base_url": "N/A"}

                # Append the extracted data as a dictionary (row)
                extracted_data.append({
                    'Domain': site_name,
                    'Customer ID': customer_id,  # No truncation for long numbers
                    'Pixel ID': pixel_to_display,  # No truncation for Pixel ID (Handled as string)
                    'Pixel Name': pixel_name,
                    'Route': route,
                    'Parking URL': json.dumps(parking_url)  # JSON format
                })

            return extracted_data

        else:
            print(f"No results found for domain: {domain}")
            return []

    except mysql.connector.Error as e:
        print(f"Error occurred: {e}")
        return []

    finally:
        if connection:
            connection.close()

def process_domains_from_excel(input_excel_path, output_csv_path):
    """
    Read domain names from an Excel file (`domain_name` column) and 
    fetch details from the database and merge them into a CSV.
    """
    try:
        # Open the CSV output file to write data
        with open(output_csv_path, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=output_columns)
            
            # Write the CSV header
            writer.writeheader()

            # Read the Excel file that has the domain names
            df = pd.read_excel(input_excel_path)

            # Extract domain names from the 'domain_name' column
            domains = df['domain_name'].tolist()

            # Process each domain separately
            for domain in domains:
                if domain:
                    # Fetch site info for the given domain
                    data = get_site_info_by_domain(domain.strip())
                    
                    # Write data into the CSV
                    for row_data in data:
                        writer.writerow({
                            'Domain': row_data['Domain'],
                            'Customer ID': row_data['Customer ID'],  # Treated as string
                            'Pixel ID': row_data['Pixel ID'],  # Ensure Pixel ID is treated as text
                            'Pixel Name': row_data['Pixel Name'],
                            'Route': row_data['Route'],
                            'Parking URL': row_data['Parking URL']  # JSON URL field remains safe as string
                        })

        print(f"Data extraction completed. Results saved in '{output_csv_path}'.")

    except Exception as e:
        print(f"Error processing files: {e}")

# Define the input Excel file path and the output CSV file path
input_excel_file = '/mnt/c/Users/tiwari.g/OneDrive - MEDIA.NET SOFTWARE SERVICES (INDIA) PRIVATE LIMITED/Documents/Python for automate/wp_config/wpconfig_csv.xlsx'
output_csv_file = '/mnt/c/Users/tiwari.g/Downloads/wpconfig_details.csv'

# Process the domains from Excel and save the result in a CSV
process_domains_from_excel(input_excel_file, output_csv_file)

# to  get account id.
# select adword_account_id,customer_id,name,token from accounts
# where customer_id in ('8CU4OR3OD','8CU942HB6')