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
    Return the result as a list of dictionaries.
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
                # For each source, extract the appropriate fields
                customer_id = source_value.get("customer_id", "")
                conversion_id = source_value.get("conversion_id", "")
                conversion_label = source_value.get("conversion_label", "")
                route = source_value.get("route", "")
                pixel_id = source_value.get("pixel_id", "")
                parking_url = source_value.get("parking_crew_url", "").strip()  # Handle empty strings

                # If no conversion_id, fallback to pixel_id. If none, display N/A.
                pixel_to_display = conversion_id if conversion_id else (pixel_id if pixel_id else "N/A")
                
                # If no conversion_label, display N/A.
                pixel_name = conversion_label if conversion_label else "N/A"
                
                # Ensure the Parking URL defaults to 'N/A' when not present or empty
                parking_url = {"base_url": parking_url} if parking_url else {"base_url": "N/A"}
                
                # Append the extracted data as a dictionary (row)
                extracted_data.append({
                    'Domain': site_name,
                    'Customer ID': customer_id,
                    'Pixel ID': pixel_to_display,
                    'Pixel Name': pixel_name,
                    'Route': route,
                    'Parking URL': json.dumps(parking_url)  # Convert to JSON format string
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
    merge the results into a single CSV, `wpconfig_details.csv`.
    """
    try:
        # Open the output CSV file in write mode
        with open(output_csv_path, mode='w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=output_columns)
            
            # Write the CSV header row
            writer.writeheader()

            # Read the Excel file into a pandas DataFrame
            df = pd.read_excel(input_excel_path)

            # Extract the domain names - assuming a column named 'domain_name'
            domains = df['domain_name'].tolist()

            # Process each domain
            for domain in domains:
                if domain:
                    # Fetch site info for the current domain
                    data = get_site_info_by_domain(domain.strip())
                    
                    # Write each row of data to the common output CSV file
                    for row_data in data:
                        writer.writerow(row_data)

        print(f"Data extraction completed. Results are saved in '{output_csv_path}'.")

    except Exception as e:
        print(f"Error processing files: {e}")

# Paths to the input Excel file and the output CSV file
input_excel_file = '/mnt/c/Users/tiwari.g/OneDrive - MEDIA.NET SOFTWARE SERVICES (INDIA) PRIVATE LIMITED/Documents/Python for automate/wp_config/wpconfig_csv.xlsx'
output_csv_file = '/mnt/c/Users/tiwari.g/Downloads/wpconfig_details.csv'

# Process domains from Excel and save merged results into one CSV file
process_domains_from_excel(input_excel_file, output_csv_file)