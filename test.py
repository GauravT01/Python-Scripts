# from fastapi import FastAPI, HTTPException, Query
# from typing import Optional
# import mysql.connector
# from mysql.connector import Error
# import csv
# import os
# from tqdm import tqdm
# from pathlib import Path  

# app = FastAPI()



# def create_connection(db_name: str):
#     try:
#         connection = mysql.connector.connect(
#             host="sem-prod-wordpress-galera.service.gcp-us-west-1.consul",
#             user="sem_wordpress_sites_ro",
#             password="OkwPBEkE2o2jYKuhFmH0lQA09Up5nuEV",
#             database=db_name,
#             charset='utf8mb4',
#             collation='utf8mb4_unicode_ci'
#         )
#         print(f"Connection to {db_name} database successful")
#         return connection
#     except Error as e:
#         print(f"The error '{e}' occurred")
#         return None


# def fetch_data(connection, domain_name: str):
#     cursor = connection.cursor()
#     try:
#         # SQL query to fetch the desired data
#         query = f'''
#         SELECT
#             wp_post.ID, 
#             wp_post.post_title,
#             wp_post.post_date,
#             wp_post.post_status,
#             CONCAT("https://{domain_name}/", wp_post.post_name, "/") AS slug,
#             (
#                 SELECT GROUP_CONCAT(wt.name) AS name
#                 FROM wp_term_relationships wtr
#                 JOIN wp_term_taxonomy wtt ON wtt.term_taxonomy_id = wtr.term_taxonomy_id
#                 JOIN wp_terms wt ON wt.term_id = wtt.term_id
#                 WHERE wtr.object_id = wp_post.ID AND wtt.taxonomy = 'post_tag'
#             ) AS tags,
#             (
#                 SELECT GROUP_CONCAT(wt.name) AS name
#                 FROM wp_term_relationships wtr
#                 JOIN wp_term_taxonomy wtt ON wtt.term_taxonomy_id = wtr.term_taxonomy_id
#                 JOIN wp_terms wt ON wt.term_id = wtt.term_id
#                 WHERE wtr.object_id = wp_post.ID AND wtt.taxonomy = 'category'
#             ) AS categories,
#             (
#                 SELECT CONCAT("https://{domain_name}/wp-content/uploads/", wpm2.meta_value)
#                 FROM wp_postmeta wpm1
#                 JOIN wp_postmeta wpm2 ON wpm1.meta_value = wpm2.post_id
#                 WHERE wpm1.meta_key = '_thumbnail_id' AND wpm1.post_id = wp_post.ID
#                 LIMIT 1
#             ) AS image_url,
#             (
#                 SELECT meta_value 
#                 FROM wp_postmeta wpm3 
#                 WHERE wpm3.post_id = wp_post.ID AND wpm3.meta_key = 'wp_sem_cf_subtitle'
#                 LIMIT 1
#             ) AS subtitle            
#         FROM wp_posts wp_post
#         WHERE wp_post.post_type = 'post' AND wp_post.post_status = 'publish';
#         '''  # wp_post.post_content removed from query

#         cursor.execute(query)
#         result = cursor.fetchall()
#         column_names = cursor.column_names
#         return result, column_names

#     except Error as err:
#         raise HTTPException(status_code=500, detail=str(err))

#     finally:
#         cursor.close()


# def write_to_csv(result_dicts, column_names, domain_name):
#     # downloads_folder = r'/mnt/c/Users/tiwari.g/Downloads'
#     downloads_folder = get_downloads_folder()
#     csv_file = os.path.join(downloads_folder, f'{domain_name}_articledump.csv')

#     if not os.path.exists(downloads_folder):
#         os.makedirs(downloads_folder)

#     with open(csv_file, 'w', newline='', encoding='utf-8-sig') as csv_obj:
#         csv_writer = csv.writer(csv_obj)
#         csv_writer.writerow(column_names)
#         for row_dict in tqdm(result_dicts, desc="Downloading", unit="rows"):
#             csv_writer.writerow([str(value) if value is not None else '' for value in row_dict.values()])

#     return csv_file

# def get_downloads_folder():
#     # Determine the user's Downloads folder path depending on OS
#     if os.name == "nt":
#         # Windows
#         downloads_path = str(Path.home() / "Downloads")
#     else:
#         # Linux or MacOS
#         downloads_path = str(Path.home() / "Downloads") # type: ignore
    
#     return downloads_path

# @app.post("/articledump/")
# def articledump(domain: str = Query(..., description="Enter the domain name you want to connect to."),
#                         tld: str = Query(..., description="Enter the TLD of your domain name.")):
#     db_name = domain + "_" + tld
#     domain_name = f"{domain}.{tld}"

#     connection = create_connection(db_name)
#     if connection:
#         try:
#             result, column_names = fetch_data(connection, domain_name)
#             result_dicts = [dict(zip(column_names, row)) for row in result]

#             # Write data to CSV
#             csv_file = write_to_csv(result_dicts, column_names, domain_name)

#             return {"message": f"Data has been written to {csv_file} successfully."}

#         except Exception as e:
#             raise HTTPException(status_code=500, detail=str(e))

#         finally:
#             if connection.is_connected():
#                 connection.close()
#                 print("MySQL connection is closed")

#     else:
#         raise HTTPException(status_code=500, detail="Cannot connect to the database.")


# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="127.0.0.1", port=8000)
# import mysql.connector
# import json
# import csv
# import pandas as pd  # For reading data from Excel sheets

# # Database connection information
# db_config = {
#     'host': "sem-prod-cloud-sql-proxy.service.gcp-us-west-1.consul",
#     'user': "sem_gizmo_api_ro",
#     'password': "v95MbUkEKuFbWYN9PVB9MFFLMxFsayAx",
#     'database': "wordpress_info",
#     'charset': 'utf8mb4',
#     'collation': 'utf8mb4_unicode_ci'
# }

# # Define the column headers for the output CSV file.
# output_columns = ['Domain', 'Customer ID', 'Pixel ID', 'Pixel Name', 'Route', 'Parking URL']

# def get_site_info_by_domain(domain):
#     """
#     Fetch site info from the database based on the domain.
#     Return the result as a list of dictionaries.
#     """
#     connection = None
#     try:
#         # Connect to the database
#         connection = mysql.connector.connect(**db_config)
#         cursor = connection.cursor(dictionary=True)

#         # SQL query to match site name (domain)
#         query = '''
#             SELECT site_name, json_config 
#             FROM wordpress_info.credentials 
#             WHERE site_name LIKE %s 
#             ORDER BY id DESC LIMIT 1;
#         '''
        
#         # Execute the query
#         cursor.execute(query, (f'%{domain}%',))
        
#         # Fetch the result
#         result = cursor.fetchone()

#         if result:
#             site_name = result.get("site_name")
#             json_config_str = result.get("json_config")

#             # Convert json_config string to a Python dictionary
#             json_config = json.loads(json_config_str)

#             extracted_data = []  # To hold each row of extracted data
            
#             # Extract pixel data and URLs for each source in customer's configuration
#             sources = json_config.get("sources", {})

#             for source_key, source_value in sources.items():
#                 # For each source, extract the appropriate fields
#                 customer_id = source_value.get("customer_id", "")
#                 conversion_id = source_value.get("conversion_id", "")
#                 conversion_label = source_value.get("conversion_label", "")
#                 route = source_value.get("route", "")
#                 pixel_id = source_value.get("pixel_id", "")
#                 parking_url = source_value.get("parking_crew_url", "").strip()  # Handle empty strings

#                 # If no conversion_id, fallback to pixel_id. If none, display N/A.
#                 pixel_to_display = conversion_id if conversion_id else (pixel_id if pixel_id else "N/A")
                
#                 # If no conversion_label, display N/A.
#                 pixel_name = conversion_label if conversion_label else "N/A"
                
#                 # Ensure the Parking URL defaults to 'N/A' when not present or empty
#                 parking_url = parking_url if parking_url else "N/A"
                
#                 # Append the extracted data as a dictionary (row)
#                 extracted_data.append({
#                     'Domain': site_name,
#                     'Customer ID': customer_id,
#                     'Pixel ID': pixel_to_display,
#                     'Pixel Name': pixel_name,
#                     'Route': route,
#                     'Parking URL': parking_url
#                 })

#             return extracted_data
#         else:
#             print(f"No results found for domain: {domain}")
#             return []

#     except mysql.connector.Error as e:
#         print(f"Error occurred: {e}")
#         return []
    
#     finally:
#         if connection:
#             connection.close()

# def process_domains_from_excel(input_excel_path, output_csv_path):
#     """
#     Read domain names from an Excel file (`domain_name` column) and 
#     merge the results into a single CSV, `wpconfig_details.csv`.
#     """
#     try:
#         # Open the output CSV file in write mode
#         with open(output_csv_path, mode='w', newline='', encoding='utf-8') as outfile:
#             writer = csv.DictWriter(outfile, fieldnames=output_columns)
            
#             # Write the CSV header row
#             writer.writeheader()

#             # Read the Excel file into a pandas DataFrame
#             df = pd.read_excel(input_excel_path)

#             # Extract the domain names - assuming a column named 'domain_name'
#             domains = df['domain_name'].tolist()

#             # Process each domain
#             for domain in domains:
#                 if domain:
#                     # Fetch site info for the current domain
#                     data = get_site_info_by_domain(domain.strip())
                    
#                     # Write each row of data to the common output CSV file
#                     for row_data in data:
#                         writer.writerow(row_data)

#         print(f"Data extraction completed. Results are saved in '{output_csv_path}'.")

#     except Exception as e:
#         print(f"Error processing files: {e}")

# # Paths to the input Excel file and the output CSV file
# input_excel_file = '/mnt/c/Users/tiwari.g/OneDrive - MEDIA.NET SOFTWARE SERVICES (INDIA) PRIVATE LIMITED/Documents/Python for automate/wp_config/wpconfig_csv.xlsx'
# output_csv_file = '/mnt/c/Users/tiwari.g/Downloads/wpconfig_details.csv'

# # Process domains from Excel and save merged results into one CSV file
# process_domains_from_excel(input_excel_file, output_csv_file)

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