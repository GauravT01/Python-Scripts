import mysql.connector
import json

# Database connection information
db_config = {
    'host': "sem-prod-cloud-sql-proxy.service.gcp-us-west-1.consul",
    'user': "sem_gizmo_api_ro",
    'password': "v95MbUkEKuFbWYN9PVB9MFFLMxFsayAx",
    'database': "wordpress_info",
    'charset': 'utf8mb4',
    'collation': 'utf8mb4_unicode_ci'
}

def get_site_info_by_domain(domain):
    connection = None
    try:
        # Connect to the database
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor(dictionary=True)

        # SQL query with the variable domain
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

            print(f"Site Name: {site_name}")
            print("Extracted Data:")

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
                parking_url = parking_url if parking_url else "N/A"
                
                # Extract data per structure
                print(f"Source Key: {source_key}")
                print(f"  Customer ID: {customer_id}")
                print(f"  Pixel ID: {pixel_to_display}")
                print(f"  Pixel Name: {pixel_name}")
                print(f"  Route: {route}")
                print(f"  Parking URL: {parking_url}")
                print("")

        else:
            print(f"No results found for domain: {domain}")

    except mysql.connector.Error as e:
        print(f"Error occurred: {e}")
    
    finally:
        if connection:
            connection.close()

# Get domain input from the user
domain = input("Enter the domain to search for: ")

# Fetch and output the site info
get_site_info_by_domain(domain)