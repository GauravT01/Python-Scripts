import mysql.connector
from mysql.connector import Error
import csv
import os
from tqdm import tqdm

def create_connection(db_name):
    try:
        connection = mysql.connector.connect(
            host="sem-prod-wordpress-galera.service.gcp-us-west-1.consul",
            user="sem_wordpress_sites_ro",
            password="OkwPBEkE2o2jYKuhFmH0lQA09Up5nuEV",
            database=db_name,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        connection.set_charset_collation('utf8mb4', 'utf8mb4_unicode_ci')
        print(f"Connection to {db_name} database successful")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred")
        return None

def sanitize_value(value):
    """Sanitize individual cell values to ensure UTF-8 consistency."""
    if isinstance(value, bytes):
        try:
            return value.decode('utf-8')
        except UnicodeDecodeError:
            # Handle specific erroneous byte sequences by trying alternative decoding
            return value.decode('latin1')
    return str(value)

def main():
    domain = input("Enter the domain name you want to connect to: ")
    tld = input("Enter the TLD of your domain name: ")
    db_name = domain + "_" + tld
    Domain_name = domain + "." + tld

    connection = create_connection(db_name)

    if connection:
        try:
            cursor = connection.cursor()
            cursor.execute("SET NAMES utf8mb4;")
            cursor.execute("SET CHARACTER SET utf8mb4;")
            cursor.execute("SET collation_connection = 'utf8mb4_unicode_ci';")

            # SQL query to fetch the desired data
            query = f'''
            SELECT
                wp_posts.ID,
                wp_posts.post_title,
                wp_posts.post_date,
                wp_posts.post_status,
                CONCAT("https://{Domain_name}/", wp_posts.post_name, "/") AS slug,
                (SELECT GROUP_CONCAT(wt.name) FROM wp_term_relationships wtr
                 JOIN wp_term_taxonomy wtt ON wtt.term_taxonomy_id = wtr.term_taxonomy_id
                 JOIN wp_terms wt ON wt.term_id = wtt.term_id
                 WHERE wtr.object_id = wp_posts.ID AND wtt.taxonomy = 'post_tag') AS tags,
                (SELECT GROUP_CONCAT(wt.name) FROM wp_term_relationships wtr
                 JOIN wp_term_taxonomy wtt ON wtt.term_taxonomy_id = wtr.term_taxonomy_id
                 JOIN wp_terms wt ON wt.term_id = wtt.term_id
                 WHERE wtr.object_id = wp_posts.ID AND wtt.taxonomy = 'category') AS categories,
                (SELECT CONCAT("https://{Domain_name}/wp-content/uploads/", wpm2.meta_value)
                 FROM wp_postmeta wpm1
                 JOIN wp_postmeta wpm2 ON wpm1.meta_value = wpm2.post_id
                 WHERE wpm1.meta_key = '_thumbnail_id' AND wpm1.post_id = wp_posts.ID LIMIT 1) AS image_url,
                (SELECT meta_value FROM wp_postmeta wpm3 
                 WHERE wpm3.post_id = wp_posts.ID AND wpm3.meta_key = 'wp_sem_cf_subtitle' LIMIT 1) AS subtitle,
                wp_posts.post_content
            FROM wp_posts
            WHERE wp_posts.post_type = 'post' AND wp_posts.post_status = 'publish';
            '''

            # Execute the SQL query
            cursor.execute(query)

            # Fetch all the rows
            result = cursor.fetchall()
            column_names = [i[0] for i in cursor.description]

            # Convert the result to a list of dictionaries
            result_dicts = []
            for row in tqdm(result, desc="Processing rows", unit="row"):
                sanitized_row = [sanitize_value(value) for value in row]
                result_dicts.append(dict(zip(column_names, sanitized_row)))

            # Define the path to the Windows Downloads directory in WSL
            downloads_folder = r'/mnt/c/Users/tiwari.g/Downloads'
            csv_file = os.path.join(downloads_folder, f'{Domain_name}_articledump.csv')

            # Ensure the downloads folder exists
            if not os.path.exists(downloads_folder):
                os.makedirs(downloads_folder)

            # Write the JSON data to a CSV file with progress bar
            with open(csv_file, 'w', newline='', encoding='utf-8') as csv_obj:
                csv_writer = csv.writer(csv_obj)
                # Write the header
                csv_writer.writerow(column_names)
                # Write the rows with progress bar
                for row_dict in tqdm(result_dicts, desc="Writing to CSV", unit="rows"):
                    csv_writer.writerow([value for value in row_dict.values()])

            print(f"Data has been written to {csv_file} successfully.")

        except Error as err:
            print(f"Error: {err}")
        
        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print("MySQL connection is closed")

if __name__ == "__main__":
    main()