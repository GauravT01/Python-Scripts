import csv
import mysql.connector
from mysql.connector import Error
import pandas as pd
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
        print(f"Connection to {db_name} database successful")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred")
        return None

def fetch_and_save_articles(domain, tld):
    db_name = domain + "_" + tld
    Domain_name = domain + "." + tld

    connection = create_connection(db_name)
    
    if connection:
        try:
            # Create a cursor object
            cursor = connection.cursor()

            # SQL query to fetch the desired data
            query = f'''
            SELECT
                wp_post.ID, 
                wp_post.post_title,
                wp_post.post_date,
                wp_post.post_status,
                CONCAT("https://{Domain_name}/", wp_post.post_name, "/") AS slug,
                (
                    SELECT GROUP_CONCAT(wt.name) AS name
                    FROM wp_term_relationships wtr
                    JOIN wp_term_taxonomy wtt ON wtt.term_taxonomy_id = wtr.term_taxonomy_id
                    JOIN wp_terms wt ON wt.term_id = wtt.term_id
                    WHERE wtr.object_id = wp_post.ID AND wtt.taxonomy = 'post_tag'
                ) AS tags,
                (
                    SELECT GROUP_CONCAT(wt.name) AS name
                    FROM wp_term_relationships wtr
                    JOIN wp_term_taxonomy wtt ON wtt.term_taxonomy_id = wtr.term_taxonomy_id
                    JOIN wp_terms wt ON wt.term_id = wtt.term_id
                    WHERE wtr.object_id = wp_post.ID AND wtt.taxonomy = 'category'
                ) AS categories,
                (
                    SELECT CONCAT("https://{Domain_name}/wp-content/uploads/", wpm2.meta_value)
                    FROM wp_postmeta wpm1
                    JOIN wp_postmeta wpm2 ON wpm1.meta_value = wpm2.post_id
                    WHERE wpm1.meta_key = '_thumbnail_id' AND wpm1.post_id = wp_post.ID
                    LIMIT 1
                ) AS image_url,
                (
                    SELECT meta_value 
                    FROM wp_postmeta wpm3 
                    WHERE wpm3.post_id = wp_post.ID AND wpm3.meta_key = 'wp_sem_cf_subtitle'
                    LIMIT 1
                ) AS subtitle
            FROM wp_posts wp_post
            WHERE wp_post.post_type = 'post' AND wp_post.post_status = 'publish';
            '''#wp_post.post_content removed
            
            # Execute the SQL query
            cursor.execute(query)

            # Fetch all the rows
            result = cursor.fetchall()
            column_names = cursor.column_names

            # Convert the result to a list of dictionaries
            result_dicts = [dict(zip(column_names, row)) for row in result]

            # Define the path to the target directory
            target_folder = r'/mnt/c/Users/tiwari.g/Downloads'
            if not os.path.exists(target_folder):
                os.makedirs(target_folder)

            csv_file = os.path.join(target_folder, f'{Domain_name}_articledump.csv')

            # Write the JSON data to a CSV file with progress bar
            with open(csv_file, 'w', newline='', encoding='utf-8') as csv_obj:
                csv_writer = csv.writer(csv_obj)
                # Write the header
                csv_writer.writerow(column_names)
                # Write the rows with progress bar
                for row_dict in tqdm(result_dicts, desc=f"Downloading {Domain_name}", unit="rows"):
                    csv_writer.writerow(row_dict.values())

            print(f"Data has been written to {csv_file} successfully.")

        except Error as err:
            print(f"Error: {err}")

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print(f"MySQL connection to {db_name} is closed")

def main():
    # Specify the absolute path to the Excel file
    excel_file_path = '/mnt/c/Users/tiwari.g/OneDrive - MEDIA.NET SOFTWARE SERVICES (INDIA) PRIVATE LIMITED/Documents/Python for automate/Bulk_Article_dump/ADdata.xlsx'
    
    try:
        # Read the Excel file using pandas
        df = pd.read_excel(excel_file_path)
    except FileNotFoundError:
        print(f"File not found: {excel_file_path}")
        return
    except Exception as e:
        print(f"An error occurred: {e}")
        return

    # Iterate over each row in the DataFrame
    for index, row in df.iterrows():
        domain, tld = row['domain'], row['tld']
        print(f"Processing {domain}.{tld}")
        fetch_and_save_articles(domain, tld)

if __name__ == "__main__":
    main()
