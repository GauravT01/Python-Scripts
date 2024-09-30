import mysql.connector
from mysql.connector import Error
import csv
import os
from tqdm import tqdm

def create_connection(db_name):
    try:
        connection = mysql.connector.connect(
            host="sem-prod-cloud-sql-proxy.service.gcp-us-west-1.consul",  # Updated host
            port=3308,  # Updated port
            user="sem_wordpress_sites_ro",  # Updated user
            password="8HMATYXt0K1J3FdeuG7NR6Dx4lkgt4ei",  # Updated password
            database=db_name,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        print(f"Connection to {db_name} database successful")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred")
        return None

def main():
    domain = input("Enter the domain name you want to connect to: ")
    tld = input("Enter the TLD of your domain name: ")
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
            ''' # wp_post.post_content removed from query

            # Execute the SQL query
            cursor.execute(query)

            # Fetch all the rows
            result = cursor.fetchall()
            column_names = cursor.column_names

            # Convert the result to a list of dictionaries
            result_dicts = [dict(zip(column_names, row)) for row in result]

            # Define the path to the Windows Downloads directory in WSL
            downloads_folder = r'/mnt/c/Users/tiwari.g/Downloads'
            csv_file = os.path.join(downloads_folder, f'{Domain_name}_articledump.csv')

            # Ensure the downloads folder exists
            if not os.path.exists(downloads_folder):
                os.makedirs(downloads_folder)

            # Write the JSON data to a CSV file with progress bar
            with open(csv_file, 'w', newline='', encoding='utf-8-sig') as csv_obj: #utf-8-sig
                csv_writer = csv.writer(csv_obj)
                # Write the header
                csv_writer.writerow(column_names)
                # Write the rows with progress bar
                for row_dict in tqdm(result_dicts, desc="Downloading", unit="rows"):
                    csv_writer.writerow([str(value) if value is not None else '' for value in row_dict.values()])
                    # csv_writer.writerow(row_dict.values())
                    # print(row_dict.values())

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