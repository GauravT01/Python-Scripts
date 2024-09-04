from fastapi import FastAPI, HTTPException, Query
from typing import Optional
import mysql.connector
from mysql.connector import Error
import csv
import os
from tqdm import tqdm

app = FastAPI()


def create_connection(db_name: str):
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


def fetch_data(connection, domain_name: str):
    cursor = connection.cursor()
    try:
        # SQL query to fetch the desired data
        query = f'''
        SELECT
            wp_post.ID, 
            wp_post.post_title,
            wp_post.post_date,
            wp_post.post_status,
            CONCAT("https://{domain_name}/", wp_post.post_name, "/") AS slug,
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
                SELECT CONCAT("https://{domain_name}/wp-content/uploads/", wpm2.meta_value)
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
        '''  # wp_post.post_content removed from query

        cursor.execute(query)
        result = cursor.fetchall()
        column_names = cursor.column_names
        return result, column_names

    except Error as err:
        raise HTTPException(status_code=500, detail=str(err))

    finally:
        cursor.close()


def write_to_csv(result_dicts, column_names, domain_name):
    # downloads_folder = r'/mnt/c/Users/tiwari.g/Downloads'
    downloads_folder = get_downloads_folder()
    csv_file = os.path.join(downloads_folder, f'{domain_name}_articledump.csv')

    if not os.path.exists(downloads_folder):
        os.makedirs(downloads_folder)

    with open(csv_file, 'w', newline='', encoding='utf-8-sig') as csv_obj:
        csv_writer = csv.writer(csv_obj)
        csv_writer.writerow(column_names)
        for row_dict in tqdm(result_dicts, desc="Downloading", unit="rows"):
            csv_writer.writerow([str(value) if value is not None else '' for value in row_dict.values()])

    return csv_file

def get_downloads_folder():
    # Determine the user's Downloads folder path depending on OS
    if os.name == "nt":
        # Windows
        downloads_path = str(Path.home() / "Downloads")
    else:
        # Linux or MacOS
        downloads_path = str(Path.home() / "Downloads") # type: ignore
    
    return downloads_path

@app.post("/articledump/")
def articledump(domain: str = Query(..., description="Enter the domain name you want to connect to."),
                        tld: str = Query(..., description="Enter the TLD of your domain name.")):
    db_name = domain + "_" + tld
    domain_name = f"{domain}.{tld}"

    connection = create_connection(db_name)
    if connection:
        try:
            result, column_names = fetch_data(connection, domain_name)
            result_dicts = [dict(zip(column_names, row)) for row in result]

            # Write data to CSV
            csv_file = write_to_csv(result_dicts, column_names, domain_name)

            return {"message": f"Data has been written to {csv_file} successfully."}

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        finally:
            if connection.is_connected():
                connection.close()
                print("MySQL connection is closed")

    else:
        raise HTTPException(status_code=500, detail="Cannot connect to the database.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)