from fastapi import FastAPI, UploadFile, File, HTTPException
import pandas as pd
import mysql.connector
import csv
import os
from mysql.connector import Error
from fastapi.responses import HTMLResponse, FileResponse
from tqdm import tqdm

app = FastAPI()

# Define the MySQL connection function
def create_connection(db_name):
    try:
        connection = mysql.connector.connect(
            host="sem-prod-cloud-sql-proxy.service.gcp-us-west-1.consul",
            user="sem_wordpress_sites_ro",
            password="8HMATYXt0K1J3FdeuG7NR6Dx4lkgt4ei",
            database=db_name,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        print(f"Connected to database: {db_name}")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred.")
        return None

# Fetch articles and save them into CSV file for each domain
def fetch_and_save_articles(domain, tld, output_folder):
    db_name = f"{domain}_{tld}"
    domain_name = f"{domain}.{tld}"

    connection = create_connection(db_name)

    if connection:
        try:
            cursor = connection.cursor(dictionary=True)

            query = f'''
            SELECT
                wp_post.ID, 
                wp_post.post_title,
                wp_post.post_date,
                wp_post.post_status,
                CONCAT("https://{domain_name}/", wp_post.post_name, "/") AS slug,
                (
                    SELECT GROUP_CONCAT(wt.name)
                    FROM wp_term_relationships wtr
                    JOIN wp_term_taxonomy wtt ON wtt.term_taxonomy_id = wtr.term_taxonomy_id
                    JOIN wp_terms wt ON wt.term_id = wtt.term_id
                    WHERE wtr.object_id = wp_post.ID AND wtt.taxonomy = 'post_tag'
                ) AS tags,
                (
                    SELECT GROUP_CONCAT(wt.name)
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
            '''
            
            cursor.execute(query)
            result = cursor.fetchall()
            column_names = cursor.column_names

            # Ensure the output folder exists
            if not os.path.exists(output_folder):
                os.makedirs(output_folder)

            # Write to CSV
            csv_file = os.path.join(output_folder, f'{domain_name}_articledump.csv')
            with open(csv_file, 'w', newline='', encoding='utf-8') as f:
                csv_writer = csv.writer(f)
                csv_writer.writerow(column_names)  # Write CSV header
                for row_dict in tqdm(result, desc=f"Processing {domain_name}", unit="rows"):
                    csv_writer.writerow([str(value) if value else '' for value in row_dict.values()])

            print(f"Data saved to {csv_file}")
            return csv_file

        except Error as err:
            print(f"Error: {err}")
            return None

        finally:
            if connection.is_connected():
                cursor.close()
                connection.close()
                print(f"MySQL connection to {db_name} closed.")
    else:
        print(f"Could not connect to {db_name}")
        return None

# API endpoint to upload an Excel file and process it
@app.post("/upload-file/", response_class=HTMLResponse)
async def upload_file(file: UploadFile = File(...)):
    # Ensure the uploaded file is an Excel file
    if not file.filename.endswith(('.xls', '.xlsx')):
        raise HTTPException(status_code=400, detail="The uploaded file must be an Excel file (.xls, .xlsx)")
    
    try:
        # Read Excel file into pandas DataFrame
        df = pd.read_excel(file.file)

        # Validate that the required columns exist
        if 'domain' not in df.columns or 'tld' not in df.columns:
            raise HTTPException(status_code=400, detail="The Excel file must have 'domain' and 'tld' columns.")

        # Prepare directories to store CSVs
        output_folder = './article_dumps'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        download_links = []  # Store download links to present to the user
        # Iterate over each domain and process it
        for index, row in df.iterrows():
            domain, tld = row['domain'], row['tld']
            print(f"Processing {domain}.{tld}")
            csv_file_path = fetch_and_save_articles(domain, tld, output_folder)
            if csv_file_path:
                # Create a download link for each generated CSV file
                download_links.append(f"<a href='/download/{os.path.basename(csv_file_path)}' target='_blank'>Download {domain}.{tld}_articledump.csv</a>")

        # Generate HTML with download links for each CSV
        links_html = "<br>".join(download_links)
        return f"""
        <html>
        <head><title>Article Dumps</title></head>
        <body>
        <h1>Download Links</h1>
        {links_html}
        </body>
        </html>
        """

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing Excel file: {str(e)}")

# API endpoint to serve the generated CSV files
@app.get("/download/{file_name}")
async def download_file(file_name: str):
    file_path = os.path.join('./article_dumps', file_name)
    
    if os.path.exists(file_path):
        return FileResponse(path=file_path, filename=file_name)
    else:
        raise HTTPException(status_code=404, detail=f"File {file_name} not found.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)