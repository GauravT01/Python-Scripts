from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
import csv
import mysql.connector
from mysql.connector import Error
import io
import asyncio

app = FastAPI()

# Function to connect to the MySQL database using mysql.connector
def create_connection(db_name):
    try:
        connection = mysql.connector.connect(
            host="sem-prod-cloud-sql-proxy.service.gcp-us-west-1.consul",
            port=3308,
            user="sem_wordpress_sites_ro",
            password="8HMATYXt0K1J3FdeuG7NR6Dx4lkgt4ei",
            database=db_name,
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        print(f"Connected to database: {db_name}")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred while connecting to the database")
        return None

# Function to fetch article data from the MySQL database
def fetch_data_from_db(connection, domain_name: str):
    try:
        cursor = connection.cursor(dictionary=True)  # Use dictionary cursor to fetch results in key-value format.
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
        result = cursor.fetchall()  # Fetch all data from the database
        column_names = cursor.column_names  # Retrieve column names from the query
        print(f"Fetched {len(result)} rows from the database.")
        return (result, column_names)
    except Error as err:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(err)}")
    finally:
        cursor.close()

# CSV streaming generator
async def stream_csv(result_dicts, column_names):
    output = io.StringIO()
    writer = csv.writer(output)
    # Write CSV headers only once
    writer.writerow(column_names)
    yield output.getvalue()

    # Now stream individual rows
    for row in result_dicts:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow([str(row[col]) if row[col] is not None else '' for col in column_names])
        yield output.getvalue()

# Main FastAPI end-point to generate the CSV file from article data
@app.post("/articledump/")
async def articledump(
    domain: str = Query(..., description="Enter the domain name"),
    tld: str = Query(..., description="Enter the TLD of the domain")
):
    db_name = f"{domain}_{tld}"
    domain_name = f"{domain}.{tld}"

    # Establish a database connection
    connection = create_connection(db_name)

    if connection:
        try:
            # Fetch the article data from the DB
            result, column_names = fetch_data_from_db(connection, domain_name)
            if not result:
                raise HTTPException(status_code=404, detail="No articles found.")

            # Stream CSV response
            print(f"Generating CSV for {domain_name} with {len(result)} rows.")
            return StreamingResponse(
                stream_csv(result, column_names),
                media_type="text/csv",
                headers={"Content-Disposition": f"attachment; filename={domain_name}_articledump.csv"}
            )

        except Exception as e:
            print(f"Error while generating CSV: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Error generating CSV: {str(e)}")

        finally:
            # Close the DB connection after query completes
            connection.close()
            print(f"Database connection to {db_name} closed.")

    else:
        raise HTTPException(status_code=500, detail="Failed to connect to the database.")

if __name__ == "__main__":
    import uvicorn
    # Run FastAPI app
    uvicorn.run(app, host="127.0.0.1", port=8000)