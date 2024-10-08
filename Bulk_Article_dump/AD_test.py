from fastapi import FastAPI, HTTPException, Header, Depends, Query
from fastapi.responses import StreamingResponse
from pydantic import constr
from databases import Database
import csv
import io
import asyncio
from mysql.connector import Error

app = FastAPI()

# Predefined static token for access
PREDEFINED_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzUxMiIsImtpZCI6IjJzV3FHZXFiNk1zbldheUFiZmRZd2pxRmxNUDRuNUdPTVhmYnE2Tl9EX1kiLCJqd2siOiJ7XCJrZXlzXCI6W3tcImt0eVwiOlwiUlNBXCIsXCJ1c2VcIjpcInNpZ1wiLFwiYWxnXCI6XCJSUzUxMlwiLFwia2lkXCI6XCIyc1dxR2VxYjZNc25XYXlBYmZkWXdqcUZsTVA0bjVHT01YZmJxNk5fRF9ZXCIsXCJuXCI6XCJ4ektsem9MN1R4QUNsdWUwZkZGTDJrWXNvU045OUx3Y0ZDa2djU0dDemstRUZ4eTZnX1NBNGZzSUNyYTV0WnNzN3NaNE5CSWlsbXhRMGk0VnNpeHJiRS01UnhHSmNZV0ZacGVMOXZUTl9NTU54dnA3Zk5jSGd2Z0ljZmNCZkJMZlE5TlEwUjhablJHb1ByV2ZBelFZWExpWlRmcEg3ZTJvZEF6RENzcVlMSFU5R04zbFFVQ29sUk1sTGt2U3lLUVBaMVllclBacVdUSGhXX0Q4bEhNZ3NydUQyRXZ2RlQyeWV0Ul9HcGZqV3dWbGFuSVhqMmdyRWlYei1MMHNtRHhEMHRPanh4Q3FQbjJTQkljanhBbXljeG5mTTN6aUhFYVNIMWIzT1FZZ3Z5STJnQ3RZWHlZMWl6RkcwMDE5WGhucmxkLVhYUklqN0Q0R2NxYnBWRXdxb2xmWDd0Z0pPXzVQR2pxak92RW8zMmE4Y1Bvd3NqdElqbHpNd3M3V3dyb09ITUNMX1ZCVnpsUUxuZnZJYmFCdnpxbFdzZ1RrRkxMR0x3MGNhenBhX0FNaTJaSEZCLVpIVXVSbXhMY1dyUWNaWFlJUFJlc0dpZ2xOdXBMRlIyYk0xczNWdmlLSnBnckVTNnk0RkI0S3poZTQ1UlIzS3BhRV93NG1veUNmS3V4ajlnak90S2FnNDhqbUtLVm1lQkJfa3VNRUtRYzhlNGFMMHdqQXE2ckxyV2pjWXlwRkRXdkN3dDNhTnRLU25QZWEteVF4S1owVGZES1V1TEJJNDRKLWVWeVE4MnFaTzBHRFcxLUNmNWNkRzZRdEEwdnQ4Nl9DbDNPY2VJWjVJMnRmTjBRZ2pDdkwwOHR0Q2FNVXJuU1hIZUVxMllDbW9nbkc1c3Izd0poSHZRMFwiLFwiZVwiOlwiQVFBQlwifV19In0.eyJzdWIiOiIyMDEwMCIsImRhdGEiOiJ7XCJpZFwiOjIwMTAwLFwiZmlyc3RfbmFtZVwiOlwidGl3YXJpLmdcIixcImxhc3RfbmFtZVwiOlwiZ1wiLFwidXNlcl9uYW1lXCI6XCJ0aXdhcmkuZ1wiLFwiZW1haWxfYWRkcmVzc1wiOlwidGl3YXJpLmdAbWVkaWEubmV0XCIsXCJ1c2VyX3R5cGVfaWRcIjpcIjFcIixcImdyb3VwX2lkXCI6XCIxXCIsXCJpc19zdXBlcl9hZG1pblwiOlwiMFwiLFwibWFuYWdlbWVudF9ncm91cFwiOlwiVGVjaFwifSIsImlzcyI6Imh0dHA6Ly91c2VyLWF1dGguZ2l6bW8uYXBwLnNlbS5pbmZyYSIsImF1ZCI6Iiouc2VtLnJlcG9ydHMubW4iLCJpYXQiOjE3MjU2MTA4MzYuMjczMDIsIm5iZiI6MTcyNTYxMDgzNi4yNzMwMiwiZXhwIjoxNzI1Njk3MjM2LjI3MzAyfQ.CPImaEJFdZv4lM-WJUvgi63EMLCemderBsO8Aaa_pbwRzhKJD6iqOITOk_VCMXDLpgtC8V7V8XRKqvk__dMFfjyp2AahcRXJCSzkzuVoGiZ5Ztqq9NPQRC9xwGpFc6OV3An70gp_xJjszXbesYNKlUwO4k3sm7BvOgLHrgtyXT77FgVF4hyX8exvMx06abq63KTYo-BkHkPzYFq6NsB7J04JFmA4EfTTUPxz1bQ7e1Tm5rufewlsG_KbsinysKjHYIws_f9H247OCOL-uhVdG3ai0nFDLWT6s68uyD774w2ME7-w-eRwVVJPbg2K39owMXoksWPN4KyKxauB9ObGyze6Mlg0TASyqvGKff7eJZ1uEjuzESXTiZfPlxrqBKoALGr9LL5liJWWnvJzJr-4BWLkjXQHzEATP1gGknMqB8gQZMhhSZV9vauBdfVbhr9N0HEhS65waMksEUdrvfRN9G4_zeV-0dNsASpp1-XQ349bh6c0jVs4bnbHi_MJNTRi72pRAnuYxA3b7VXAy-ZVh5na1KiOiT6ocPkINV7AxVUh3J_G2F2m7BZBN6bgI85Qkp9FsTYZvqpYwigbYWD_sU4CSrYZUkZrITsgMR1tT0Xt9Ivz9GyZKrlt9ibAdSjGS32CgEACANFaaWPHL3Clvf2U6T_g9-7Jbc9bqPAZ8u4re"

# Simple Token-based check
async def validate_token(authorization: str = Header(...)):
    if authorization.split(" ")[1] != PREDEFINED_TOKEN:
        raise HTTPException(
            status_code=401,
            detail="Invalid or missing token"
        )

# Asynchronous database connection creation using Databases library
async def async_create_connection(db_name: str):
    try:
        database_url = (
            f"mysql+aiomysql://sem_wordpress_sites_ro:"
            f"8HMATYXt0K1J3FdeuG7NR6Dx4lkgt4ei"
            f"@sem-prod-cloud-sql-proxy.service.gcp-us-west-1.consul:3308/{db_name}?charset=utf8mb4"
        )
        database = Database(database_url)
        await database.connect()
        print(f"Async connection to {db_name} database successful")
        return database
    except Error as e:
        print(f"The error '{e}' occurred")
        return None

# Fetch data asynchronously from the database
async def async_fetch_data(database, domain_name: str):
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
    '''
    try:
        rows = await database.fetch_all(query=query)
        column_names = [desc[0] for desc in rows[0].cursor.description]
        return rows, column_names
    except Error as err:
        raise HTTPException(status_code=500, detail="Database query failed")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Generate the CSV in chunks for large datasets (using async generator)
async def generate_csv_chunks(result_dicts, column_names):
    output = io.StringIO()
    csv_writer = csv.writer(output)
    csv_writer.writerow(column_names)
    yield output.getvalue()

    for row_dict in result_dicts:
        output = io.StringIO()
        csv_writer.writerow([str(value) if value is not None else '' for value in row_dict.values()])
        await asyncio.sleep(0.01)  # Simulate the chunked process
        output.seek(0)  # Reset pointer to the start of the stream
        yield output.getvalue()

# Main endpoint to dump articles while ensuring authentication and secure access.
@app.post("/articledump/")
async def articledump(
    domain: constr(max_length=255) = Query(..., description="Enter the domain name you want to connect to."),
    tld: str = Query(..., pattern=r"^[a-z]{2,3}$", description="Enter the TLD of your domain name."),
    _: str = Depends(validate_token)  # Validate token access
):
    db_name = f"{domain}_{tld}"
    domain_name = f"{domain}.{tld}"

    database = await async_create_connection(db_name)
    if database:
        try:
            # Fetch data asynchronously
            result, column_names = await async_fetch_data(database, domain_name)
            result_dicts = [dict(zip(column_names, row)) for row in result]

            # Stream response as CSV file
            csv_file_generator = generate_csv_chunks(result_dicts, column_names)
            response = StreamingResponse(csv_file_generator, media_type="text/csv")
            filename = f"{domain_name}_articledump.csv"
            response.headers["Content-Disposition"] = f"attachment; filename={filename}"
            return response

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        finally:
            await database.disconnect()  # Close async database connection
            print("Async MySQL connection is closed")

    else:
        raise HTTPException(status_code=500, detail="Cannot connect to the database.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)