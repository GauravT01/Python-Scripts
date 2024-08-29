# import csv
import mysql.connector
from mysql.connector import Error
import pandas as pd
# import os
# from tqdm import tqdm

def create_connection():
    try:
        connection = mysql.connector.connect(
            host="sem-prod-cloud-sql-proxy.service.gcp-us-west-1.consul",
            user="sem_gizmo_api_ro",
            password="v95MbUkEKuFbWYN9PVB9MFFLMxFsayAx",
            database="wordpress_info",
            charset='utf8mb4',
            collation='utf8mb4_unicode_ci'
        )
        print(f"Connection to wordpress_info database successful")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred")
        return None
    
def main(database):
    domain = input("Enter Domain name without TLD : ")

    connection = create_connection(database)

    if connection:
        try:
            # Create a cursor object
            cursor = connection.cursor()
            # SQL query to fetch the desired data
            query = f'''
            select site_name,json_config from wordpress_info.credentials 
            where site_name like '%{domain}%'order by id desc limit 1;
            ''' # wp_post.post_content removed from query

            # Execute the SQL query
            cursor.execute(query)

            # Fetch all the rows
            result = cursor.fetchall()
            column_names = cursor.column_names
            print("result")
        except Error as err:
            print(f"Error: {err}")