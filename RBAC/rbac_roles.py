import pandas as pd
import requests
import os
import csv

# Constants
INPUT_EXCEL = r'/mnt/c/Users/tiwari.g/OneDrive - MEDIA.NET SOFTWARE SERVICES (INDIA) PRIVATE LIMITED/Documents/Python for automate/RBAC/rbac_users.xlsx'
SHEET_NAME = 'Sheet1'
OUTPUT_CSV_DIR = r'/mnt/c/Users/tiwari.g/Downloads'
OUTPUT_CSV = os.path.join(OUTPUT_CSV_DIR, 'output_roles.csv')
API_URL = 'http://sem-prod-rbac-api.service.gcp-us-west-1.consul/api/users/{sub}/roles'
TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzUxMiIsImtpZCI6IjJzV3FHZXFiNk1zbldheUFiZmRZd2pxRmxNUDRuNUdPTVhmYnE2Tl9EX1kiLCJqd2siOiJ7XCJrZXlzXCI6W3tcImt0eVwiOlwiUlNBXCIsXCJ1c2VcIjpcInNpZ1wiLFwiYWxnXCI6XCJSUzUxMlwiLFwia2lkXCI6XCIyc1dxR2VxYjZNc25XYXlBYmZkWXdqcUZsTVA0bjVHT01YZmJxNk5fRF9ZXCIsXCJuXCI6XCJ4ektsem9MN1R4QUNsdWUwZkZGTDJrWXNvU045OUx3Y0ZDa2djU0dDemstRUZ4eTZnX1NBNGZzSUNyYTV0WnNzN3NaNE5CSWlsbXhRMGk0VnNpeHJiRS01UnhHSmNZV0ZacGVMOXZUTl9NTU54dnA3Zk5jSGd2Z0ljZmNCZkJMZlE5TlEwUjhablJHb1ByV2ZBelFZWExpWlRmcEg3ZTJvZEF6RENzcVlMSFU5R04zbFFVQ29sUk1sTGt2U3lLUVBaMVllclBacVdUSGhXX0Q4bEhNZ3NydUQyRXZ2RlQyeWV0Ul9HcGZqV3dWbGFuSVhqMmdyRWlYei1MMHNtRHhEMHRPanh4Q3FQbjJTQkljanhBbXljeG5mTTN6aUhFYVNIMWIzT1FZZ3Z5STJnQ3RZWHlZMWl6RkcwMDE5WGhucmxkLVhYUklqN0Q0R2NxYnBWRXdxb2xmWDd0Z0pPXzVQR2pxak92RW8zMmE4Y1Bvd3NqdElqbHpNd3M3V3dyb09ITUNMX1ZCVnpsUUxuZnZJYmFCdnpxbFdzZ1RrRkxMR0x3MGNhenBhX0FNaTJaSEZCLVpIVXVSbXhMY1dyUWNaWFlJUFJlc0dpZ2xOdXBMRlIyYk0xczNWdmlLSnBnckVTNnk0RkI0S3poZTQ1UlIzS3BhRV93NG1veUNmS3V4ajlnak90S2FnNDhqbUtLVm1lQkJfa3VNRUtRYzhlNGFMMHdqQXE2ckxyV2pjWXlwRkRXdkN3dDNhTnRLU25QZWEteVF4S1owVGZES1V1TEJJNDRKLWVWeVE4MnFaTzBHRFcxLUNmNWNkRzZRdEEwdnQ4Nl9DbDNPY2VJWjVJMnRmTjBRZ2pDdkwwOHR0Q2FNVXJuU1hIZUVxMllDbW9nbkc1c3Izd0poSHZRMFwiLFwiZVwiOlwiQVFBQlwifV19In0.eyJzdWIiOiIyMDEwMCIsImRhdGEiOiJ7XCJpZFwiOjIwMTAwLFwiZmlyc3RfbmFtZVwiOlwidGl3YXJpLmdcIixcImxhc3RfbmFtZVwiOlwiZ1wiLFwidXNlcl9uYW1lXCI6XCJ0aXdhcmkuZ1wiLFwiZW1haWxfYWRkcmVzc1wiOlwidGl3YXJpLmdAbWVkaWEubmV0XCIsXCJ1c2VyX3R5cGVfaWRcIjpcIjFcIixcImdyb3VwX2lkXCI6XCIxXCIsXCJpc19zdXBlcl9hZG1pblwiOlwiMFwiLFwibWFuYWdlbWVudF9ncm91cFwiOlwiVGVjaFwifSIsImlzcyI6Imh0dHA6Ly91c2VyLWF1dGguZ2l6bW8uYXBwLnNlbS5pbmZyYSIsImF1ZCI6Iiouc2VtLnJlcG9ydHMubW4iLCJpYXQiOjE3MjQzMTg3MzguNTU5MTg1LCJuYmYiOjE3MjQzMTg3MzguNTU5MTg1LCJleHAiOjE3MjQ0MDUxMzguNTU5MTg1fQ.Xd3ofS5pLhO3YZCdSTGnLEgVCpjR6orL2BTA5EQ7xoOgalUs3K-IG014La4UCrjC_az4yoYo_L3ieH_Wj_NhOZcdtJHzTr-TX5121ZMPxPNXTl4R0GlfF1xNmIzTsRu_tjk3zo9dKFS1tA59hI3hebt3fJVaA5WucNJb9k-UUpLbqQzqxHQlJ5Q4VpdI7hnAWS78xw4v-mhBtN-Vmccf84X268r5qNEQNubU7E122qpxIbHShN9DjPUQA04Q_mRI7NozenD6400c1oCu-I8O-0ydLQK6D734dZBQW3xlzol65gm5lKZaHHe3wpBH3d09SxKCqf5-oZx340YPbJi4HNpwSWNgbmsruQTQRotUKIKBeZrABG_yPDoc0Lcq2gBynTFMyJeifoJfT5K4bBiPxiv5Ukx4SjzIrC1aPcZoZ_W-cAOVlOiB3Ao3Uca8CkCKU-GSbZkpLm_8Dzk2S4-m7J3FKaTeFZooHk5vdvTkfTd7j9XWMJ951mvmTcCUADiOBG7MHXJaMw1udv3DV7j59IBwyh8crECDProDW7buFIikguljm5lNu5qWDXT-UIuHcpjzkrjuyciiK82hFQD_N20x84sfg-I7cq8CqzKITWZ-JODMk1iRjJOauBqvJ0y8DuSr3_vBJ_wKdTAB11jbWHP0xh4E1fg6A24R3_XXM50'

def get_user_roles(email):
    """Get roles for a given user email."""
    url = API_URL.format(sub=email)
    headers = {
        'Authorization': f'Bearer {TOKEN}'
    }
    response = requests.get(url, headers=headers)
    # Debug: Print URL and response status
    print(f"Requesting roles for {email} from {url}")
    print(f"Response status code: {response.status_code}")
    if response.status_code == 200:
        # Debug: Print the response content
        print(f"Response content: {response.json()}")
        return response.json()  # Assume the response JSON is a list of roles
    else:
        # Debug: Print error message
        print(f"Failed to retrieve roles for {email}: {response.text}")
        return []  # Return an empty list if something went wrong

def read_emails_from_excel(file_path, sheet_name):
    """Read user emails from an Excel file."""
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    emails = df['user_email'].tolist()  # Assuming the column is named 'user_email'
    return emails

def write_roles_to_csv(file_path, data):
    """Write user roles to a CSV file."""
    with open(file_path, mode='w', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Email', 'Roles'])  # Writing the header
        for email, roles in data:
            writer.writerow([email, ', '.join(roles)])

def print_sheet_names(file_path):
    """Print sheet names in the Excel file for debugging."""
    xls = pd.ExcelFile(file_path)
    print("Sheet names:", xls.sheet_names)

def main():
    # Print available sheet names for debugging
    print_sheet_names(INPUT_EXCEL)
    
    # Read emails from the input Excel file
    emails = read_emails_from_excel(INPUT_EXCEL, SHEET_NAME)
    
    # Prepare a list to hold the results
    results = []
    
    # Fetch roles for each email
    for email in emails:
        roles = get_user_roles(email)
        results.append((email, roles))
    
    # Write the results to the output CSV
    write_roles_to_csv(OUTPUT_CSV, results)
    print(f"Results written to {OUTPUT_CSV}")

if __name__ == '__main__':
    main()