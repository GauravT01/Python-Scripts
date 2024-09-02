import dns.resolver
import pandas as pd

# Define paths
input_file = r'/mnt/c/Users/tiwari.g/OneDrive - MEDIA.NET SOFTWARE SERVICES (INDIA) PRIVATE LIMITED/Documents/Python for automate/NS_record/ns_records.xlsx'
output_file = r'/mnt/c/Users/tiwari.g/Downloads/ns_records_output.xlsx'

# Read the Excel file
df = pd.read_excel(input_file)

# Create a list to store the NS records
ns_records_list = []

# Iterate over each domain name and fetch the NS records
for domain in df['domain_name']:
    try:
        result = dns.resolver.resolve(domain, 'NS')
        ns_records = [str(ns) for ns in result]
        ns_records_list.append(", ".join(ns_records))
    except dns.resolver.NoAnswer:
        ns_records_list.append("No NS record found")
    except Exception as e:
        ns_records_list.append(f"Error occurred - {e}")

# Add the NS records as a new column to the DataFrame
df['ns_records'] = ns_records_list

# Write the updated DataFrame back to Excel
df.to_excel(output_file, index=False)

print(f"NS records have been written to {output_file}")