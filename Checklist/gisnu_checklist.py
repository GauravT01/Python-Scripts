import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urlparse
import os

def fetch_html(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def get_conversion_data(soup):
    print("Extracting conversion data...")
    conversion_data_script = soup.select_one('body > div.cmn-footer-tgt > div.conversionTracking-tgt script:nth-of-type(3)')
    if not conversion_data_script:
        print("No conversion tracking script found.")
        return None, None
    
    script_content = conversion_data_script.string
    if not script_content:
        print("No script content found.")
        return None, None
    
    send_to_index = script_content.find("'send_to': 'AW-")
    if send_to_index == -1:
        print("No 'send_to' found in script.")
        return None, None
    
    send_to = script_content[send_to_index:].split("'")[1]
    send_to_parts = send_to.replace("AW-", "").split('/')
    
    if len(send_to_parts) != 2:
        print("send_to format is incorrect. Value:", send_to_parts)
        return None, None

    conversion_id, conversion_label = send_to_parts
    print(f"Extracted conversion_id: {conversion_id}, conversion_label: {conversion_label}")
    return conversion_id, conversion_label

def get_customer_id(soup):
    print("Extracting customer id...")
    customer_data_script = soup.select_one('body > main > div.container.mcRelKwdUnit-tgt > script:nth-child(4)')
    if not customer_data_script:
        print("No customer data script found.")
        return None
    
    script_content = customer_data_script.string
    if not script_content:
        print("No customer script content found.")
        return None
    
    cid_index = script_content.find('"cid":"')
    if cid_index == -1:
        print("No 'cid' found in script.")
        return None
    
    cid = script_content[cid_index:].split('"')[2]
    print(f"Extracted customer_id: {cid}")
    return cid

def get_keyword_block_url(soup):
    print("Extracting keyword block URL...")
    keyword_url_tag = soup.select_one("body > main > div.container.mcRelKwdUnit-tgt > div > div > div.sm_kwd_blk-1.mc_kwd_blk > ul > li:nth-child(1) > div > a")
    if not keyword_url_tag:
        print("No keyword block URL found.")
        return None
    
    url = keyword_url_tag['href']
    print(f"Extracted keyword block URL: {url}")
    return url

def validate_url(url, country):
    accepted_domains = {
        'US': 'https://search.yahoo.com/yhs/',
        'UK': 'https://uk.search.yahoo.com/yhs/'
    }
    
    parsed_url = urlparse(url)
    query_params = dict([param.split('=') for param in parsed_url.query.split('&')])
    
    if not url.startswith(accepted_domains[country]):
        print(f"URL does not start with {accepted_domains[country]}")
        return False
    
    required_params = ['hspart', 'hsimp', 'type', 'param2', 'param3']
    if not all(param in query_params for param in required_params):
        print(f"Missing required parameters in URL: {required_params}")
        return False
    
    if not query_params['type'].startswith('smchn_'):
        print(f"type parameter does not start with 'smchn_'")
        return False
    
    if not query_params['param2'].startswith('ccs_1_1__pi'):
        print(f"param2 does not start with 'ccs_1_1__pi'")
        return False
    
    if not query_params['param3'].startswith('ccs'):
        print(f"param3 does not start with 'ccs'")
        return False
    
    return True

def main():
    input_excel_path = r'/mnt/c/Users/tiwari.g/OneDrive - MEDIA.NET SOFTWARE SERVICES (INDIA) PRIVATE LIMITED/Documents/Python for automate/Checklist/ginsu_checklist_details.xlsx'
    output_csv_path = r'/mnt/c/Users/tiwari.g/Downloads/ginsu_checklist_result.csv'
    
    # Check if input Excel file exists
    if not os.path.exists(input_excel_path):
        print(f"File not found: {input_excel_path}")
        return
    else:
        print(f"File found: {input_excel_path}")
    
    # Read the input Excel file
    try:
        data = pd.read_excel(input_excel_path)
        print(f"Columns in the Excel file: {data.columns.tolist()}")  # Debugging line to show columns
    except Exception as e:
        print(f"Error reading {input_excel_path}: {e}")
        return

    # Trim column names in case of extra spaces
    data.columns = data.columns.str.strip()

    results = []
    for index, row in data.iterrows():
        account_id = row['account_id']
        account_name = row['account_name']
        customer_id_expected = row['customer_id']
        conversion_id_expected = row['conversion_id']
        conversion_label_expected = row['conversion_label']
        country = row['Country']
        checklist_url = row['Checklist_URL']

        print(f"Processing URL: {checklist_url}")

        html_content = None
        soup = None

        try:
            html_content = fetch_html(checklist_url)
            soup = BeautifulSoup(html_content, 'html.parser')
        except Exception as e:
            print(f"Error fetching URL {checklist_url}: {e}")
            continue

        conversion_id, conversion_label = get_conversion_data(soup)
        customer_id = get_customer_id(soup)
        keyword_block_url = get_keyword_block_url(soup)

        # Initialize matched validation checks
        customer_id_matched = 'validated' if customer_id == customer_id_expected else 'not matched'
        conversion_id_matched = 'validated' if conversion_id == conversion_id_expected else 'not matched'
        conversion_label_matched = 'validated' if conversion_label == conversion_label_expected else 'not matched'

        # Print mismatches if any
        if customer_id_matched != 'validated':
            print(f"Customer ID mismatch for {checklist_url}")
            print(f"Expected: {customer_id_expected}, Found: {customer_id}")

        if conversion_id_matched != 'validated':
            print(f"Conversion ID mismatch for {checklist_url}")
            print(f"Expected: {conversion_id_expected}, Found: {conversion_id}")

        if conversion_label_matched != 'validated':
            print(f"Conversion Label mismatch for {checklist_url}")
            print(f"Expected: {conversion_label_expected}, Found: {conversion_label}")

        # Validate the redirected URL of the keyword block
        if keyword_block_url:
            redirected_url = None
            try:
                redirected_html = fetch_html(keyword_block_url)
                redirected_url = redirected_html  # This is a placeholder, modify if fetching the actual URL differently
                if not validate_url(redirected_url, country):
                    print(f"Redirected URL validation failed for {checklist_url}")
            except Exception as e:
                print(f"Error fetching keyword block URL {keyword_block_url}: {e}")

        results.append({
            'account_id': account_id,
            'account_name': account_name,
            'customer_id': customer_id,
            'conversion_id': conversion_id,
            'conversion_label': conversion_label,
            'Country': country,
            'Checklist_URL': checklist_url,
            'customer_id_matched': customer_id_matched,
            'conversion_id_matched': conversion_id_matched,
            'conversion_label_matched': conversion_label_matched,
            'Validation': 'Passed' if all([customer_id_matched == 'validated', conversion_id_matched == 'validated', conversion_label_matched == 'validated']) else 'Failed'
        })

    results_df = pd.DataFrame(results)
    print(f"Writing {len(results)} results to {output_csv_path}")  # Debugging statement for result length
    try:
        results_df.to_csv(output_csv_path, index=False)
    except Exception as e:
        print(f"Error writing to {output_csv_path}: {e}")

if __name__ == "__main__":
    main()