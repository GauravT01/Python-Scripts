import requests
import re
from bs4 import BeautifulSoup

# Function to get the source code of a webpage
def get_source_code(url):
    try:
        # Send a GET request to the URL
        response = requests.get(url)

        # Check if the request succeeded (status code 200)
        if response.status_code == 200:
            return response.text  # Return the HTML content as a string
        else:
            print(f"Failed to retrieve the page. Status code: {response.status_code}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

# Function to extract 'send_to' value from a <script> block with gtag_report_conversion function
def get_conversion_value(source_code):
    # Parse the HTML content using BeautifulSoup for easier extraction
    soup = BeautifulSoup(source_code, 'html.parser')

    # Find all <script> tags
    script_tags = soup.find_all('script')

    # Define the regular expression to match the 'send_to' value in the gtag_report_conversion function
    send_to_pattern = r"send_to\s*:\s*['\"](AW-[0-9]+/[a-zA-Z0-9_-]+)['\"]"

    # Go through each <script> tag and search for the send_to pattern
    for script in script_tags:
        if script.string:  # Ensure the script tag contains text
            match = re.search(send_to_pattern, script.string, re.DOTALL)
            if match:
                send_to_value = match.group(1)
                conversions_value = send_to_value.replace("AW-", "")  # Remove 'AW-' to get the desired value
                print(f"conversions = {conversions_value}")
                return  # Stop once found

    print("Conversion data not found")

# Main function to tie it all together
def main():
    # URL you want the source code from
    url = "https://swiftresults.net/topic/818/85L6237/?utm_campaign=85L6237&ekw=wHKjrujRd0GF0EDGnc69gZj4QUMwoeB1wFHCs19yumM="

    # Step 1: Get the source code of the webpage
    source_code = get_source_code(url)

    if source_code:
        # Step 2: Extract 'send_to' value (conversions)
        get_conversion_value(source_code)

# Run the main function
if __name__ == "__main__":
    main()