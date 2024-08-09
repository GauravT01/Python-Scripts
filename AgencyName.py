from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time

# Your credentials and domain details
username = 'your_username'
password = 'your_password'
domain_site_id = 'domainsiteid'
domain_name = 'domainname'
domain_tld = 'domaintld'

# Initialize the WebDriver
driver = webdriver.Chrome()

try:
    # Go to the login page
    driver.get('https://sem.access.mn/WC3WebsiteConfig/editConfig4?siteId={}&mode=edit&v=4&siteName={}&tld={}'.format(domain_site_id, domain_name, domain_tld))
    
    # Log in (modify the locators as needed)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'username'))).send_keys(username)
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'password'))).send_keys(password)
    driver.find_element(By.ID, 'login_button').click()
    
    # Wait for the page to load
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'agency_name_dropdown')))

    # Select 'Advertising Tech' from the dropdown
    select = Select(driver.find_element(By.ID, 'agency_name_dropdown'))
    select.select_by_visible_text('Advertising Tech')
    
    # Save the configuration
    driver.find_element(By.ID, 'save_button').click()
    
    # Push to stage
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, 'push_to_stage_button'))).click()

    print("Configuration updated and pushed to stage successfully.")
    
finally:
    # Close the browser
    time.sleep(3)
    driver.quit()
