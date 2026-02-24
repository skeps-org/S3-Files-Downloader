import streamlit as st
import boto3
import os
import pandas as pd
import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import getpass


def read_config(config_file="product_configs.xlsx"):
    """Reads product-to-account mapping from an Excel file."""
    config_df = pd.read_excel(config_file)
    return config_df.set_index('Product').to_dict('index')


def fetch_credentials_via_selenium(account_id):
    """Fetches AWS credentials dynamically using Selenium and Chrome SSO."""
    username = getpass.getuser()
    profile_path = fr'D:\Users\{username}\AppData\Local\Google\Chrome\User Data\Default'
    chrome_path = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
    options = Options()
    options.add_argument("--start-maximized")
    options.add_argument(f"--user-data-dir={profile_path}")

    # Disable the sandboxing feature that can cause this issue
    options.add_argument('--no-sandbox')

    # Disable GPU hardware acceleration (sometimes helps with the crash)
    options.add_argument('--disable-gpu')

    options.add_argument('--disable-dev-shm-usage')

    # Disable the DevToolsActivePort file issue
    # options.add_argument('--remote-debugging-port=9222')

    # options.add_argument('--headless')

    options.binary_location = chrome_path
    options.add_argument("--restart")
    options.add_argument("--flag-switches-begin")
    options.add_argument("--flag-switches-end")

    driver = webdriver.Chrome(options=options)
    try:
        driver.get("https://d-90670e2182.awsapps.com/start/#/?tab=accounts")  

        # Locate the account list div
        wait = WebDriverWait(driver, 60)

        account_list_div = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[data-testid='account-list']")))

        # Find all account buttons
        account_buttons = account_list_div.find_elements(By.CSS_SELECTOR, ".tkbnebnefszuGESxQTeA")

        for button in account_buttons:
            try:
                # Find the div containing account information
                account_info_div = button.find_element(By.CSS_SELECTOR, ".awsui_child_18582_whr0e_149:nth-of-type(2)")
                # changing lbexh to whr0e on 20250515
                account_p = account_info_div.find_element(By.CSS_SELECTOR, "p.awsui_color-text-body-secondary_18wu0_3h5y5_316")
                # changing 1yxfb to fxrr2 on 20250515
                # changing fxrr2_302 to khxlc_316 on 20251120
                # changing khxlc to 3h5y5 on 20260121
                account_text = account_p.find_element(By.CSS_SELECTOR, ".awsui_child_18582_whr0e_149:nth-of-type(1)").text
                # changing finding by div tag to css class 

                # Extract account ID and name
                account_id_in_button = account_text.strip()

                if account_id_in_button == account_id:
                    button.click()

                    # Find and click the access keys button
                    access_keys_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "a[data-analytics='accounts-list-item-credential-modal-button']")))
                    access_keys_button.click()

                    # Locate input fields for access keys and secret keys
                    time.sleep(3)
                    keys_to_copy = driver.find_elements(By.CSS_SELECTOR, "input.awsui_input_2rhyz_3fiyi_149.awsui_input-readonly_2rhyz_3fiyi_203")
                    # changing 6kb1z to 7gdci on 20250515
                    # changed 7gdci to 8c1nk on 20251006
                    # changed 8c1nk to 1dhxm and 196 to 203 on 20251120
                    # changed 1dhxm to mfjkh on 20260121
                    # changed mfjkh to 3fiyi on 20260224

                    access_key = keys_to_copy[2].get_attribute("value")
                    secret_key = keys_to_copy[3].get_attribute("value")
                    session_token = keys_to_copy[4].get_attribute("value")
                    driver.quit()
                    break

            except Exception as e:
                continue  # Skip non-matching buttons

    except Exception as e:
        print(e)  
    return access_key, secret_key, session_token

def list_s3_files(bucket_name, folder_path, s3_client):
    """Lists all files in a specific S3 folder."""
    files_list = []
    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_path)
    for page in page_iterator:
        files = page.get('Contents')
        for file in files:
            files_list.append(file.get('Key')) 
    return files_list


def filter_files_by_date(files, start_date, end_date, product, s3_path_given):
    """Filters files by date range based on their names and ensures they are .txt files with 'transaction' in the name."""
    print("Inside date filter function")
    filtered_files = []
    for file in files:
        try:
            file_name = os.path.basename(file) 
            if product.split()[0] in ['FNBO', 'CP']: 
                date_str = file_name.split('.')[0][-8:]  # Extract date part
                file_date = datetime.strptime(date_str, '%Y%m%d').date()
            elif product.split()[0] == 'NF':
                date_str = file_name.split(".")[0][-10:]
                file_date = datetime.strptime(date_str, '%Y_%m_%d').date()
            if start_date <= file_date <= end_date:
                if s3_path_given:
                    filtered_files.append(file)
                    # print(file)
                else:
                    if "transaction" in file_name:
                        filtered_files.append(file)
                        # print(file)
        except ValueError:
            continue  # Skip files that don't match the date format
    return filtered_files

def filter_files_by_criteria(files, matching_text):
    print("Inside criteria match function")
    print(matching_text)
    print(len(files))
    filtered_files = []
    for file in files:
        if matching_text.strip() in os.path.basename(file):
            filtered_files.append(file)
            # print(file)
    return filtered_files

def filter_files_by_exact_matches(files, exact_names):
    print("Inside exact Match Function")
    filtered_files = []
    file_list = [name.strip() for name in exact_names.split(",")]
    print(file_list)
    for file in files:
        # print(file, " name is checked")
        file_name = os.path.basename(file)
        if (file_name.split(".")[0] in file_list) or (file_name in file_list):
            # print(file, " is being appended")
            filtered_files.append(file)
    return filtered_files

def filter_files_by_file_type(files, file_types_selected):
    if len(file_types_selected):
        filtered_files = [f for f in files if f.split(".")[-1] in file_types_selected]
        return filtered_files
    return files


def download_files(files, bucket_name, local_folder, s3_client):
    """Downloads specified files from S3 to a local folder."""
    print("Inside Downloads")
    if not os.path.exists(local_folder):
        os.makedirs(local_folder, exist_ok=True)
    for file in files:
        local_file_path = os.path.join(local_folder, os.path.basename(file))
        s3_client.download_file(bucket_name, file, local_file_path)
        st.write(f"Downloaded: {file}")

# Main Streamlit App
st.title("S3 File Downloader")

# Read configuration file for product mapping
config = read_config()

# Select product
product = st.selectbox("Select a Product:", list(config.keys()))
s3_download_folder = st.text_input("Enter the s3 folder path (leave blank for transaction folder)")
user_folder = st.text_input("Enter the local download folder path (leave blank to create a folder with latest date and time)")

file_type_options = ["txt", "json", "abc"]
file_types = st.segmented_control("Type of file to download", file_type_options, selection_mode="multi", help="leave blank for all file types")


# Select Criterias
row1col1, row1col2 = st.columns(2)
with row1col1:
    daterange_criteria = st.checkbox ("Date Range", help="Must contain date in filename", value=False)
with row1col2:
    partial_filename_criteria = st.checkbox ("Search Criteria", help="Must match filename partially", value=False)

row2col1, row2col2 = st.columns(2)
with row2col1:
    exact_filename_criteria = st.checkbox ("File Names", help="Exact name matches with or without extension", value=False)
with row2col2:
    all_files_criteria = st.checkbox ("All Files", help="All files inside that folder", value=False)

start_date = end_date = matching_text = exact_names = None
# Conditional display of input fields based on the selected criteria
if daterange_criteria:
    start_date = st.date_input("Start Date", min_value=datetime(2024, 2, 10))
    end_date = st.date_input("End Date")

if partial_filename_criteria:
    matching_text = st.text_input("Enter Search Criteria")

if exact_filename_criteria:
    exact_names = st.text_input("Enter Comma Separated File Names")

if all_files_criteria:
    # For 'All Files' criteria, do not display date, search, or name input
    print("Download All Files")

# Form for submitting and downloading files
start_btn = st.button("Download Files")

if start_btn:
    # Fetch and process the form data
    if product:
        account_details = config[product]
        account_id = account_details['AccountId'].replace('"', "")
        
        if s3_download_folder:
            folder_path = s3_download_folder.split("/", 3)[-1]
            bucket_name = s3_download_folder.split("/", 3)[-2]
            s3_path_given = True
        else:
            folder_path = account_details['FolderPath']
            bucket_name = account_details['BucketName']
            s3_path_given = False

        # Fetch AWS credentials dynamically
        st.write("Fetching credentials...")
        try:
            aws_access_key_id, aws_secret_access_key, session_token = fetch_credentials_via_selenium(account_id)
            st.success("Credentials fetched successfully.")
        except Exception as e:
            st.error(f"Failed to fetch credentials: {e}")
            st.stop()

        try:
            st.write("Connecting to s3 client...")
            # Initialize S3 client
            s3_client = boto3.client('s3',
                                    aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key,
                                    aws_session_token=session_token)
            
            # List files in S3 folder
            all_files = list_s3_files(bucket_name, folder_path, s3_client)

            st.write(f"{len(all_files)} Found in Bucket")

            st.write("Filtering required files...")

            filtered_files_all = []
            
            # Filter files based on criteria
            if daterange_criteria:
                print("Inside daterange_criteria")
                filtered_files_all.append(filter_files_by_date(all_files, start_date, end_date, product, s3_path_given))

            if partial_filename_criteria:
                print("Inside partial_filename_criteria")
                filtered_files_all.append(filter_files_by_criteria(all_files, matching_text))

            if exact_filename_criteria:
                print("Inside exact_filename_criteria")
                filtered_files_all.append(filter_files_by_exact_matches(all_files, exact_names))
                
            if all_files_criteria:
                print("Inside all_files_criteria")
                filtered_files_all.append(all_files[1:])
        

            filtered_files = list(set.intersection(*map(set, filtered_files_all)))

            filtered_files = filter_files_by_file_type(filtered_files, file_types)

            print("Filtered Files Distinct:", filtered_files)

            if user_folder.strip():
                download_folder = user_folder
            else:
                download_folder = product.split()[0]+"_"+datetime.now().strftime("%Y%m%d_%H%M%S")
                os.makedirs(download_folder, exist_ok=True)

            if filtered_files:
                st.write(f"Found {len(filtered_files)} files. Downloading...")
                download_files(filtered_files, bucket_name, download_folder, s3_client)
                st.success("Download completed.")
            else:
                st.warning("No files found for the selected criteria.")

        except Exception as e:
            st.error(f"An error occurred: {e}")