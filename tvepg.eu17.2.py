# use selenium browser to bypass cloudflare
# unnecessary functions and code for normal proxies /requests must be removed
# use gemini to redisgn v17 with 2 soft retries and 5 hard retries (browser cycles)
# use seperate gemini to do parent child retry for ip bans
# major change to main loop by AI

import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import xml.dom.minidom
import urllib3
import re
import urllib.parse
import time, random, sys

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import undetected_chromedriver as uc
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException
from bs4 import BeautifulSoup

class DriverRecycleError(Exception):
    """Custom exception to signal that the WebDriver needs to be closed and restarted."""
    pass

class FatalScrapeError(Exception):
    """Custom exception to signal the main loop to terminate the entire script (e.g., due to IP ban)."""
    pass


start_time = datetime.now()
start_timestamp = start_time.strftime('%Y-%m-%d %H:%M:%S')
print(f"Program started at: {start_timestamp}")

channames = ['eurosport_1', 'eurosport_2', 'tnt_sports_1', 'tnt_sports_2', 'tnt_sports_3', 'tnt_sports_4']
channel_ids = {
    'eurosport_1': 'Eurosport 1.uk',
    'eurosport_2': 'Eurosport 2.uk',
    'tnt_sports_1': 'TNT Sports 1.uk',
    'tnt_sports_2': 'TNT Sports 2.uk',
    'tnt_sports_3': 'TNT Sports 3.uk',
    'tnt_sports_4': 'TNT Sports 4.uk'
}
channel_display_names = {
    'eurosport_1': 'UK: Eurosport 1',
    'eurosport_2': 'UK: Eurosport 2',
    'tnt_sports_1': 'UK: Tnt Sports 1',
    'tnt_sports_2': 'UK: Tnt Sports 2',
    'tnt_sports_3': 'UK: Tnt Sports 3',
    'tnt_sports_4': 'UK: Tnt Sports 4'
}

root = ET.Element('tv')
root.set('generator-info-name', 'enerdude')
root.set('generator-info-url', 'enerdude.com/')

time_offset = '-01:00'
time_offset = '+00:00' # currently - end oct to mar
hours, minutes = map(int, time_offset.split(':'))
time_offset_delta = timedelta(hours=hours, minutes=minutes)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Referer': 'https://tvepg.eu/en/united_kingdom/channel/'
}

webproxy_list = [
    'https://proxy-de.steganos.com/browse.php?u=',
    'https://proxy.tersel.cz/subdom/proxy/browse.php?u=',
    'https://www.cct.lsu.edu/sites/default/files/glype/browse.php?u=',
    'https://proxy-es.steganos.com/browse.php?u=',
    #'https://onlineproxy.eu/browse.php?u=',
    'https://proxy-us.steganos.com/browse.php?u='
]

proxy_headers_list = [
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        'Referer': 'https://proxy-de.steganos.com/'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        'Referer': 'https://proxy.tersel.cz/subdom/proxy/'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        'Referer': 'https://www.cct.lsu.edu/sites/default/files/glype/'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        'Referer': 'https://proxy-es.steganos.com/'
    },
    {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0',
        'Referer': 'https://proxy-us.steganos.com/'
    },
]

use_webproxy = False
proxy_counter = 0

# Track proxy failures
proxy_failures = [0] * len(webproxy_list)
MAX_PROXY_FAILURES = 3
REQUEST_TIMEOUT = 24  # seconds, recommended for proxies
removed_proxies = []

if use_webproxy:
    print(f'Using proxies: {webproxy_list}\n')

# Get runner ip information
def fetch_ip_data(api_key):
    url = f"https://api.ipdata.co/?api-key={api_key}"
    
    try:
        # Sending the GET request
        response = requests.get(url)
        
        # Check if the response status code is 200 (successful)
        if response.status_code == 200:
            data = response.json()
            
            # Print all fields except for 'languages' and 'currency'
            fields_to_exclude = ['languages', 'currency']
            for key, value in data.items():
                if key not in fields_to_exclude:
                    print(f"{key}: {value}")
        else:
            print("could not obtain ip data")
    except requests.exceptions.RequestException as e:
        # Handle any exceptions during the request
        print("could not obtain ip data")
        print(f"Error: {e}")

api_key = '1bcf06bab8d185f0e3a170e16bcc33ed7180651e89b97e1e7a08ddb0'


def terminate_with_banflag():
    with open('ban_flag.txt', 'w') as f:
        f.write('retry')
    sys.exit(10) # Seperate ban flag as 10 compared to normally 1

# --- Driver Setup Function ---
def setup_driver():
    """Initializes and returns the uc.Chrome driver instance with CI-specific settings."""
    options = uc.ChromeOptions()
    
    # --- Arguments for CI/Xvfb (Crucial for stability) ---
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    
    # --- FIX: Explicit Chrome Binary Path (Resolves v140/v141 mismatch) ---
    chrome_path = None
    
    if sys.platform.startswith('linux'):
        print("Using GitHub Actions/Linux Setup...")
        # Path where 'browser-actions/setup-chrome' installs the binary
        chrome_path = '/opt/hostedtoolcache/setup-chrome/chrome/stable/x64/chrome'
    else:
        print("Using Local Windows/Mac Setup...")
        
    try:
        driver = uc.Chrome(
            options=options, 
            headless=False,
            # Pass executable path only if set
            browser_executable_path=chrome_path if chrome_path else None
        )

        # This targets the 'HTTPConnectionPool: Read timed out' error.
        driver.command_executor.set_timeout(300) 
        print("Set Selenium Command Timeout to 300 seconds.")

        # Set page load timeout for network requests (already 60s)
        driver.set_page_load_timeout(60)
        
        return driver
    
    except Exception as e:
        print(f"Failed to set up Chrome driver: {e}", file=sys.stderr)
        return None

# --- Scraping Function (Uses existing driver) ---
def scrape_url(driver, url):
    """
    Navigates and scrapes a single URL, using specific exception handling 
    to force soft retries on TimeoutExceptions and detect specific 403 blocks.
    """
    
    if driver is None:
        # A simple RuntimeError is sufficient for a non-recoverable setup issue.
        # The main loop's 'except Exception' or default Python exit will handle this, 
        # resulting in a standard Exit Code 1.
        raise RuntimeError("FATAL: WebDriver instance is None. Cannot attempt URL scrape.")

    MAX_SOFT_RETRIES = 2
    
    for attempt in range(1, MAX_SOFT_RETRIES + 1):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"{timestamp} --- Attempting to load/scrape {url} (Soft Attempt {attempt}/{MAX_SOFT_RETRIES}) ---")
        
        try:
            # 1. Navigate
            driver.get(url)
            
            # 2. Wait for the content (30 seconds for key elements)
            wait = WebDriverWait(driver, 30)  
            sidebar_present = EC.presence_of_element_located((By.CLASS_NAME, 'sidebar-wrapper'))
            nav_present = EC.presence_of_element_located((By.CLASS_NAME, 'nav'))

            print("Waiting for key page elements...")
            wait.until(EC.all_of(sidebar_present, nav_present))
            
            # 3. Get source and pause (Success)
            print(f"Success: {driver.title}")
            # Use your tested delay range: random.uniform(1.0, 2.0)
            delay = random.uniform(1.0, 2.0) # change code for exit 10: 300 timeout, 63min  4:38pm // 59min 7:01pm // github enerdude2 62min 7nov 11:04 pm // 60min 8nov 02:59am // 53min 12:38pm // 52min 7:20pm // first run with correct xml
            print(f"Pausing for {delay:.2f} seconds...")
            time.sleep(delay)
            return driver.page_source

        # --- MODIFIED SPECIFIC EXCEPTION HANDLER (Timeout) ---
        except TimeoutException as e:
            # Check the page source for the hard block message
            page_source = driver.page_source if driver else ""
            
            is_security_ban = "Security issues.." in page_source
            is_too_many_requests_ban = "container contact-form hi" in page_source
            
            # -------------------------------------------------------------
            # --- CRITICAL FIX: DIRECT TERMINATION ON BAN DETECTION ---
            # -------------------------------------------------------------
            if is_security_ban or is_too_many_requests_ban:
                ban_type = "Security issues" if is_security_ban else "Too many requests"
                print(f"\n!!! FATAL IP BLOCK DETECTED ({ban_type}) !!! Script terminating immediately with Exit Code 10.", file=sys.stderr)
                
                # Crucial cleanup before forced exit
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                
                # FORCED EXIT (Guarantees Exit Code 10)
                terminate_with_banflag() 
                # Execution stops here.
                
            # -------------------------------------------------------------

            # If NO ban was found, continue with soft retries
            if attempt < MAX_SOFT_RETRIES:
                print(f"Content Timeout Error: Key elements failed to appear. Soft Retrying in 5 seconds...", file=sys.stderr)
                time.sleep(5)
                continue # Go to next iteration (Attempt 2/2)
            else:
                print(f"Max soft retries failed on Content Timeout. Signaling for driver recycle.", file=sys.stderr)
                # If we exhausted soft retries on a generic timeout, we recycle the driver.
                raise DriverRecycleError(f"Driver failed unrecoverably on {url} after element timeout.")

        # --- EXISTING GENERIC EXCEPTION HANDLER (Connection/Driver Errors) ---
        except Exception as e:
            error_msg = str(e)
            
            # Identify other errors that indicate a driver/connection failure
            is_fatal_connection_error = (
                "cannot connect" in error_msg or  
                "Read timed out" in error_msg or
                "HTTPConnectionPool" in error_msg or
                "Session not started or terminated" in error_msg
            )
            
            if is_fatal_connection_error:
                if attempt < MAX_SOFT_RETRIES:
                    print(f"Connection Error: {error_msg.splitlines()[0]}. Soft Retrying in 5 seconds...", file=sys.stderr)
                    time.sleep(5)  
                else:
                    # Max soft retries failed on persistent connection issue, signal hard recycle
                    print(f"Max soft retries failed on URL (connection). Signaling for driver recycle.", file=sys.stderr)
                    raise DriverRecycleError(f"Driver failed unrecoverably on {url} after soft retries.")
            else:
                # Critical error that is neither a timeout nor a connection issue: signal recycle immediately.
                print(f"Non-retryable critical error encountered: {error_msg}. Signaling for driver recycle.", file=sys.stderr)
                raise DriverRecycleError(f"Critical error on {url}. Recyling driver.")
            
    # As a safeguard (should not be reached)
    raise DriverRecycleError(f"Scrape function finished without success or error. Recycling driver.")


def extract_url(url):
    # Match any ?u=... query parameter, regardless of the base path
    match = re.search(r'[?&]u=([^&]+)', url)
    if match:
        decoded_url = urllib.parse.unquote(match.group(1))
        if decoded_url.startswith('http'):
            return decoded_url
        else:
            return 'https://tvepg.eu' + decoded_url

    # Already a full URL
    if url.startswith('http'):
        return url

    # Relative URL
    return 'https://tvepg.eu' + url


def get_url(url):
    global proxy_counter, webproxy_list, proxy_headers_list, proxy_failures, removed_proxies
    max_attempts = 5
    attempts = 0

    while attempts < max_attempts:
        if not webproxy_list:
            print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} Error: No proxies left to use.")
            return None

        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if use_webproxy:
                proxy_index = proxy_counter % len(webproxy_list)
                proxy_counter += 1
                print(f"{timestamp} Requesting URL (Proxy {proxy_index + 1}): {url}")
                full_url = webproxy_list[proxy_index] + url + '&b=4'
                response = requests.get(
                    full_url,
                    allow_redirects=False,
                    headers=proxy_headers_list[proxy_index],
                    verify=False,
                    timeout=REQUEST_TIMEOUT
                )
            else:
                print(f"{timestamp} Requesting URL (Direct): {url}")
                response = requests.get(url, allow_redirects=False, headers=headers, verify=False, timeout=REQUEST_TIMEOUT)
                time.sleep(2)

            print(f"{timestamp} Received HTTP {response.status_code}")

            # Accept 200 OK and 500 Internal Server Error
            if response.status_code not in (200, 500):
                print(f"{timestamp} Warning: Unacceptable HTTP status {response.status_code}")
                if use_webproxy:
                    proxy_failures[proxy_index] += 1
                    print(f"{timestamp} Warning: Proxy {proxy_index + 1} failure count = {proxy_failures[proxy_index]}")
                    if proxy_failures[proxy_index] >= MAX_PROXY_FAILURES:
                        print(f"{timestamp} Removing Proxy {proxy_index + 1} after {MAX_PROXY_FAILURES} failures.")
                        removed_proxies.append(webproxy_list[proxy_index])
                        del webproxy_list[proxy_index]
                        del proxy_headers_list[proxy_index]
                        del proxy_failures[proxy_index]
                attempts += 1
                continue

            soup = BeautifulSoup(response.text, 'html.parser')

            if soup.find('div', class_='sidebar-wrapper') and soup.find('ul', class_='nav'):
                #print('yes')
                #a=input()
                return response
            else:
                print(f"{timestamp} Warning: Expected sidebar HTML not found in response")
                if use_webproxy:
                    proxy_failures[proxy_index] += 1
                    print(f"{timestamp} Warning: Proxy {proxy_index + 1} failure count = {proxy_failures[proxy_index]} (bad HTML)")
                    if proxy_failures[proxy_index] >= MAX_PROXY_FAILURES:
                        print(f"{timestamp} Removing Proxy {proxy_index + 1} after {MAX_PROXY_FAILURES} failures.")
                        removed_proxies.append(webproxy_list[proxy_index])
                        del webproxy_list[proxy_index]
                        del proxy_headers_list[proxy_index]
                        del proxy_failures[proxy_index]
                attempts += 1

        except requests.RequestException as e:
            attempts += 1
            print(f"{timestamp} Request Exception (attempt {attempts}/{max_attempts}): {str(e)}")
            if use_webproxy:
                proxy_failures[proxy_index] += 1
                print(f"{timestamp} Request failed on Proxy {proxy_index + 1} (failure {proxy_failures[proxy_index]}): {str(e)}")
                if proxy_failures[proxy_index] >= MAX_PROXY_FAILURES:
                    print(f"{timestamp} Removing Proxy {proxy_index + 1} after {MAX_PROXY_FAILURES} failures.")
                    removed_proxies.append(webproxy_list[proxy_index])
                    del webproxy_list[proxy_index]
                    del proxy_headers_list[proxy_index]
                    del proxy_failures[proxy_index]
            
    if use_webproxy:
        print(f"{timestamp} Error: All proxy attempts failed.")
    else:
        print(f"{timestamp} Error: All attempts failed.")
    return None

def process_epg_data(driver, soup, date_str, process_next_day=False):
    """
    Parses EPG data from the guide page and scrapes detail pages for image URLs.
    
    Args:
        driver: The active Selenium WebDriver instance.
        soup (BeautifulSoup): The BeautifulSoup object of the main channel page.
        date_str (str): The date string being processed (YYYY-MM-DD).
        process_next_day (bool): Flag to indicate whether to process the next day's data if encountered.
        
    Raises:
        DriverRecycleError: If the driver fails during a scrape, signaling the main loop to recover.
        FatalScrapeError: If an unrecoverable IP block is detected, signaling the main loop to terminate.
    """
    all_programmes = []
    date_text = None
    next_day_encountered = False
    
    for element in soup.find_all(['h4', 'tr']):
        if element.name == 'h4' and element.find('b'):
            date_text = element.find('b').text.strip().replace('TV guide - ', '')
            if ', ' in date_text:
                guide_date = datetime.strptime(date_text.split(', ')[1], '%d/%m/%y').strftime('%Y-%m-%d')
                if guide_date != date_str:
                    next_day_encountered = True
                    if not process_next_day:
                        break
        elif element.get('itemprop') == 'publication' and date_text and ', ' in date_text:
            try:
                guide_date = datetime.strptime(date_text.split(', ')[1], '%d/%m/%y').strftime('%Y-%m-%d')
                if guide_date == date_str or (next_day_encountered and len(all_programmes) == 0):
                    start_time = element.find('h5', itemprop='startDate').find('b').text.strip()
                    start_time = f"{date_text.split(', ')[1]} {start_time}"
                    
                    # Apply date context and offset (time_offset_delta is assumed defined)
                    start_time = datetime.strptime(start_time, '%d/%m/%y %H:%M') + time_offset_delta 
                    
                    program_name_tag = element.find('h6', itemprop='name')
                    program_name = program_name_tag.text.strip() if program_name_tag else 'Unknown'
                    program_url_tag = extract_url(program_name_tag.find('a')['href'])
                    
                    # --- ATTEMPT DETAIL SCRAPE WITH RECYCLE/FATAL HANDLING ---
                    image_url = 'Unknown'
                    description = 'Unknown'

                    try:
                        # scrape_url can now raise DriverRecycleError or FatalScrapeError
                        programme_response = scrape_url(driver, program_url_tag)
                        
                        if programme_response is None:
                            # Scrape failed after all soft retries (Non-fatal, use defaults)
                            print(f"Warning: Failed to scrape detail page for {program_name}. Skipping image/description.", file=sys.stderr)
                        else:
                            # Scrape successful. Parse image/description.
                            programme_soup = BeautifulSoup(programme_response, 'html.parser')
                            
                            image_tag = programme_soup.find('article').find('img')
                            if image_tag and 'src' in image_tag.attrs:
                                image_url = extract_url(image_tag['src'])
                            
                            description_tag = element.find('div', class_='description-text') 
                            if description_tag:
                                description = description_tag.text.strip()

                    except FatalScrapeError:
                        # CRITICAL: Re-raise the Fatal signal to be caught by the main loop for termination
                        raise
                        
                    except DriverRecycleError:
                        # CRITICAL: Reraise the signal up to the main loop for full driver restart
                        raise 

                    except Exception as scrape_e:
                        # Catch unexpected errors during parsing of the detail page
                        print(f"Error parsing detail page for {program_name}: {scrape_e}. Skipping detail.", file=sys.stderr)

                    # --- APPEND PROGRAM DATA ---
                    all_programmes.append({
                        'start_time': start_time,
                        'program_name': program_name,
                        'description': description,
                        'image_url': image_url
                    })
                    
                    if next_day_encountered:
                        break
                
            except (AttributeError, IndexError, ValueError): 
                # Catch issues with finding/parsing the time or name elements
                pass
                
    return all_programmes


if __name__ == '__main__':

    # Fetch and print IP data
    fetch_ip_data(api_key)

    # 0. INITIAL SETUP AND START TIME
    root = ET.Element('tv') 
    root.set('generator-info-name', 'tvepg.eu EPG')
    start_time = datetime.now()
    # print(f"Program started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}") 
    
    # 1. LAUNCH THE BROWSER ONCE
    active_driver = setup_driver() # Assumes setup_driver() is defined
    
    # Store channels that still need to be processed
    channames_to_process = list(channames) # Assumes 'channames' is your full list
    
    # Data storage to persist across driver restarts
    channel_soups = {}
    channel_last_dates = {}
    
    # --- GLOBAL DRIVER RECYCLING PARAMETERS ---
    MAX_GLOBAL_RECYCLES = 5 
    global_recycle_count = 0
    
    # =================================================================
    # PHASE 1: METADATA AND LAST DATE COLLECTION (Aggressive Retrying)
    # =================================================================
    print("--- Starting Phase 1: Metadata and Date Collection ---")

    current_channame_index = 0
    
    # Loop until all channels are processed OR we hit the global recycle limit
    while current_channame_index < len(channames_to_process) and global_recycle_count < MAX_GLOBAL_RECYCLES:
        
        # A. Driver check/re-initialization after a crash
        if active_driver is None:
            active_driver = setup_driver()
            if active_driver is None:
                print("FATAL: Could not initialize driver. Stopping script.", file=sys.stderr)
                break
        
        current_channame = channames_to_process[current_channame_index]
        
        try:
            print(f"\n--- Processing Channel: {current_channame} ---")
            
            # --- START: ORIGINAL PHASE 1 LOGIC ---
            
            # 1. XML Metadata setup 
            channel = ET.SubElement(root, 'channel')
            channel.set('id', channel_ids[current_channame]) # channel_ids is assumed
            display_name = ET.SubElement(channel, 'display-name')
            display_name.text = channel_display_names[current_channame] # channel_display_names is assumed
            
            # 2. URL construction
            today = datetime.now()
            date = today.strftime('%Y-%m-%d')
            url = f"https://tvepg.eu/en/united_kingdom/channel/{current_channame}/{date}"
            
            # 3. Scrape (scrape_url now only returns page_source or raises DriverRecycleError/FatalScrapeError)
            page_source = scrape_url(active_driver, url) # <-- Error raised here

            # 4. Parsing and Data Storage
            soup = BeautifulSoup(page_source, 'html.parser')
            channel_soups[current_channame] = soup
            
            # 5. Icon extraction
            image_url = soup.find('img', itemprop='image')['src']
            icon = ET.SubElement(channel, 'icon')
            icon.set('src', extract_url(image_url))
            
            # 6. last_date extraction
            cards = soup.find_all('div', class_='card')
            last_date = None
            for card in reversed(cards):
                link = card.find('a')
                if link and link.has_attr('href'):
                    original_url = extract_url(link['href'])
                    date_str_last = original_url.split('/')[-1]
                    if len(date_str_last.split('-')) == 3:
                        last_date = date_str_last
                        break
                        
            # 7. last_date validation/retry loop
            max_retries = 3
            retries = 0
            
            if last_date is None:
                print(f"Failed to find any last_date for channel {current_channame}. Skipping channel.", file=sys.stderr)
                current_channame_index += 1
                continue
            
            while retries < max_retries:
                try:
                    datetime.strptime(last_date, '%Y-%m-%d')
                    print(f'{current_channame}: last date found: {last_date}')
                    break  # Success
                except ValueError:
                    print(f"ValueError parsing last_date '{last_date}', retrying scrape (attempt {retries + 1})")
                    retries += 1
                    
                    # Re-fetch and Re-extract logic
                    page_source = scrape_url(active_driver, url)
                    
                    # If scrape_url returns successfully after a retry:
                    soup = BeautifulSoup(page_source, 'html.parser')
                    # RE-EXTRACT last_date from the new soup here (You would put the extraction logic from 6 here)
                    cards = soup.find_all('div', class_='card')
                    last_date = None # Reset and re-extract
                    for card in reversed(cards):
                        link = card.find('a')
                        if link and link.has_attr('href'):
                            original_url = extract_url(link['href'])
                            date_str_last = original_url.split('/')[-1]
                            if len(date_str_last.split('-')) == 3:
                                last_date = date_str_last
                                break
            else: 
                # If retry loop failed permanently
                print(f"Failed to parse last_date after {max_retries} attempts for channel {current_channame}. Skipping channel.")
                current_channame_index += 1
                continue
                
            channel_last_dates[current_channame] = last_date
            
            # SUCCESS! Move to the next channel
            current_channame_index += 1 
            
            # --- END: ORIGINAL PHASE 1 LOGIC ---
            
        # -------------------------------------------------------------
        # --- CATCH FATAL ERROR (IP/SESSION BLOCK) ---
        # -------------------------------------------------------------
        except FatalScrapeError as e:
            print(f"\n!!! FATAL IP BLOCK DETECTED !!! Script terminating. Error: {e}", file=sys.stderr)
            
            # 1. Safely quit the failing driver (CRUCIAL CLEANUP)
            if active_driver:
                try:
                    active_driver.quit()  
                except WebDriverException:
                    pass  
            active_driver = None 
            
            # 2. Terminate the entire script run
            terminate_with_banflag()

        # -------------------------------------------------------------
        # --- CATCH DRIVER RECYCLE (Recycle Driver / Retry Channel) ---
        # -------------------------------------------------------------
        except DriverRecycleError as e:
            # --- DRIVER RECYCLE LOGIC: RETRY SAME CHANNEL ---
            global_recycle_count += 1
            print(f"HARD FAILURE. Recycling driver (Attempt {global_recycle_count}/{MAX_GLOBAL_RECYCLES}). Error: {e}", file=sys.stderr)
            
            # Safely quit the failing driver
            if active_driver:
                try:
                    active_driver.quit()  
                except WebDriverException:
                    pass  
                active_driver = None  
            
            # Index is NOT incremented, loop tries the SAME channel again.
            continue  

        except Exception as e:
            # Catches non-driver critical errors (e.g., unexpected parsing crash)
            print(f"Non-driver critical error during processing {current_channame}: {e}. Skipping channel.", file=sys.stderr)
            current_channame_index += 1 # MOVE ON to the next channel
            continue
            
    # =================================================================
    # PHASE 2: EPG DATA SCRAPE AND XML GENERATION
    # =================================================================
    print("\n--- Starting Phase 2: EPG Data Scrape and XML Generation ---")

    successful_channames = list(channel_soups.keys())

    for channame in successful_channames:
        channel_id = channel_ids[channame]
        soup = channel_soups[channame]
        last_date = channel_last_dates[channame]
        
        # --- Date calculations (timedelta is assumed) ---
        today = datetime.now()
        date = today.strftime('%Y-%m-%d')
        start_date = datetime.strptime(date, '%Y-%m-%d')
        end_date = datetime.strptime(last_date, '%Y-%m-%d')
        dates = []
        current_date = start_date
        while current_date <= end_date:
            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
            
        # Process today's data 
        all_programmes = process_epg_data(active_driver, soup, date)
        
        # Iterate through future dates
        for i, date_to_scrape in enumerate(dates[1:]):
            url = f"https://tvepg.eu/en/united_kingdom/channel/{channame}/{date_to_scrape}"
            
            # Scrape URL with driver crash handling
            try:
                page_source = scrape_url(active_driver, url) 
            except DriverRecycleError:
                # If a driver crash happens in Phase 2, we stop collecting future data 
                # for this channel, and ensure the driver is quit for the NEXT channel.
                print(f"Driver crashed while scraping future date {date_to_scrape}. Skipping remaining dates for {channame}.", file=sys.stderr)
                print("Quitting dead driver and signaling for re-initialization before next channel.", file=sys.stderr)
                
                # 1. Safely quit the failing driver
                if active_driver:
                    try:
                        active_driver.quit()
                    except WebDriverException:
                        pass
                    active_driver = None # Mark as dead
                
                # 2. Break the inner date loop (will proceed to next channel)
                break
            except FatalScrapeError as e:
                # If a fatal error occurs in Phase 2, we must terminate.
                print(f"\n!!! FATAL IP BLOCK DETECTED in Phase 2 !!! Script terminating. Error: {e}", file=sys.stderr)
                if active_driver:
                    try:
                        active_driver.quit()  
                    except WebDriverException:
                        pass  
                active_driver = None
                terminate_with_banflag()
            
            # If execution reaches here, page_source is valid.
                
            soup = BeautifulSoup(page_source, 'html.parser')
            
            if i == len(dates[1:]) - 1:
                programmes = process_epg_data(active_driver, soup, date_to_scrape, process_next_day=True) 
            else:
                programmes = process_epg_data(active_driver, soup, date_to_scrape)
                
            all_programmes.extend(programmes)

        # --- XML Generation ---
        for programme in all_programmes:
            programme_element = ET.SubElement(root, 'programme')
            programme_element.set('start', programme['start_time'].strftime('%Y%m%d%H%M%S') + ' +0000')
            if all_programmes.index(programme) < len(all_programmes) - 1:
                next_programme = all_programmes[all_programmes.index(programme) + 1]
                programme_element.set('stop', next_programme['start_time'].strftime('%Y%m%d%H%M%S') + ' +0000')
            else:
                programme_element.set('stop', (programme['start_time'] + timedelta(hours=1)).strftime('%Y%m%d%H%M%S') + ' +0000')
            programme_element.set('channel', channel_id)

            # Title
            title = ET.SubElement(programme_element, 'title')
            title.set('lang', 'en')
            title.text = programme['program_name']

            # Description
            desc = ET.SubElement(programme_element, 'desc')
            desc.set('lang', 'en')
            desc.text = programme['description']

            # Icon/Image
            if programme.get('image_url') and programme['image_url'] != 'Unknown':
                icon = ET.SubElement(programme_element, 'icon')
                icon.set('src', extract_url(programme['image_url']))

            
    # --- FINAL CLEANUP AND XML OUTPUT ---
    
    # Check if the process completed successfully (all channels processed)
    if current_channame_index == len(channames_to_process):
        try:
            # YOUR FINAL XML WRITING LOGIC HERE (Requires import xml.dom.minidom)
            xml_str = ET.tostring(root, encoding='unicode')
            xml = xml.dom.minidom.parseString(xml_str)
            pretty_xml_str = xml.toprettyxml()
            with open('epguk.xml', 'w', encoding='utf-8') as f:
                f.write(pretty_xml_str)
            
            # Time reporting
            end_time = datetime.now()
            end_timestamp = end_time.strftime('%Y-%m-%d %H:%M:%S')
            total_runtime = end_time - start_time
            total_runtime_str = str(total_runtime).split('.')[0]
            print(f"Program ended at: {end_timestamp}")
            print(f"Total runtime: {total_runtime_str}")
            
        except Exception as e:
            print(f"A critical error occurred during final XML processing: {e}", file=sys.stderr)
            
    else:
        print("Script finished early due to driver failure or channels skipped.", file=sys.stderr)


    # CLOSE THE BROWSER AT THE VERY END 
    if active_driver:
        print("\nClosing final browser session.")
        try:
            active_driver.quit()
        except Exception:
            pass
