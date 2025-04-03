"""
DouScrape.py
=================

Overview:
---------
This module implements a Douyin video scraper that automates the process of searching for videos by hashtags.
This can then be piped into our other modules for further analysis.
It uses Selenium WebDriver to simulate user interactions with the Douyin website, making it easy for users
with limited technical or computer science knowledge to gather video data.

Purpose:
--------
The primary purpose of this module is to provide an end-to-end solution for scraping Douyin video content.
Specifically, it:
  - Launches a Firefox browser (using Selenium) to interact with the Douyin website.
  - Handles login, captcha challenges, and pop-up closures via JavaScript mutation observers.
  - Searches for videos based on provided hashtags and scrolls the page to load a sufficient number of video items.
  - Extracts video links and like counts, and processes this data to determine top videos.
  - Computes overlap data between hashtags and saves interim results for backup.
  - Provides functionality to export the final data (top videos, overlap information) as CSV files.
  - Uses additional libraries (pandas, numpy, pickle, YAML) for data manipulation, file handling, and configuration.

Key Features:
-------------
- **Selenium WebDriver Integration**: Automates browser interactions (clicking, scrolling, entering text)
  to simulate a real user searching on Douyin.
- **Captcha and Popup Handling**: Implements JavaScript mutation observers and dedicated functions to
  detect and dismiss login and captcha pop-ups.
- **Dynamic Data Extraction**: Continuously scrolls the page to load video items and extracts relevant
  data such as video links and like counts.
- **Interim Data Management**: Optionally saves temporary data as CSV files and pickles for recovery.
- **Data Aggregation and Ranking**: Aggregates and sorts video data (e.g., by like counts) to identify top videos.
- **Robust Error Handling**: Contains multiple layers of error checks and retries to handle unexpected
  issues during scraping.

Usage:
------
This module is designed to be executed as a standalone script. Upon running, it will:
  1. Launch a Firefox browser and navigate to Douyin.
  2. Log in if credentials are provided, and manage captcha pop-ups automatically.
  3. Search for videos under specified hashtags.
  4. Scrape video links, like counts, and other metadata.
  5. Compute overlaps among hashtags and generate a ranked list of top videos.
  6. Save the collected data into CSV files for easy viewing and analysis.

Example:
    To run the scraper, simply execute:
        python douyin_scraper.py

Technical Details:
------------------
The module uses the following libraries:
  - **Selenium**: To control the Firefox browser and automate web interactions.
  - **WebDriverWait** and **Expected Conditions (ec)**: To wait for dynamic elements to load.
  - **Python Standard Libraries**: (time, os, re, pickle, itertools) for various utility functions.
  - **Pandas and Numpy**: For data processing, sorting, and CSV file export.
  - **YAML**: For reading and updating Docker configuration files if needed.
  - **Winsound**: To provide audible alerts when a captcha is detected.
  - **Docker SDK**: For monitoring Docker container logs and restarting the container if errors occur.

Docstring Note:
---------------
I'm a wee bit lazy when it comes to docstrings, so this is AI-generated. But hey, it does the job, right?
Similarly, the comments in the code have been fleshed out with AI, because I can't be bothered to write them all myself.
Basically, AI took my fragmented comments and made something helpful to people who might not know what the code does.
Just remember, if you find any typos or weird phrases, it's probably because I didn't proofread it. Cheers!
Contact me regarding any issues or questions you might have.

Author:
-------
Román Schiffino
GitHub: https://github.com/schiffinor

Date:
-----
2025-04-02
"""

import time
from itertools import combinations

import pandas as pd
import selenium.common.exceptions as selexcept
import winsound as ws
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.support.ui import WebDriverWait

from CommentTree import *

global driver
global wait1
global wait3
global wait5
global wait10
global wait30
global mutation_observer_script
global pause_observer_script
global resume_observer_script


# noinspection DuplicatedCode
def scrape_hashtags(n: int,
                    htList: List[str],
                    userPhone: str = None,
                    userPass: str = None,
                    sampleMult: int = 5,
                    saveInterimData: bool = True,
                    calcOverlap: bool = True,
                    outputData: bool = True,
                    waitForLgin: bool = False,
                    basePath: str = None) -> Union[
    Dict[str, Dict[str, int]], Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, int]]], Tuple[
        Dict[str, Dict[str, int]], Dict[str, np.ndarray[Any, np.dtype]]], Tuple[
        Dict[str, Dict[str, int]], Dict[str, Dict[str, int]], Dict[str, np.ndarray[Any, np.dtype]]]]:
    """
    Scrapes Douyin for videos corresponding to each hashtag in htList and returns the top n videos overall,
    sorted in descending order by like count (which approximates video popularity).

    The function uses Selenium to open a Firefox browser, log in (if credentials are provided), and
    navigate to Douyin. It then searches for each hashtag, scrolls to load a sufficient number of videos,
    extracts video URLs and their like counts, and aggregates the data. Optionally, it can compute overlap
    between hashtags and save interim results as CSV files.

    :param n: Total number of top videos to select.
    :param htList: List of hashtags to search on Douyin.
    :param userPhone: Optional; user's phone number for login.
    :param userPass: Optional; user's password for login.
    :param sampleMult: Multiplier to adjust the number of videos sampled per hashtag.
    :param saveInterimData: If True, saves interim scraped video links to CSV.
    :param calcOverlap: If True, calculates the intersection of video links across hashtags.
    :param outputData: If True, outputs final data; if False, only interim data is returned.
    :param waitForLgin: If True, waits for the login process to complete (unused in current logic).
    :param basePath: Optional; base directory for saving interim data. If None, a new path is generated.
    :return: Depending on flags, returns a dictionary or a tuple of dictionaries containing the video data.
    :raises ValueError: If htList is empty or if n is less than 1.
    """
    # Determine if user credentials are provided.
    userInfo: bool = userPhone is not None and userPass is not None
    if not htList:
        raise ValueError("htList must contain at least one hashtag.")
    if n < 1:
        raise ValueError("n must be a positive integer.")
    if not userInfo:
        wn.warn("User phone number and password are required for Douyin login.")

    # Set up Firefox options using Selenium.
    options = Options()
    options.headless = False  # Change to True for headless browsing if desired.
    options.set_preference("media.volume_scale", "0.0")  # Mute audio.
    options.log.level = "trace"
    global driver
    driver = webdriver.Firefox(options=options)

    # Enable browser logging for debugging purposes.
    d = DesiredCapabilities.FIREFOX
    d['loggingPrefs'] = {'browser': 'ALL'}

    # Define wait times to ensure dynamic elements load correctly.
    global wait1, wait3, wait5, wait10, wait30
    wait1 = WebDriverWait(driver, 1)
    wait3 = WebDriverWait(driver, 3)
    wait5 = WebDriverWait(driver, 5)
    wait10 = WebDriverWait(driver, 10)
    wait30 = WebDriverWait(driver, 30)

    # JavaScript mutation observer to auto-close login and download popups.
    mutation_observer_script = """
    (function() {
        const observer = new MutationObserver(function(mutationsList) {
            mutationsList.forEach(function(mutation) {
                mutation.addedNodes.forEach(function(node) {
                    if (node.nodeType === Node.ELEMENT_NODE) {
                        if (node.matches('.douyin-login')) {
                            try {
                                const closeBtn = node.querySelector('.douyin-login__close.dy-account-close');
                                if (closeBtn) {
                                    closeBtn.click();
                                    console.log('Popup closed by observer.');
                                }
                            } catch (e) {
                                // Ignore errors during popup closure.
                            }
                        }
                        if (node.matches('.douyin-web-download-guide-container')) {
                            try {
                                const closeBtn = node.querySelector('.FKwOQ0w9');
                                if (closeBtn) {
                                    closeBtn.click();
                                    console.log('Popup closed by observer.');
                                }
                            } catch (e) {
                                // Ignore errors.
                            }
                        }
                    }
                });
            });
        });
        
        // Expose pause and resume functions to window for external control.
        window.pauseMutationObserver = function() {
            observer.disconnect();
            console.log('MutationObserver paused.');
        };
        
        window.resumeMutationObserver = function() {
            observer.observe(document.body, { childList: true, subtree: true });
            console.log('MutationObserver resumed.');
        };
        
        // Start observing immediately.
        observer.observe(document.body, { childList: true, subtree: true });
    })();
    """
    pause_observer_script = "window.pauseMutationObserver();"
    resume_observer_script = "window.resumeMutationObserver();"

    # Inject the mutation observer into the browser.
    driver.execute_script(mutation_observer_script)

    # Calculate the number of videos to sample per hashtag.
    videos_per_hashtag = int(2.5 * n) // (len(htList)) if htList else 0
    video_sample_per_ht = videos_per_hashtag * sampleMult
    tagDictDict = {}  # Will store video like counts by hashtag.
    tagDataDict = {}  # Will store raw video data arrays per hashtag.

    # Local helper to generate a base path for interim data storage.
    def basePather(basePath: str = None):
        if basePath is None:
            basePath = f"interimData\\{(str(dt.now()).replace(" ","_").replace(":", "-").replace(".", "_"))}\\"
            return basePath
        else:
            return basePath

    # Determine the base path for interim data.
    basePath = basePather(basePath)

    # Iterate through each hashtag to scrape relevant video data.
    for hashtag in htList:
        print(f"Searching for hashtag: {hashtag}")
        # Use Douyin's homepage as the search starting point.
        search_url = f"https://www.douyin.com"

        # Inner function to handle login actions.
        def login(wfl: bool):
            driver.execute_script(pause_observer_script)
            if wfl:
                try:
                    wait1.until(ec.presence_of_element_located((By.CLASS_NAME, "douyin-login__close.dy-account-close")))
                    loginAppear = 1
                except selexcept.TimeoutException:
                    wn.warn("Login box did not appear.")
                    loginAppear = 0

                if loginAppear == 1:
                    if userInfo:
                        # If credentials are provided, fill in the login form.
                        wrapTab = driver.find_element(by=By.CLASS_NAME, value="web-login-common-wrapper__tab")
                        wrapTabList = wrapTab.find_element(by=By.CLASS_NAME, value="web-login-tab-list")
                        wrapTabList.find_element(by=By.CLASS_NAME, value="web-login-tab-list__item").click()
                        wait10.until(ec.presence_of_element_located((By.CLASS_NAME, "web-login-normal-input__input")))
                        phoneInput = driver.find_element(by=By.CLASS_NAME, value="web-login-normal-input__input")
                        phoneInput.send_keys(userPhone)
                        passInput = driver.find_element(by=By.CLASS_NAME, value="web-login-button-input__input")
                        passInput.send_keys(userPass)
                        passInput.send_keys(Keys.RETURN)
                        driver.execute_script(resume_observer_script)
                    else:
                        # If no credentials, simply close the login popup.
                        driver.find_element(by=By.CLASS_NAME, value="douyin-login__close.dy-account-close").click()
                        driver.execute_script(resume_observer_script)

        # Inner function to simulate hovering over an element.
        def hover(element):
            driver.execute_script("console.log('Scrolling element into view'); arguments[0].scrollIntoView(true);", element)
            wait3.until(ec.visibility_of(element))
            ActionChains(driver).move_to_element(element).perform()

        # Inner function to pause execution for captcha handling.
        def captchaPause(timeout: int = 2, timeout2: int = 30) -> float:
            global driver
            timeStart = time.time()
            waitT1 = WebDriverWait(driver, timeout)
            waitT2 = WebDriverWait(driver, timeout2)
            try:
                waitT1.until(ec.visibility_of_element_located((By.ID, "captcha_container")))
                try:
                    wn.warn("Captcha detected. Please solve the captcha.")
                    ws.Beep(1000, 300)
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    ws.Beep(1000, 300)
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    print("Please solve the captcha.")
                except selexcept.TimeoutException:
                    wn.warn("Captcha resolved or disappeared.")
                    return time.time() - timeStart
            except selexcept.TimeoutException:
                wn.warn("Captcha not found.")
            return time.time() - timeStart

        # Inner function that executes the complete search workflow for a given hashtag.
        def whole_search():
            global driver, wait1, wait3, wait5, wait10, wait30
            driver.get(search_url)
            wait1 = WebDriverWait(driver, 1)
            wait3 = WebDriverWait(driver, 3)
            wait5 = WebDriverWait(driver, 5)
            wait10 = WebDriverWait(driver, 10)
            wait30 = WebDriverWait(driver, 30)

            # Load session cookies to preserve login state.
            with open("cookies.pickle", "rb") as file:
                cookies = pickle.load(file)
            for cookie in cookies:
                driver.add_cookie(cookie)

            driver.get(search_url)
            captchaPause()
            driver.execute_script(mutation_observer_script)
            print("Opened Douyin.")
            print("Searching for hashtag: ", hashtag)
            print(f"Collecting {videos_per_hashtag} videos for hashtag.")

            captchaPause()
            wait3.until(ec.presence_of_element_located((By.ID, "douyin-header")))
            douyinHeader = driver.find_element(by=By.ID, value="douyin-header")
            searchBox = douyinHeader.find_element(by=By.XPATH, value="//div/div[1]/input[@data-e2e='searchbar-input']")
            login(True)
            searchBox.send_keys(hashtag + Keys.RETURN)

            try:
                wait3.until(ec.presence_of_element_located((By.CLASS_NAME, "search-result-card")))
            except selexcept.TimeoutException:
                try:
                    searchBttn = driver.find_element(by=By.XPATH, value="//div/div[2]/div/button[@data-e2e='searchbar-button']")
                    login(True)
                    try:
                        searchBttn.click()
                    except selexcept.ElementClickInterceptedException:
                        captchaPause()
                        searchBttn.click()
                except selexcept.NoSuchElementException:
                    wn.warn("Search button not found.")
                    found = False
                    while not found:
                        try:
                            searchBox = driver.find_element(by=By.XPATH, value="//div/div[1]/input[@data-e2e='searchbar-input']")
                            searchBox.send_keys(hashtag + Keys.RETURN)
                        except selexcept.NoSuchElementException:
                            wn.warn("Search box not found.")
                            driver.get(search_url)
                            with open("cookies.pickle", "rb") as file:
                                cookies = pickle.load(file)
                            for cookie in cookies:
                                driver.add_cookie(cookie)
                            driver.get(search_url)
                            captchaPause()
                            driver.execute_script(mutation_observer_script)
                            wait3.until(ec.presence_of_element_located((By.ID, "douyin-header")))
                            douyinHeader = driver.find_element(by=By.ID, value="douyin-header")
                            searchBox = douyinHeader.find_element(by=By.XPATH, value="//div/div[1]/input[@data-e2e='searchbar-input']")
                            captchaPause()
                            login(True)
                            captchaPause()
                            searchBox.send_keys(hashtag + Keys.RETURN)
                        try:
                            driver.find_element(by=By.CLASS_NAME, value="search-result-card")
                            found = True
                        except selexcept.TimeoutException:
                            wn.warn("Search results did not appear.")
                            found = False

            # Locate the grid button that switches the view mode.
            gridButtonFnd = False
            gridButtonFnd2 = False
            timeout = time.time() + 30  # 30-second timeout for grid button search.
            while not gridButtonFnd and not gridButtonFnd2 and time.time() < timeout:
                try:
                    wait3.until(ec.presence_of_element_located(
                        (By.XPATH, "/html/body/div[2]/div[1]/div[3]/div[3]/div/div/div[1]/div[1]/div/div/div/div[1]")))
                    wait3.until(ec.element_to_be_clickable(
                        (By.XPATH, "/html/body/div[2]/div[1]/div[3]/div[3]/div/div/div[1]/div[1]/div/div/div/div[1]")))
                    gridButtonFnd = True
                    try:
                        gridButton = driver.find_element(by=By.XPATH, value="/html/body/div[2]/div[1]/div[3]/div[3]/div/div/div[1]/div[1]/div/div/div/div[1]")
                        gridButtonFnd2 = True
                    except selexcept.NoSuchElementException:
                        wn.warn("Grid button not found.")
                except selexcept.TimeoutException:
                    wn.warn("Grid button not found.")
            if not gridButtonFnd or not gridButtonFnd2:
                raise TimeoutError("Grid button not found.")


            # Go to grid mode. Commenting out the grid button click as it may not be necessary. Not removed in case it is needed in the future.
            """wait5.until(EC.presence_of_element_located((By.CLASS_NAME, "search-result-card")))
            wait5.until(EC.presence_of_element_located((By.XPATH, "//div[@class='xVpK5ilp']/div[1]")))
            gridButton = driver.find_element(by=By.XPATH, value="//div[@class='xVpK5ilp']/div[1]")
            gridButton.click()"""

            # click video tab
            try:
                wait5.until(ec.presence_of_element_located((By.ID, "waterFallScrollContainer")))
                return driver
            except selexcept.TimeoutException:
                wn.warn("Video tab not found.")
                driver.quit()
                driver = webdriver.Firefox(options=options)
                whole_search()

        # Execute the whole search process.
        whole_search()

        def videoMode():
            global driver, wait1, wait3, wait5, wait10, wait30
            # Reinitialize wait times.
            wait1 = WebDriverWait(driver, 1)
            wait3 = WebDriverWait(driver, 3)
            wait5 = WebDriverWait(driver, 5)
            wait10 = WebDriverWait(driver, 10)
            wait30 = WebDriverWait(driver, 30)
            # Find the video tab and click it.
            vidTab = driver.find_element(by=By.XPATH, value="//div/div/div/span[@data-key='video']")
            try:
                vidTab.click()
            except selexcept.ElementClickInterceptedException:
                wn.warn("Video tab not clicked.")
                driver.quit()
                driver = webdriver.Firefox(options=options)
                whole_search()
                videoMode()

            # Go to grid mode. Currently commented out as it caused issues. Not removed just in case we need it later.
            """wait5.until(EC.presence_of_element_located((By.CLASS_NAME, "search-result-card")))
            wait5.until(EC.presence_of_element_located((By.XPATH, "//div[@class='xVpK5ilp']/div[1]")))
            gridButton = driver.find_element(by=By.XPATH, value="//div[@class='xVpK5ilp']/div[1]")
            gridButton.click()"""

            try:
                wait3.until(ec.presence_of_element_located((By.XPATH, "//ul[@data-e2e='scroll-list']")))
                wait10.until(ec.presence_of_element_located((By.XPATH, "//div[@class='search-result-card']//a/div/div[1]/div/div[2]/div[2]/div[3]/span")))
                return driver
            except selexcept.TimeoutException:
                wn.warn("Video tab not found.")
                driver.quit()
                driver = webdriver.Firefox(options=options)
                driver = whole_search()
                videoMode()

        # Switch to video mode.
        videoMode()

        # Define the CSS class selector for individual video items.
        video_selector = "search-result-card"

        # Initialize a set to collect unique video links and a dictionary to store like counts.
        linkSet = set()
        likeMap: Dict[str, int] = {}
        timeout = time.time() + 300  # Set a total timeout of 300 seconds for scrolling.
        count = 0
        prevCounts = []
        captchaPause()
        # Loop to scroll and collect video links until the required number is reached or timeout occurs.
        while len(linkSet) < video_sample_per_ht and time.time() < timeout:
            timeout = timeout + captchaPause(1)
            count += 1
            # Scroll to the bottom of the page to trigger lazy-loading of videos.
            bottomElem = driver.find_elements(By.CLASS_NAME, "search-result-card")[-1]
            bottomElem.location_once_scrolled_into_view
            driver.find_element(by=By.CSS_SELECTOR, value="html").send_keys(Keys.CONTROL + Keys.END)
            bottomElem = driver.find_elements(By.CLASS_NAME, "search-result-card")[-1]
            bottomElem.location_once_scrolled_into_view
            time.sleep(2)  # Wait for new content to load.
            video_items = driver.find_elements(By.CLASS_NAME, video_selector)
            print(f"Video items: {len(video_items)}")
            # Extract video links and like counts from the video items.
            for vid in video_items:
                try:
                    vidLinkNode = vid.find_element(By.XPATH, ".//a")
                    vidLink = vidLinkNode.get_attribute("href")
                    print(f"Video link: {vidLink}")
                    linkSet.add(vidLink)
                    try:
                        likeCount = vid.find_element(By.XPATH, ".//a/div/div[1]/div/div[2]/div[2]/div[3]/span").text
                        print(f"Like count: {likeCount}")
                        # Convert abbreviated like counts to integer values.
                        if likeCount[-1] == "万":
                            likeCount = int(float(likeCount[:-1]) * 10000)
                        elif likeCount[-1] in ["k", "K"]:
                            likeCount = int(float(likeCount[:-1]) * 1000)
                        else:
                            likeCount = int(likeCount)
                        likeMap[vidLink] = likeCount
                    except selexcept.NoSuchElementException:
                        print("Like count not found.")
                        wn.warn("Like count not found.")
                except selexcept.NoSuchElementException:
                    print("Video link not found.")
                    wn.warn("Video link not found.")
            prev_count = len(linkSet)
            prevCounts.append(prev_count)
            # If no new videos are being loaded, break to avoid an infinite loop.
            if count > 5 and prevCounts[-1] == prevCounts[-3]:
                break

        print(f"Collected {len(linkSet)} videos for hashtag.")
        print(f"Like Map: {likeMap}")
        likeMapItems = likeMap.items()
        # Create a numpy array from the likeMap for easy sorting.
        likeArr = np.array([[item[0], item[1], hashtag] for item in likeMapItems], dtype=object)
        print(f"Likes array: {likeArr}")
        # Sort the array by the like counts (descending).
        likeArr = likeArr[likeArr[:, 1].argsort()[::-1]]
        # Limit the results to the desired number of videos per hashtag.
        likeArr = likeArr[:videos_per_hashtag]
        linkSet = set(likeArr[:, 0])
        likeMap: Dict[str, int] = {str(item[0]): int(item[1]) for item in likeArr}
        print(f"Likes array: {likeArr}")
        print(f"Link Set: {linkSet}")
        print(f"Like Map: {likeMap}")

        # If enabled, save the interim video data (links, like counts, hashtag) to a CSV file.
        if saveInterimData:
            print(f"base path: {basePath}")  # Debug information.
            os.makedirs(os.path.dirname(basePath), exist_ok=True)
            df = pd.DataFrame(likeArr, columns=["Video Link", "Likes", "Hashtag"])
            path = f"{basePath}{hashtag}_videos.csv"
            df.to_csv(path, index=False)

        # Store the like counts and raw data for the current hashtag.
        tagDictDict[hashtag] = likeMap
        print(f"Collected {len(linkSet)} videos for hashtag.")
        tagDataDict[hashtag] = likeArr

    # After processing all hashtags, determine what to return based on calcOverlap and outputData flags.
    if not all([calcOverlap, outputData]):
        driver.quit()
        return tagDictDict

    if calcOverlap:
        overlapDict = {}
        # Compute the intersection of video links for every combination of hashtags.
        for comboSize in range(2, len(htList)):
            for combo in combinations(hashtags, comboSize):
                comboSet = set.intersection(*[set(tagDictDict[ht].keys()) for ht in combo])
                dictFromSet: Dict[str, int] = {key: tagDictDict[combo[0]][key] for key in comboSet}
                overlapDict["-".join(combo)] = dictFromSet
        if saveInterimData:
            df = pd.DataFrame(overlapDict.items(), columns=["Hashtags", "Video Links"])
            path = f"{basePath}overlap_data.csv"
            df.to_csv(path, index=False)
        if not outputData:
            driver.quit()
            return tagDictDict, overlapDict
        else:
            driver.quit()
            return tagDictDict, overlapDict, tagDataDict
    elif outputData:
        return tagDictDict, tagDataDict
    else:
        raise ValueError("This should not happen.")


def top_videos(tagDictDict: Dict[str, Dict[str, int]], n: int, valList: bool = True, dictify: bool = False,
               htDict: bool = False) -> List[str] | Dict[str, int] | Tuple[List[str], List[int]] | Tuple[
    List[str], Dict[str, int]] | Tuple[List[str], List[int], Dict[str, str]] | Tuple[Dict[str, int], Dict[str, str]]:
    """
    Extract the top n videos from the collected hashtag data.

    This function merges the video data across all hashtags, sorts the videos by their like counts in
    descending order, and returns the top n video links. It offers several formatting options for the output:
      - As a dictionary of video links to like counts.
      - As a tuple of lists (video links and like counts).
      - Optionally, include a dictionary mapping each video link to its corresponding hashtag.

    :param tagDictDict: Dictionary mapping each hashtag to a dictionary of video links and their like counts.
    :param n: The number of top videos to extract.
    :param valList: If True, returns video links and like counts as two lists.
    :param dictify: If True, returns the results as a dictionary.
    :param htDict: If True, includes a dictionary mapping video links to their hashtags.
    :return: The top videos in one of several possible formats based on the flags.
    """
    # Combine video data from all hashtags into a single dictionary.
    unionDict = {}
    unionDictHT = {}
    for ht in tagDictDict:
        unionDict.update(tagDictDict[ht])
        # Create a mapping from each video link to its hashtag.
        unionDictHT.update({key: ht for key in tagDictDict[ht]})
    print(f"Union Dict: {unionDict}")
    print(f"Union Dict HT: {unionDictHT}")
    # Sort the combined dictionary by like counts in descending order and take the top n.
    sortedDict = dict(sorted(unionDict.items(), key=lambda x: x[1], reverse=True)[:n])
    sortedDictHT = {key: unionDictHT[key] for key in sortedDict}
    print(f"Sorted Dict: {sortedDict}")
    print(f"Sorted Dict HT: {sortedDictHT}")
    # Format the output based on the parameters.
    if dictify:
        if htDict:
            return sortedDict, sortedDictHT
        return sortedDict
    if valList:
        if htDict:
            return list(sortedDict.keys())[:n], list(sortedDict.values())[:n], sortedDictHT
        return list(sortedDict.keys())[:n], list(sortedDict.values())[:n]
    if htDict:
        return list(sortedDict.keys())[:n], sortedDictHT
    return list(sortedDict.keys())[:n]


def exBasePather(basePath: str = None):
    """
    Generate an output base path for saving data files.

    If no base path is provided, this function creates a new directory path based on the current date
    and time (formatted to be file-system friendly). If a base path is provided, it is returned unchanged.

    :param basePath: Optional; user-specified base path.
    :return: A string representing the base output directory.
    """
    if basePath is None:
        basePath = f"outData\\{(str(dt.now()).replace(" ","_").replace(":", "-").replace(".", "_"))}\\"
        return basePath
    else:
        return basePath


if __name__ == "__main__":
    """
    Main Execution Block:
    ---------------------
    This block serves as an entry point for running the Douyin scraper directly. It demonstrates the end-to-end
    process of scraping video data based on a set of predefined hashtags, processing the scraped data to extract the
    top videos based on like counts, and finally saving the results to both a pickle file and a CSV file.

    The workflow is as follows:
      1. Two lists of hashtags (hashtags and hashtags2) are defined. In this example, hashtags2 is used to scrape videos.
      2. The variable 'top_n' sets the number of top videos to select.
      3. The function `scrape_hashtags` is called with parameters to scrape the videos:
         - It collects video data for each hashtag.
         - The 'saveInterimData' flag indicates that interim data (such as video links and like counts) should be saved.
         - The 'calcOverlap' flag is set to False, meaning no intersection of video links across hashtags will be computed.
         - The 'outputData' flag is True so that the scraped data is returned.
         - The 'sampleMult' parameter adjusts the sampling rate per hashtag.
      4. The scraped data is saved to a pickle file for potential later use.
      5. The `top_videos` function is then used to extract the top videos based on like counts.
         - This function merges video data across hashtags, sorts the videos by like count in descending order,
           and provides the top 'n' results.
      6. The top videos along with their associated like counts and hashtags are printed to the console.
      7. A Pandas DataFrame is created with columns ["hashtag", "video", "likes"] to store the top video information.
      8. The DataFrame is saved as a CSV file in a dynamically generated output directory (using `exBasePather`).
      9. Finally, the script prints confirmation messages indicating that the top videos have been saved and that
         the process is complete.

    Note:
    -----
    This block is provided as an example usage scenario for the scraper. In production, parameters such as hashtags,
    output paths, and the number of top videos (top_n) can be adjusted as needed.
    """
    # Define two sets of hashtags; here, we use 'hashtags2' for scraping.
    hashtags = [
        "科技界性别平等",
        "打破职场偏见",
        "她也能编码",
        "抖音为平等发声",
        "挑战刻板印象",
        "女性科技领袖",
        "平等未来由你创",
        "职场中的她们力量",
        "算法支持平等",
        "性别平等行动派",
        "用幽默改变观念",
        "科技行业多元共融",
        "平等始于对话",
        "拒绝性别标签",
        "她经济崛起",
        "数字倡导新浪潮",
        "职场妈妈也顶半边天",
        "平等从字节跳动开始",
        "科技女孩向前冲",
        "文化共鸣改变行业",
        "她主宰算法"
    ]
    hashtags2 = [
        "码力女孩",
        "科技无性别",
        "数据里的她力量",
        "别叫我女程序员",
        "传统不是借口",
        "平等从键盘开始",
        "职场天花板粉碎机",
        "直男编程vs姐式编程",
        "反卷性别偏见",
        "老板我要平等",
        "用短视频打破沉默",
        "大厂平等报告",
        "招聘无性别",
        "董事会需要她",
        "小镇科技女孩",
        "妈妈也是极客",
        "无性别创新",
        "Z世代要平等",
        "未来科技她定义"
    ]
    top_n = 250  # Total number of top videos to select.

    # Scrape videos using the specified hashtags. In this example, hashtags2 is used.
    videos = scrape_hashtags(top_n, hashtags2, saveInterimData=True, calcOverlap=False, outputData=True, sampleMult=4)

    # Save the scraped video data to a pickle file for backup or further processing.
    pickle.dump(videos, open("videos2.pickle", "wb"))
    print(videos)

    # Extract top videos from the scraped data.
    print("Top videos:")
    output: Tuple[Dict[str, int], Dict[str, str]] = top_videos(videos[0], top_n, dictify=True, htDict=True)
    top_vids_dict: Dict[str, int] = output[0]
    top_vids_ht_dict: Dict[str, str] = output[1]

    # Print the dictionaries containing the top video links, their like counts, and corresponding hashtags.
    print(f"Top videos dict: {top_vids_dict}")
    print(f"Top videos ht dict: {top_vids_ht_dict}")
    print("vid Count: ", len(top_vids_dict))
    for i, vidKey in enumerate(top_vids_dict.keys()):
        print(f"i: {i} Video: {vidKey}, Likes: {top_vids_dict[vidKey]} Hashtag: {top_vids_ht_dict[vidKey]}")

    # Create a Pandas DataFrame with columns for hashtag, video link, and like count.
    df: pd.DataFrame = pd.DataFrame(columns=["hashtag", "video", "likes"])
    for vidKey in top_vids_dict.keys():
        newRow = pd.Series({"hashtag": top_vids_ht_dict[vidKey], "video": vidKey, "likes": top_vids_dict[vidKey]})
        df.loc[len(df)] = newRow
    print(df)

    # Generate an output base path and ensure the directory exists.
    basePath = exBasePather()
    os.makedirs(os.path.dirname(basePath), exist_ok=True)

    # Save the DataFrame containing top videos to a CSV file.
    df.to_csv(f"{basePath}top_videos.csv", index=False)
    print("Top videos saved to CSV.")
    print("Done.")
