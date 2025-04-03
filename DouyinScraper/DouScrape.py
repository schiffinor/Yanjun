from CommentTree import *
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
import selenium.common.exceptions as selexcept
import time
from typing import List, Dict, Any
import numpy as np
import pandas as pd
from itertools import combinations
import pickle
import winsound as ws
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
                    outputData: bool  = True,
                    waitForLgin: bool = False,
                    basePath: str = None) -> Union[Dict[str, Dict[str, int]], Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, int]]], Tuple[Dict[str, Dict[str, int]], Dict[str, np.ndarray[Any, np.dtype]]], Tuple[Dict[str, Dict[str, int]], Dict[str, Dict[str, int]], Dict[str, np.ndarray[Any, np.dtype]]]]:
    """
    Scrapes Douyin for videos under each hashtag in htList and returns the top n videos overall,
    sorted by the number of watches (descending).

    :param n: Total number of top videos to select.
    :param htList: List of hashtags to search.
    :return: A list of dictionaries containing video data.
    """
    userInfo: bool = userPhone is not None and userPass is not None
    if not htList:
        raise ValueError("htList must contain at least one hashtag.")
    if n < 1:
        raise ValueError("n must be a positive integer.")
    if not userInfo:
        wn.warn("User phone number and password are required for Douyin login.")

    # Set up Firefox options (e.g., headless mode)
    options = Options()
    options.headless = False  # Change to True if you want headless mode
    options.set_preference("media.volume_scale", "0.0")
    options.log.level = "trace"
    # options.add_argument("-devtools")
    global driver
    driver = webdriver.Firefox(options=options)

    # enable browser logging
    d = DesiredCapabilities.FIREFOX
    d['loggingPrefs'] = {'browser': 'ALL'}

    # various wait times
    global wait1
    global wait3
    global wait5
    global wait10
    global wait30
    wait1 = WebDriverWait(driver, 1)
    wait3 = WebDriverWait(driver, 3)
    wait5 = WebDriverWait(driver, 5)
    wait10 = WebDriverWait(driver, 10)
    wait30 = WebDriverWait(driver, 30)

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
                                // Continue if error occurs.
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
                                // Continue if error occurs.
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

    driver.execute_script(mutation_observer_script)

    # Determine how many videos to collect per hashtag
    videos_per_hashtag = int(2.5 * n) // (len(htList)) if htList else 0
    video_sample_per_ht = videos_per_hashtag * sampleMult
    tagDictDict = {}
    tagDataDict = {}

    # Define a function to create a base path for saving interim data.
    def basePather(basePath: str = None):
        if basePath is None:
            basePath = f"interimData\\{(str(dt.now()).replace(" ","_").replace(":", "-").replace(".", "_"))}\\"
            return basePath
        else:
            return basePath

    basePath = basePather(basePath)

    for hashtag in htList:
        print(f"Searching for hashtag: {hashtag}")
        # Construct the search URL for the hashtag.
        # (You might need to adjust the URL according to Douyin's actual search URL pattern.)
        search_url = f"https://www.douyin.com"
        def login(wfl: bool):
            driver.execute_script(pause_observer_script)
            if wfl:
                # Wait for the search box to appear
                try:
                    # should be 10 sec temporarily 1 sec
                    wait1.until(ec.presence_of_element_located((By.CLASS_NAME, "douyin-login__close.dy-account-close")))
                    loginAppear = 1
                except selexcept.TimeoutException:
                    wn.warn("Login box did not appear.")
                    loginAppear = 0

                # if login box appears, log in or close it
                if loginAppear == 1:
                    # Log in if user information is provided, else close login box
                    if userInfo:
                        # Find the login box
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
                        # Close the login box
                        driver.find_element(by=By.CLASS_NAME, value="douyin-login__close.dy-account-close").click()
                        driver.execute_script(resume_observer_script)
        # define hover action
        def hover(element):
            # Scroll the element into view
            driver.execute_script("console.log('were here');arguments[0].scrollIntoView(true);", element)
            # Wait until the element is visible (up to 10 seconds)
            wait3.until(ec.visibility_of(element))
            # Then perform the hover action
            ActionChains(driver).move_to_element(element).perform()

        def captchaPause(timeout: int = 2, timeout2: int = 30) -> float:
            global driver
            timeStart = time.time()
            # Create wait objects
            waitT1 = WebDriverWait(driver, timeout)
            waitT2 = WebDriverWait(driver, timeout2)
            # Pause for captcha
            try:
                waitT1.until(ec.visibility_of_element_located((By.ID, "captcha_container")))
                # give user time to solve captcha
                try:
                    wn.warn("Captcha detected. Please solve the captcha.")
                    # play sound to alert user to solve captcha
                    # noinspection DuplicatedCode
                    ws.Beep(1000, 300)

                    # wait for user to solve captcha
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    # play sound to alert user to solve captcha
                    # noinspection DuplicatedCode
                    ws.Beep(1000, 300)
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    waitT2.until(ec.invisibility_of_element_located((By.ID, "captcha_container")))
                    print("you're dumb, solve the captcha")
                except selexcept.TimeoutException:
                    wn.warn("captcha gone, you're good gang")
                    return time.time() - timeStart
            except selexcept.TimeoutException:
                wn.warn("Captcha not found.")
            return time.time() - timeStart

        # noinspection DuplicatedCode
        def whole_search():
            # various wait times
            global driver
            driver.get(search_url)
            global wait1
            global wait3
            global wait5
            global wait10
            global wait30
            wait1 = WebDriverWait(driver, 1)
            wait3 = WebDriverWait(driver, 3)
            wait5 = WebDriverWait(driver, 5)
            wait10 = WebDriverWait(driver, 10)
            wait30 = WebDriverWait(driver, 30)

            # Load cookies
            with open("cookies.pickle", "rb") as file:
                cookies = pickle.load(file)

            for cookie in cookies:
                driver.add_cookie(cookie)

            # Reload the page
            driver.get(search_url)
            captchaPause()

            driver.execute_script(mutation_observer_script)
            print("Opened Douyin.")

            print("Searching for hashtag: ", hashtag)
            print(f"Collecting {videos_per_hashtag} videos for hashtag.")

            # Check for login box and log in if necessary

            # check for captcha
            captchaPause()

            # wait 3 sec for the search box to appear
            wait3.until(ec.presence_of_element_located((By.ID, "douyin-header")))
            douyinHeader = driver.find_element(by=By.ID, value="douyin-header")
            searchBox = douyinHeader.find_element(by=By.XPATH, value="//div/div[1]/input[@data-e2e='searchbar-input']")
            login(True)
            searchBox.send_keys(hashtag + Keys.RETURN)


            # wait 5 sec for the sort button to appear
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

                            # Load cookies
                            with open("cookies.pickle", "rb") as file:
                                cookies = pickle.load(file)

                            for cookie in cookies:
                                driver.add_cookie(cookie)

                            # Reload the page
                            driver.get(search_url)

                            captchaPause()
                            driver.execute_script(mutation_observer_script)
                            # wait 3 sec for the search box to appear
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


            gridButtonFnd = False
            gridButtonFnd2 = False
            timeout = time.time() + 30  # 30-second timeout
            while not gridButtonFnd and not gridButtonFnd2 and time.time() < timeout:
                try:
                    wait3.until(ec.presence_of_element_located((By.XPATH, "/html/body/div[2]/div[1]/div[3]/div[3]/div/div/div[1]/div[1]/div/div/div/div[1]")))
                    wait3.until(ec.element_to_be_clickable((By.XPATH, "/html/body/div[2]/div[1]/div[3]/div[3]/div/div/div[1]/div[1]/div/div/div/div[1]")))
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


            # go to grid mode
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
                # quit and reload driver
                driver.quit()
                driver = webdriver.Firefox(options=options)
                whole_search()


        whole_search()

        def videoMode():
            global driver
            global wait1
            global wait3
            global wait5
            global wait10
            global wait30
            wait1 = WebDriverWait(driver, 1)
            wait3 = WebDriverWait(driver, 3)
            wait5 = WebDriverWait(driver, 5)
            wait10 = WebDriverWait(driver, 10)
            wait30 = WebDriverWait(driver, 30)
            vidTab = driver.find_element(by=By.XPATH, value="//div/div/div/span[@data-key='video']")
            try:
                vidTab.click()
            except selexcept.ElementClickInterceptedException:
                wn.warn("Video tab not clicked.")
                # quit and reload driver
                driver.quit()
                driver = webdriver.Firefox(options=options)
                whole_search()
                videoMode()

            # go to grid mode
            """wait5.until(EC.presence_of_element_located((By.CLASS_NAME, "search-result-card")))
            wait5.until(EC.presence_of_element_located((By.XPATH, "//div[@class='xVpK5ilp']/div[1]")))
            gridButton = driver.find_element(by=By.XPATH, value="//div[@class='xVpK5ilp']/div[1]")
            gridButton.click()"""

            try:
                wait3.until(ec.presence_of_element_located((By.XPATH, "//ul[@data-e2e='scroll-list']")))
                # Scroll to the bottom of the page to load more videos until the desired number of videos is reached.

                # wait until requisite stuff appears:
                wait10.until(ec.presence_of_element_located((By.XPATH, "//div[@class='search-result-card']//a/div/div[1]/div/div[2]/div[2]/div[3]/span")))
                return driver
            except selexcept.TimeoutException:
                wn.warn("Video tab not found.")
                # quit and reload driver
                driver.quit()
                driver = webdriver.Firefox(options=options)
                driver = whole_search()
                videoMode()

        videoMode()

        # Define the class selector for individual video items.
        video_selector = "search-result-card"

        # Get the initial count of video items.
        # Commented out cuz not necessary now and messes with things
        # driver.find_element(by=By.CSS_SELECTOR, value="body").click()
        # Scroll until the number of video items is at least videos_per_hashtag.
        linkSet = set()
        likeMap: Dict[str, int] = {}
        timeout = time.time() + 300  # 300-second timeout
        count = 0
        prevCounts = []
        captchaPause()
        while len(linkSet) < video_sample_per_ht and time.time() < timeout:
            timeout = timeout + captchaPause(1)
            count += 1
            # Scroll to the bottom of the page to load more videos.
            bottomElem = driver.find_elements(By.CLASS_NAME, "search-result-card")[-1]
            bottomElem.location_once_scrolled_into_view
            driver.find_element(by=By.CSS_SELECTOR, value="html").send_keys(Keys.CONTROL + Keys.END)
            bottomElem = driver.find_elements(By.CLASS_NAME, "search-result-card")[-1]
            bottomElem.location_once_scrolled_into_view
            time.sleep(2)  # Pause to allow new content to load
            video_items = driver.find_elements(By.CLASS_NAME, video_selector)
            print(f"Video items: {len(video_items)}")
            # Add Video Links to linkSet to avoid duplicates
            for vid in video_items:
                try:
                    vidLinkNode = vid.find_element(By.XPATH, ".//a")
                    vidLink = vidLinkNode.get_attribute("href")
                    print(f"Video link: {vidLink}")
                    linkSet.add(vidLink)
                    try:
                        likeCount = vid.find_element(By.XPATH, ".//a/div/div[1]/div/div[2]/div[2]/div[3]/span").text
                        print(f"Like count: {likeCount}")
                        if likeCount[-1] == "万":
                            likeCount = int(float(likeCount[:-1]) * 10000)
                        elif likeCount[-1] == "k" or likeCount[-1] == "K":
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
            # If no new videos are loaded, break out to avoid an infinite loop.
            prev_count = len(linkSet)
            prevCounts.append(prev_count)
            if count > 5 and prevCounts[-1] == prevCounts[-3]:
                break

        # Restrict the number of videos to the desired number, get highest likes sample.
        # We will construct a numpy array with likes in one column and video links in the other, and a third column with ht.
        # We will then sort the array by likes and select the top {videos_per_hashtag} videos.
        # We eill use the like map for data as it has all requisite data.
        print(f"Collected {len(linkSet)} videos for hashtag.")
        print(f"Like Map: {likeMap}")
        likeMapItems = likeMap.items()

        likeArr = np.array([[item[0], item[1], hashtag] for item in likeMapItems], dtype=object)
        print(f"Likes array: {likeArr}")
        likeArr = likeArr[likeArr[:, 1].argsort()[::-1]]
        likeArr = likeArr[:videos_per_hashtag]
        linkSet = set(likeArr[:, 0])
        likeMap: Dict[str, int] = {str(item[0]): int(item[1]) for item in likeArr}
        print(f"Likes array: {likeArr}")
        print(f"Link Set: {linkSet}")
        print(f"Like Map: {likeMap}")

        # If saveInterimData is True, save the video links to a csv.
        if saveInterimData:
            # Save the video links to a CSV file.
            # Create the directory if it does not exist
            print(f"base path: {basePath}") # Debug
            os.makedirs(os.path.dirname(basePath), exist_ok=True)
            df = pd.DataFrame(likeArr, columns=["Video Link", "Likes", "Hashtag"])
            path = f"{basePath}{hashtag}_videos.csv"
            df.to_csv(path, index=False)


        # Append the hashtag's video link set to the dictionary.
        tagDictDict[hashtag] = likeMap
        print(f"Collected {len(linkSet)} videos for hashtag.")
        # Append the hashtag's video data to the dictionary.
        tagDataDict[hashtag] = likeArr

    if not all([calcOverlap, outputData]):
        driver.quit()
        return tagDictDict

    # Get the intersection of all video links for each hashtag.
    if calcOverlap:
        overlapDict = {}
        # Get all intersections across every combination of tags.
        for comboSize in range(2, len(htList)):
            for combo in combinations(hashtags, comboSize):
                comboSet = set.intersection(*[set(tagDictDict[ht].keys()) for ht in combo])
                dictFromSet: Dict[str, int] = {key: tagDictDict[combo[0]][key] for key in comboSet}
                overlapDict["-".join(combo)] = dictFromSet
        if saveInterimData:
            # Save the overlap data to a CSV file.
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

def top_videos(tagDictDict: Dict[str, Dict[str, int]], n: int, valList: bool = True, dictify: bool = False, htDict: bool = False) -> List[str] | Dict[str, int] | Tuple[List[str], List[int]] | Tuple[List[str], Dict[str, int]] | Tuple[List[str], List[int], Dict[str, str]] | Tuple[ Dict[str, int], Dict[str, str]]:
    """

    :param tagDictDict:
    :param n:
    :return:
    """
    # Get a union of the dictionaries.
    unionDict = {}
    unionDictHT = {}
    for ht in tagDictDict:
        unionDict.update(tagDictDict[ht])
        unionDictHT.update({key: ht for key in tagDictDict[ht]})
    print(f"Union Dict: {unionDict}")
    print(f"Union Dict HT: {unionDictHT}")
    # Sort the dictionary by likes
    sortedDict = dict(sorted(unionDict.items(), key=lambda x: x[1], reverse=True)[:n])
    sortedDictHT = {key: unionDictHT[key] for key in sortedDict}
    print(f"Sorted Dict: {sortedDict}")
    print(f"Sorted Dict HT: {sortedDictHT}")
    # Return
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
    if basePath is None:
        basePath = f"outData\\{(str(dt.now()).replace(" ","_").replace(":", "-").replace(".", "_"))}\\"
        return basePath
    else:
        return basePath



if __name__ == "__main__":
    # Example hashtags and total number of top videos to select
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
        "她主宰算法"]
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
        "未来科技她定义"]
    top_n = 250

    videos = scrape_hashtags(top_n, hashtags2, saveInterimData=True, calcOverlap=False, outputData=True, sampleMult=4)
    # noinspection PyTypeChecker
    pickle.dump(videos, open("videos2.pickle", "wb"))
    print(videos)
    print("Top videos:")
    output: Tuple[Dict[str, int], Dict[str, str]] = top_videos(videos[0], top_n, dictify=True, htDict=True)
    top_vids_dict: Dict[str, int] = output[0]
    top_vids_ht_dict: Dict[str, str] = output[1]

    print(f"Top videos dict: {top_vids_dict}")
    print(f"Top videos ht dict: {top_vids_ht_dict}")
    print("vid Count: ", len(top_vids_dict))
    for i, vidKey in enumerate(top_vids_dict.keys()):
        print(f"i: {i} Video: {vidKey}, Likes: {top_vids_dict[vidKey]} Hashtag: {top_vids_ht_dict[vidKey]}")

    # To CSV columns=["hashtag", "video", "likes"]
    df: pd.DataFrame = pd.DataFrame(columns=["hashtag", "video", "likes"])
    for vidKey in top_vids_dict.keys():
        newRow = pd.Series({"hashtag": top_vids_ht_dict[vidKey], "video": vidKey, "likes": top_vids_dict[vidKey]})
        df.loc[len(df)] = newRow
    print(df)
    basePath = exBasePather()
    os.makedirs(os.path.dirname(basePath), exist_ok=True)
    df.to_csv(f"{basePath}top_videos.csv", index=False)
    print("Top videos saved to CSV.")
    print("Done.")
