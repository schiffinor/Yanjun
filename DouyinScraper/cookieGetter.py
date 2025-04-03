from selenium import webdriver
import pickle
from http.cookiejar import MozillaCookieJar
from typing import Dict, Any, List


class CookieGetter:

    @staticmethod
    def load(filePath: str) -> List[Dict[str, Any]]:
        mozillaJar = MozillaCookieJar()
        mozillaJar.load(filePath)

        cookies = []

        for cookieEntry in mozillaJar:
            cookie = {
                "name": cookieEntry.name,
                "value": cookieEntry.value,
                "domain": cookieEntry.domain,
                "path": cookieEntry.path,
                "secure": cookieEntry.secure,
                "expires": cookieEntry.expires
            }
            if cookieEntry.expires is None:
                cookie.pop("expires")

            cookies.append(cookie)

        del mozillaJar
        return cookies

    @staticmethod
    def saveAsPickle(cookies: List[Dict[str,Any]] ,filePath: str = None):
        if filePath is None:
            filePath = "cookies.pickle"

        driver = webdriver.Firefox()

        driver.get("https://www.douyin.com/")

        for cookie in cookies:
            driver.add_cookie(cookie)

        with open(filePath, "wb") as f:
            # noinspection PyTypeChecker
            pickle.dump(driver.get_cookies(), f)

        driver.quit()

if __name__ == "__main__":
    cookies = CookieGetter.load("cookies.txt")
    CookieGetter.saveAsPickle(cookies)
