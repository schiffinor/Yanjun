"""
Test the scraper
"""

from douyin_tiktok_scraper.scraper import Scraper

scraper = Scraper()
print(scraper.douyin_api_headers)
"""api_url = "http://localhost/api/"

def urlFromEndpoint(endpoint: str):
    if not any((endpoint.startswith("douyin"), endpoint.startswith("/douyin"),
                endpoint.startswith("api"), endpoint.startswith("/api"),
                endpoint.startswith("tiktok"), endpoint.startswith("/tiktok"),
                endpoint.startswith("bilibili"), endpoint.startswith("/bilibili"),
                endpoint.startswith("hybrid"), endpoint.startswith("/hybrid"),
                endpoint.startswith("download"), endpoint.startswith("/download"))):
        middle = "douyin/web/" if not endpoint.startswith("/") else "douyin/web"
        return api_url + middle + endpoint
    if endpoint.startswith("/"):
        return api_url + endpoint[1:]
    return api_url + endpoint

async def fetch_aweme_id(video_url):
    async with httpx.AsyncClient() as client:
        endpoint = "get_aweme_id"
        requestURl = urlFromEndpoint(endpoint)
        response = await client.get(requestURl, params={"url": video_url})
        if response.status_code == 200:
            return response.json()
        else:
            wn.warn(f"Error: \n Response Status Code: {response.status_code} \n Response Text: {response.text}")
            return None

async def fetchVideoMetadata(video_url):
    async with httpx.AsyncClient() as client:
        endpoint = "fetch_one_video"
        requestURl = urlFromEndpoint(endpoint)
        aweme_id_response = await fetch_aweme_id(video_url)
        aweme_id = aweme_id_response["data"]
        response = await client.get(requestURl, params={"aweme_id": aweme_id})
        if response.status_code == 200:
            return response.json()
        else:
            wn.warn(f"Error: \n Response Status Code: {response.status_code} \n Response Text: {response.text}")
            return None
"""
