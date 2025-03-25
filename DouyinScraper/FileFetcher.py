"""
This python file will take a path to a csv file with a list of douyin video urls and download the videos to the specified folder.
The first column of the csv is hashtags and the second column is the video url, rest is irrelevant.

In one pass this script will:
- Download the videos
- Extract the metadata
- Save the metadata to a csv file
- Save the video to the specified folder
- Extract all the comments from the video
"""
import json
import httpx
import asyncio
from typing import Dict, Any, List
import sys
import os
import csv
import requests
from datetime import datetime
from douyin_tiktok_scraper.scraper import Scraper
import pandas as pd
import numpy as np
from datetime import datetime as dt
import aiofiles
import warnings as wn
import docker
import time
import database

def basePather(basePath: str = None):
    if basePath is None:
        basePath = f"videoData\\{(str(dt.now()).replace(" ","_").replace(":", "-").replace(".", "_"))}\\"
        return basePath
    else:
        return basePath



def check_docker_logs(container_name: str, since: int = None) -> bool:
    """
    Check the Docker logs of the specified container for error patterns.

    :param container_name: Name or ID of the container running the API.
    :param since: Optional Unix timestamp to filter logs.
    :return: True if an error pattern is detected, otherwise False.
    """
    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
        # Get logs (decode from bytes to string)
        logs = container.logs(since=since).decode("utf-8")
        # Define error patterns to check for
        error_patterns = [
            "500: An error occurred while fetching data",
            "peer closed connection",
            "清理未完成的文件"
        ]
        for pattern in error_patterns:
            if pattern in logs:
                print(f"Detected error pattern in Docker logs: {pattern}")
                return True
        return False
    except Exception as e:
        print("Error checking Docker logs:", e)
        return False

# Updated fetch_video_stream_with_retry with increased timeout and explicit ReadTimeout handling.
async def fetch_video_stream_with_retry(client: httpx.AsyncClient, request_url: str, params: dict,
                                        max_retries: int = 10, delay: int = 5) -> bytes:
    for attempt in range(max_retries):
        try:
            async with client.stream("GET", request_url, params=params) as response:
                if response.status_code == 200:
                    expected = response.headers.get("Content-Length")
                    downloaded = 0
                    chunks = []
                    async for chunk in response.aiter_bytes(chunk_size=8192):
                        downloaded += len(chunk)
                        chunks.append(chunk)
                    if expected and downloaded < int(expected):
                        raise Exception(f"Incomplete download: {downloaded} bytes (expected {expected})")
                    return b"".join(chunks)
                else:
                    print(f"Attempt {attempt+1}: Received status code {response.status_code}. Retrying in {delay} seconds...")
        except (httpx.ReadTimeout, httpx.RequestError) as e:
            print(f"Attempt {attempt+1}: Exception occurred: {e}. Retrying in {delay} seconds...")
        await asyncio.sleep(delay)
    raise Exception("Max retries reached. Video not available.")

class Fetcher:


    def __init__(self, path: str, output_folder: str = None, api_url: str = None, main_scope: str = None):
        if api_url is None:
            api_url = "http://localhost/api/"
        self.api_url = api_url
        if main_scope is None:
            main_scope = "douyin/web/"
        self.main_scope = main_scope
        self.path = path
        self.output_folder = output_folder
        self.output_folder = basePather(self.output_folder)
        os.makedirs(self.output_folder, exist_ok=True)
        self.scraper = Scraper()
        self.df = pd.read_csv(self.path, usecols=[0, 1])
        self.df.columns = ['hashtags', 'video_url']
        self.db = database.Database()

    @staticmethod
    def dataFromResponse(response: requests.Response):
        if response.status_code == 200:
            return response.json()["data"]
        else:
            wn.warn(f"Error: \n Response Status Code: {response.status_code} \n Response Text: {response.text}")
            return None

    @staticmethod
    def routerFromResponse(response: requests.Response):
        if response.status_code == 200:
            return response.json()["router"]
        else:
            wn.warn(f"Error: \n Response Status Code: {response.status_code} \n Response Text: {response.text}")
            return None

    def urlFromEndpoint(self, endpoint: str):
        if not any((endpoint.startswith("douyin"), endpoint.startswith("/douyin"),
                    endpoint.startswith("api"), endpoint.startswith("/api"),
                    endpoint.startswith("tiktok"), endpoint.startswith("/tiktok"),
                    endpoint.startswith("bilibili"), endpoint.startswith("/bilibili"),
                    endpoint.startswith("hybrid"), endpoint.startswith("/hybrid"),
                    endpoint.startswith("download"), endpoint.startswith("/download"))):
            middle = self.main_scope if not endpoint.startswith("/") else self.main_scope[:-1]
            return self.api_url + middle + endpoint
        if endpoint.startswith("/"):
            return self.api_url + endpoint[1:]
        return self.api_url + endpoint

    async def fetch_aweme_id(self, video_url:str):
        async with httpx.AsyncClient() as client:
            endpoint = "get_aweme_id"
            requestURl = self.urlFromEndpoint(endpoint)
            response = await client.get(requestURl, params={"url": video_url})
            return Fetcher.dataFromResponse(response)

    async def fetchVideoMetadata(self, aweme_id: str):
        async with httpx.AsyncClient() as client:
            endpoint = "fetch_one_video"
            requestURl = self.urlFromEndpoint(endpoint)
            response = await client.get(requestURl, params={"aweme_id": aweme_id})
            return Fetcher.dataFromResponse(response)

    async def fetchComments(self, aweme_id: str, commentCount: int = -1):
        async with httpx.AsyncClient() as client:
            endpoint = "fetch_video_comments"
            requestURl = self.urlFromEndpoint(endpoint)
            # If commentCount is -1, retrieve the comment count from video metadata.
            if commentCount == -1:
                video_data = await self.fetchVideoMetadata(aweme_id)
                if video_data is None:
                    wn.warn("Failed to fetch video metadata; returning empty comments.")
                    return {"comments": []}
                try:
                    commentCount = video_data["aweme_detail"]["statistics"]["comment_count"]
                except KeyError as e:
                    wn.warn(f"Error parsing comment count: {e}. Returning empty comments.")
                    return {"comments": []}
            # If there are zero comments, return an empty structure without making a request.
            if commentCount == 0:
                return {"comments": []}
            response = await client.get(requestURl, params={"aweme_id": aweme_id, "count": commentCount})
            return Fetcher.dataFromResponse(response)

    async def fetchCommentReplies(self, aweme_id: str, comment_id: str, replyCount: int = -1):
        # If there are zero replies, return an empty structure immediately.
        if replyCount == 0:
            return {"comments": []}

        async with httpx.AsyncClient() as client:
            endpoint = "fetch_video_comment_replies"
            requestURl = self.urlFromEndpoint(endpoint)
            if replyCount == -1:
                # First, request a small sample to learn the total reply count
                reply_count_response = await client.get(
                    requestURl,
                    params={"item_id": aweme_id, "comment_id": comment_id, "cursor": 0, "count": 20}
                )
                try:
                    replyCount = reply_count_response.json()["data"]["comments"]["0"]["comment_reply_total"]
                except Exception as e:
                    # If something goes wrong, log it and return an empty list.
                    print(f"Error retrieving reply count: {e}")
                    return {"comments": []}
                # If the reply count turns out to be 0, just return an empty result.
                if replyCount == 0:
                    return {"comments": []}
                response = await client.get(
                    requestURl,
                    params={"item_id": aweme_id, "comment_id": comment_id, "cursor": 0, "count": replyCount}
                )
            else:
                response = await client.get(
                    requestURl,
                    params={"item_id": aweme_id, "comment_id": comment_id, "cursor": 0, "count": replyCount}
                )
            return Fetcher.dataFromResponse(response)

    async def fetchVideoFile(self, video_url: str):
        async with httpx.AsyncClient() as client:
            endpoint = "download"
            requestURl = self.urlFromEndpoint(endpoint)
            response = await client.get(requestURl, params={"url": video_url, "prefix": False, "with_watermark": False})
            return response.content

    async def fetchUserHandler(self, sec_uid: str):
        async with httpx.AsyncClient() as client:
            endpoint = "handler_user_profile"
            requestURl = self.urlFromEndpoint(endpoint)
            response = await client.get(requestURl, params={"sec_uid": sec_uid})
            return Fetcher.dataFromResponse(response)

    @staticmethod
    def parseVideoMetadata(metadata: dict) -> Dict[str, Any]:
        aweme_detail = metadata["aweme_detail"]
        sec_uid = aweme_detail["author"]["sec_uid"]
        caption = aweme_detail["caption"]
        create_time = aweme_detail["create_time"]
        desc = aweme_detail["desc"]
        duration = aweme_detail["duration"]
        item_title = aweme_detail["item_title"]
        ocr_content = aweme_detail["seo_info"]["ocr_content"]
        statistics = aweme_detail["statistics"]
        admire_count = statistics["admire_count"]
        collect_count = statistics["collect_count"]
        comment_count = statistics["comment_count"]
        digg_count = statistics["digg_count"]
        play_count = statistics["play_count"]
        share_count = statistics["share_count"]
        text_extra = aweme_detail["text_extra"]
        video_tag = aweme_detail["video_tag"]
        return {"sec_uid": sec_uid, "caption": caption, "create_time": create_time, "desc": desc, "duration": duration,
                "item_title": item_title, "ocr_content": ocr_content, "admire_count": admire_count,
                "collect_count": collect_count, "comment_count": comment_count, "digg_count": digg_count,
                "play_count": play_count, "share_count": share_count, "text_extra": text_extra, "video_tag": video_tag}

    @staticmethod
    def parseCommentData(comments: Dict[str, Any]) -> List[Dict[str, Any]]:
        comment_data = []
        commentList: List[Dict[str, Any]] = comments["comments"]
        for comment in commentList:
            sec_uid = comment["user"]["sec_uid"]
            cid = comment["cid"]
            text = comment["text"]
            create_time = comment["create_time"]
            digg_count = comment["digg_count"]
            user_digged = comment["user_digged"]
            reply_comment_total = comment["reply_comment_total"]
            is_author_digged = comment["is_author_digged"]
            is_hot = comment["is_hot"]
            is_note_comment = comment["is_note_comment"]
            comment_data.append({"sec_uid": sec_uid, "cid": cid, "text": text, "create_time": create_time,
                                 "digg_count": digg_count, "user_digged": user_digged, "reply_comment_total": reply_comment_total,
                                 "is_author_digged": is_author_digged, "is_hot": is_hot, "is_note_comment": is_note_comment})
        return comment_data

    @staticmethod
    def parseReplyData(replies: Dict[str, Any]) -> List[Dict[str, Any]]:
        reply_data = []
        replyList: List[Dict[str, Any]] = replies["comments"]
        if replyList is None:
            return []
        for reply in replyList:
            sec_uid = reply["user"]["sec_uid"]
            cid = reply["cid"]
            text = reply["text"]
            create_time = reply["create_time"]
            digg_count = reply["digg_count"]
            user_digged = reply["user_digged"]
            is_author_digged = reply["is_author_digged"]
            is_hot = reply["is_hot"]
            is_note_comment = reply["is_note_comment"]
            reply_data.append({"sec_uid": sec_uid, "cid": cid, "text": text, "create_time": create_time,
                               "digg_count": digg_count, "user_digged": user_digged, "is_author_digged": is_author_digged,
                               "is_hot": is_hot, "is_note_comment": is_note_comment})
        return reply_data

    @staticmethod
    def parseHandlerData(handler: dict) -> Dict[str, Any]:
        user = handler["user"]
        account_cert_info = user["account_cert_info"]
        aweme_count = user["aweme_count"]
        city = user["city"]
        country = user["country"]
        custom_verify = user["custom_verify"]
        district = user["district"]
        follower_count = user["follower_count"]
        following_count = user["following_count"]
        forward_count = user["forward_count"]
        gender = user["gender"]
        ip_location = user["ip_location"]
        is_activity_user = user["is_activity_user"]
        is_ban = user["is_ban"]
        is_gov_media_vip = user["is_gov_media_vip"]
        is_im_oversea_user = user["is_im_oversea_user"]
        is_star = user["is_star"]
        nickname = user["nickname"]
        short_id = user["short_id"]
        signature = user["signature"]
        total_favorited = user["total_favorited"]
        uid = user["uid"]
        unique_id = user["unique_id"]
        user_age = user["user_age"]
        return {"account_cert_info": account_cert_info, "aweme_count": aweme_count, "city": city, "country": country,
                "custom_verify": custom_verify, "district": district, "follower_count": follower_count,
                "following_count": following_count, "forward_count": forward_count, "gender": gender,
                "ip_location": ip_location, "is_activity_user": is_activity_user, "is_ban": is_ban,
                "is_gov_media_vip": is_gov_media_vip, "is_im_oversea_user": is_im_oversea_user,
                "is_star": is_star, "nickname": nickname, "short_id": short_id, "signature": signature,
                "total_favorited": total_favorited, "uid": uid, "unique_id": unique_id, "user_age": user_age}

    @staticmethod
    def videoDataFormer(metadata: Dict[str, Any], comments: List[Dict[str, Any]], replies: List[List[Dict[str, Any]]], index: int = None) -> Dict[str, Any]:
        """
        Combine video metadata, comments, and replies into a single structured dictionary.

        :param metadata: A dictionary containing video metadata (expected keys include 'aweme_detail', etc.).
        :param comments: A list of dictionaries, each representing a comment.
        :param replies: A list of lists, where each inner list contains dictionaries for replies corresponding to a comment.
        :param index: An integer used to generate file paths.
        :return: A dictionary with the combined video data.
        """

        if index is None:
            raise ValueError("An index value is required to form file paths.")

        # Define file paths based on the index value
        vidPath = f"Videos\\{index}.mp4"
        jsonPath = f"Transcriptions\\json\\{index}.json"
        srtPath = f"Transcriptions\\srt\\{index}.srt"
        tsvPath = f"Transcriptions\\tsv\\{index}.tsv"
        txtPath = f"Transcriptions\\txt\\{index}.txt"
        vttPath = f"Transcriptions\\vtt\\{index}.vtt"

        # Process comments and their replies
        comment_list = []
        if len(comments) != len(replies):
            raise ValueError("The length of comments and replies must match.")

        for i, comment in enumerate(comments):
            reply_list = replies[i]
            reply_dict_list = []
            for reply in reply_list:
                reply_dict = {
                    "sec_uid": reply["sec_uid"],
                    "reply_data": {
                        "cid": reply["cid"],
                        "content": reply["text"],
                        "metadata": {
                            "temporal": {
                                "create_time": reply["create_time"]
                            },
                            "engagement": {
                                "digg_count": reply["digg_count"]
                            },
                            "reaction_flags": {
                                "user_digged": reply["user_digged"],
                                "is_author_digged": reply["is_author_digged"]
                            },
                            "comment_flags": {
                                "is_hot": reply["is_hot"],
                                "is_note_comment": reply["is_note_comment"]
                            }
                        }
                    }
                }
                reply_dict_list.append(reply_dict)

            commment_dict = {
                "sec_uid": comment["sec_uid"],
                "comment_data": {
                    "cid": comment["cid"],
                    "content": comment["text"],
                    "metadata": {
                        "temporal": {
                            "create_time": comment["create_time"]
                        },
                        "engagement": {
                            "digg_count": comment["digg_count"],
                            "reply_comment_total": comment["reply_comment_total"]
                        },
                        "reaction_flags": {
                            "user_digged": comment["user_digged"],
                            "is_author_digged": comment["is_author_digged"]
                        },
                        "comment_flags": {
                            "is_hot": comment["is_hot"],
                            "is_note_comment": comment["is_note_comment"]
                        }
                    },
                    "replies": reply_dict_list
                }
            }
            comment_list.append(commment_dict)

        vidDataObj = {
            "file_folder": {
                "video_file": vidPath,
                "transcripts": {
                    "json": jsonPath,
                    "srt": srtPath,
                    "tsv": tsvPath,
                    "txt": txtPath,
                    "vtt": vttPath
                }
            },
            "metadata": {
                "sec_uid": metadata["sec_uid"],
                "video_metadata": {
                    "primary": {
                        "item_title": metadata["item_title"],
                        "caption": metadata["caption"],
                        "desc": metadata["desc"]
                    },
                    "basic_temporal": {
                        "create_time": metadata["create_time"],
                        "duration": metadata["duration"]
                    },
                    "engagement": {
                        "play_count": metadata["play_count"],
                        "digg_count": metadata["digg_count"],
                        "comment_count": metadata["comment_count"],
                        "share_count": metadata["share_count"],
                        "admire_count": metadata["admire_count"],
                        "collect_count": metadata["collect_count"]
                    },
                    "additional": {
                        "ocr_content": metadata["ocr_content"],
                        "text_extra": metadata["text_extra"],
                        "video_tag": metadata["video_tag"]
                    }
                }
            },
            "comments": comment_list
        }
        return vidDataObj

    @staticmethod
    def userDictFormer(sec_id: str, handler: Dict[str, Any], videoList: List[Dict[str, Any]], commentList: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine user metadata, video data, and comment data into a single structured dictionary.

        :param sec_id: A string representing the user's sec_uid.
        :param handler: A dictionary containing user metadata (expected keys include 'user', etc.).
        :param videoList: A list of dictionaries, each representing a video.
        :param commentList: A list of dictionaries, each representing a comment.
        :return: A dictionary with the combined user data.
        """
        userDataObj = {
            "sec_uid": sec_id,
            "user_data": {
                "personal_info": {
                    "nickname": handler["nickname"],
                    "short_id": handler["short_id"],
                    "signature": handler["signature"],
                    "user_age": handler["user_age"],
                    "gender": handler["gender"]
                },
                "location_and_verification": {
                    "city": handler["city"],
                    "district": handler["district"],
                    "country": handler["country"],
                    "ip_location": handler["ip_location"],
                    "account_cert_info": handler["account_cert_info"],
                    "custom_verify": handler["custom_verify"]
                },
                "statistics": {
                    "video_count": handler["aweme_count"],
                    "follower_count": handler["follower_count"],
                    "following_count": handler["following_count"],
                    "forward_count": handler["forward_count"],
                    "total_favorited": handler
                },
                "flags": {
                    "is_activity_user": handler["is_activity_user"],
                    "is_ban": handler["is_ban"],
                    "is_gov_media_vip": handler["is_gov_media_vip"],
                    "is_im_oversea_user": handler["is_im_oversea_user"],
                    "is_star": handler["is_star"]
                },
                "identifiers": {
                    "uid": handler["uid"],
                    "unique_id": handler["unique_id"]
                }
            },
            "videos": videoList,
            "comments": commentList
        }
        return userDataObj




    async def newDownload(self, video_url: str, index: int, output_folder: str = None) -> str:
        """
        Download the video, ensuring the file is fully saved before finishing.
        Checks Docker logs for errors to ensure the download was successful.

        :param video_url: URL of the video to download.
        :param index: An integer used to generate a filename.
        :param output_folder: Directory to save the video file.
        :return: The full file path of the saved video.
        """
        if output_folder is None:
            output_folder = basePather(None)
        else:
            output_folder = basePather(output_folder)
        os.makedirs(output_folder, exist_ok=True)

        # Record the current timestamp for Docker log filtering.
        sinceTimestamp = int(time.time())

        # Increase the timeout for large video downloads.
        timeout = httpx.Timeout(300.0)  # 300 seconds
        async with httpx.AsyncClient(timeout=timeout) as client:
            endpoint = "download"
            request_url = self.urlFromEndpoint(endpoint)
            params = {"url": video_url, "prefix": False, "with_watermark": False}
            video_content = await fetch_video_stream_with_retry(client, request_url, params)

        file_path = os.path.join(output_folder, f"{index}.mp4")
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(video_content)

        # Wait until the file exists and has nonzero size
        for _ in range(10):
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                break
            await asyncio.sleep(0.5)

        # Check Docker logs for errors using the helper function.
        container_name = "douyin_tiktok_api"  # Update to your container's name
        try:
            if check_docker_logs(container_name, since=sinceTimestamp):
                raise RuntimeError(f"Errors detected in Docker logs for video {video_url}")
        except RuntimeError as e:
            print(f"Error checking Docker logs: {e}")
            # Optionally, you can retry the download.
            return await self.newDownload(video_url, index, output_folder)

        print(f"Downloaded video successfully: {video_url}")
        return file_path

    async def fetch(self, dl: bool = True):
        # check if pickle exists for userSet, userVideoDict, userCommentDict if so load them, else create them
        userPickleBasePath = "userInfoPickles"
        userSetPickPath = os.path.join(userPickleBasePath, "userSet.pickle")
        userVideoDictPickPath = os.path.join(userPickleBasePath, "userVideoDict.pickle")
        userCommentDictPickPath = os.path.join(userPickleBasePath, "userCommentDict.pickle")
        if os.path.exists(userSetPickPath) and os.path.exists(userVideoDictPickPath) and os.path.exists(userCommentDictPickPath):
            userSet = pd.read_pickle(userSetPickPath)
            userVideoDict = pd.read_pickle(userVideoDictPickPath)
            userCommentDict = pd.read_pickle(userCommentDictPickPath)
        else:
            userSet = set()
            userVideoDict = {}
            userCommentDict = {}

        def userAdding(sec_uid: str, isAuthor: bool, awe_id: str, cid: str = None):
            """
            Add the sec_uid to the userSet if it is not already present.

            :param sec_uid:
            :param isAuthor:
            :param awe_id:
            :param cid:
            :return:
            """
            if sec_uid not in userSet:
                userSet.add(sec_uid)
                userVideoDict[sec_uid] = []
                userCommentDict[sec_uid] = []
            if isAuthor:
                userVideoDict[sec_uid].append({"aweme_id": awe_id})
            else:
                if cid is not None:
                    userCommentDict[sec_uid].append({"aweme_id": awe_id, "comment_id": cid})
                else:
                    raise ValueError("Comment ID is required for non-author entries.")


        for index, row in self.df.iterrows():
            video_url = row['video_url']
            hashtags = row['hashtags']
            if dl:
                pass
            # Fetch the aweme_id
            aweme_id = await self.fetch_aweme_id(video_url)
            # Fetch the video metadata
            metadata = Fetcher.parseVideoMetadata(await self.fetchVideoMetadata(aweme_id))
            author_sec_uid = metadata["sec_uid"]
            # add sec_uid to userSet
            userAdding(author_sec_uid, True, aweme_id)
            # Fetch comment count
            comment_count = metadata["comment_count"]
            # Fetch comments
            comments = Fetcher.parseCommentData(await self.fetchComments(aweme_id, comment_count))
            replyList = []
            for comment in comments:
                sec_uid = comment["sec_uid"]
                # fetch cid
                cid = comment["cid"]
                userAdding(sec_uid, False, aweme_id, cid)
                # Fetch reply count
                reply_count = comment["reply_comment_total"]
                # print(f"Reply count: {reply_count}")
                # Fetch replies
                replies = Fetcher.parseReplyData(await self.fetchCommentReplies(aweme_id, cid, reply_count))
                replyList.append(replies)
                # Fetch and store reply_sec_uid
                for reply in replies:
                    rep_sec_uid = reply["sec_uid"]
                    userAdding(rep_sec_uid, False, aweme_id, cid)
            # Determine current database index
            current_index = self.db.videoSize()
            # Form video data object
            video_data = Fetcher.videoDataFormer(metadata, comments, replyList, current_index)
            # Save video data to the database
            self.db.new_video(video_data, search_query=hashtags)

        # Save user data to the database
        for sec_uid in userSet:
            handler = Fetcher.parseHandlerData(asyncio.run(self.fetchUserHandler(sec_uid)))
            videoList = userVideoDict[sec_uid]
            commentList = userCommentDict[sec_uid]
            userData = Fetcher.userDictFormer(sec_uid, handler, videoList, commentList)
            self.db.new_user(userData)

        # Save userSet, userVideoDict, userCommentDict to pickle
        pd.to_pickle(userSet, userSetPickPath)
        pd.to_pickle(userVideoDict, userVideoDictPickPath)
        pd.to_pickle(userCommentDict, userCommentDictPickPath)








# Example usage in an async main
async def main():
    fetcher = Fetcher("videos_trim_1.csv", "output_folder")
    urls = [
        "https://www.douyin.com/video/7479004329018445097",
        "https://www.douyin.com/video/7090511186864639263",
        "https://www.douyin.com/video/7385467974053858611",
        "https://www.douyin.com/video/7452274003701517580",
        "https://www.douyin.com/video/7476213134378667316"
    ]
    indices = [13, 17, 26, 31, 50]
    for url, index in zip(urls, indices):
        try:
            file_path = await fetcher.newDownload(url, index)
            print(f"File saved to: {file_path}")
        except Exception as e:
            print(f"Failed to download video {url}: {e}")

if __name__ == '__main__':
    # asyncio.run(main())

    # asyncio.run(quickDownloadFromCSV())
    """fetcher = Fetcher("videos_trim_1.csv", "output_folder")
    aweme_id = asyncio.run(fetcher.fetch_aweme_id("https://www.douyin.com/video/7478975202127301942"))
    refData = asyncio.run(fetcher.fetchVideoMetadata(aweme_id))
    print(refData)
    refData2 = asyncio.run(fetcher.fetchComments(aweme_id))
    print(refData2)
    # Test video download
    video_file = asyncio.run(fetcher.fetchVideoFile("https://www.douyin.com/video/7478975202127301942"))
    with open("test.mp4", "wb") as f:
        f.write(video_file)
    print("Video downloaded")"""
    # asyncio.run(quickDownloadFromCSV())

    fetcher = Fetcher("videos_trim_2.csv", "output_folder")
    fetcher.db.db.drop_tables()
    asyncio.run(fetcher.fetch())
