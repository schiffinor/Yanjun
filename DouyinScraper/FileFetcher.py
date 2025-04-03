"""
FileFetcher.py
=======================

Overview:
---------
This module is designed to process a CSV file containing Douyin video URLs and associated hashtags,
download the corresponding videos, extract metadata and comments, and then export the gathered data
into user-friendly formats such as CSV or Excel files. In essence, it automates the entire process of
fetching video content and related data from Douyin, making it accessible even for users with very
limited technical or computer science background.

Purpose:
--------
The primary goal of this file is to simplify the task of retrieving Douyin video data. It reads a CSV
file where the first column contains hashtags and the second column holds video URLs. For each video,
the script performs the following actions:
  - Downloads the video file.
  - Extracts video metadata (e.g., title, description, play count, etc.).
  - Extracts comments and any associated replies.
  - Saves the metadata into a CSV or Excel file.
  - Organizes the downloaded video files into a specified folder.

By automating these steps, the module helps non-technical users gather and analyze video data with minimal
manual intervention.

Key Features:
-------------
- **CSV Input Handling**: Reads a CSV file with video URLs and hashtags, ignoring any irrelevant columns.
- **Asynchronous Networking**: Utilizes asynchronous HTTP requests (with the httpx library) for
  efficient network communication and robust error handling, ensuring smooth downloads even on slow
  connections.
- **Docker Integration**: Interfaces with Docker to check container logs and restart the container if
  necessary. This helps in maintaining a reliable connection with the Douyin API.
- **Retry Mechanisms**: Implements sophisticated retry logic with exponential backoff for both video
  download requests and API GET requests, ensuring that transient network issues do not interrupt the
  overall process.
- **Data Parsing and Transformation**: Provides helper functions to parse video metadata, comments,
  and replies into structured dictionaries. These data structures are then used to create unified objects
  for videos and users.
- **File Path Management**: Contains utility functions (such as `basePather` and `downloadVideoPath`)
  that dynamically generate file paths based on the current timestamp or index, making it easy to organize
  downloaded files.
- **Data Export**: Uses pandas to compile metadata into DataFrames and export them as CSV or Excel files,
  making the output easy to view and share.
- **User Data Aggregation**: Aggregates user-related data (videos and comments) and updates the local
  database accordingly, ensuring that user records are kept up-to-date.

Usage:
------
A typical workflow using this module might look like this:
1. Instantiate a `Fetcher` object by providing the path to a CSV file and an output folder.
2. Call the asynchronous `fetch` method to process all videos listed in the CSV.
3. The module will download videos, fetch their metadata, extract comments and replies, and update
   the local database.
4. Optionally, export metadata to CSV/Excel files for further analysis.

Example:
    import asyncio
    from douyin_video_fetcher import Fetcher

    async def main():
        # Create a Fetcher instance with the CSV file and desired output folder.
        fetcher = Fetcher("videos_trim_1.csv", "output_folder")
        # Begin processing the videos (download, fetch metadata, comments, etc.).
        await fetcher.fetch(dl=True, collect_replies=True, collect_commenter_data=True)

    asyncio.run(main())

Dependencies:
-------------
This module leverages several external libraries and modules:
  - **httpx**: For making asynchronous HTTP requests.
  - **asyncio**: To manage asynchronous operations.
  - **aiofiles**: For asynchronous file I/O operations.
  - **pandas**: For data manipulation and exporting data to CSV/Excel.
  - **docker**: To interact with Docker containers (used for monitoring and restarting the Douyin API container).
  - **requests**: For synchronous HTTP requests (used in some helper functions).
  - **yaml**: For reading and writing YAML configuration files.
  - **shutil, pathlib, os, re, time, io**: For various file system operations, regular expressions, and utility functions.
  - **database**: A local module that handles the JSON database using TinyDB.

Technical Details:
------------------
- The module is structured with multiple helper functions for file path management, Docker client interaction,
  and robust network request handling (including retries and timeouts).
- Asynchronous functions such as `fetch_ttwid`, `fetchVideoMetadata`, `fetchComments`, and others are used to
  retrieve data from the Douyin API reliably.
- The `Fetcher` class encapsulates the entire workflow, from reading the input CSV file and downloading videos
  to parsing complex data structures (metadata, comments, replies) and updating the local database.
- Extensive error handling is in place, including checking Docker container logs for errors and automatically
  restarting the container when necessary.
- The code is written to be verbose in logging and comments, ensuring that even users with limited CS knowledge
  can follow along and understand what each part of the program is doing.

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

import io
import httpx
import asyncio
from typing import Dict, Any, List, Optional
import os
import requests
from douyin_tiktok_scraper.scraper import Scraper
import pandas as pd
from datetime import datetime as dt
import aiofiles
import warnings as wn
import docker
from docker import errors as de
import time
import database
import re
import shutil
import yaml
import pathlib


def basePather(basePath: str = None):
    """
    Generate a base directory path for saving video data.

    If no base path is provided, this function creates a new path string that includes the current
    date and time (with spaces and colons replaced for file system compatibility). Otherwise, it simply
    returns the given basePath.

    :param basePath: Optional; the directory path provided by the user.
    :return: A string representing the base directory path.
    """
    # If no path is provided, generate one using the current timestamp.
    if basePath is None:
        # Create a folder name based on the current date and time.
        # Note: This replaces spaces, colons, and periods with characters safe for file names.
        basePath = f"videoData\\{(str(dt.now()).replace(" ", "_").replace(":", "-").replace(".", "_"))}\\"
        return basePath
    else:
        # If a base path is provided, return it unchanged.
        return basePath


def downloadVideoPath(index: int, basePath: str = None) -> str:
    """
    Construct a file path for a video file based on an index and an optional base path.

    :param index: The index of the video, used to uniquely name the video file.
    :param basePath: Optional; the base directory where the video should be stored.
    :return: A string representing the full path to the video file.
    """
    # If no base path is provided, set a default path using the Database folder.
    if basePath is None:
        # Determine the default database directory (one level up from this file's directory).
        db_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.joinpath("Database")
        os.makedirs(db_path, exist_ok=True)  # Ensure the directory exists.
        # We then return a path string constructed with the index and default video file name.
        return str(db_path.joinpath("Videos").joinpath(f"{index}.mp4"))
    else:
        # When a base path is provided, build the path by appending the video file name.
        return f"{basePath}\\Videos\\{index}.mp4"


def get_docker_client():
    """
    Obtain a Docker client object to interact with Docker on the host machine.

    :return: A Docker client instance if successful; otherwise, None.
    """
    try:
        # Attempt to get a Docker client from the environment.
        client = docker.from_env()
        return client
    except Exception as e:
        # Print the error if Docker is not reachable.
        print("Error connecting to Docker:", e)
        return None


def get_container_by_name(container_name: str):
    """
    Retrieve a Docker container object based on its name.

    :param container_name: The name of the Docker container.
    :return: The Docker container instance if found; otherwise, None.
    """
    # First, get the Docker client.
    client = get_docker_client()
    if client is None:
        return None
    try:
        # Attempt to get the container by name.
        container = client.containers.get(container_name)
        return container
    except de.NotFound:
        # If the container isn't found, notify the user.
        print(f"Container '{container_name}' not found.")
        return None


def get_docker_logs(container_name: str, since: int = None) -> str:
    """
    Retrieve the logs from a Docker container, optionally filtering logs by a timestamp.

    :param container_name: The name of the container to fetch logs from.
    :param since: Optional; a Unix timestamp to filter logs.
    :return: A string containing the Docker logs.
    """
    logs = ""
    try:
        # Get the container object.
        container = get_container_by_name(container_name)
        # Retrieve and decode the logs from bytes to a UTF-8 string.
        logs = container.logs(since=since).decode("utf-8")
        return logs
    except Exception as e:
        # Print any error encountered during log retrieval.
        print("Error checking Docker logs:", e)
        return logs


def check_docker_logs(container_name: str, since: int = None, tail: int = None) -> bool:
    """
    Check the Docker logs for specific error patterns to determine if there is a problem.

    :param container_name: The name or ID of the container to check.
    :param since: Optional; Unix timestamp to start checking logs from.
    :param tail: Optional; the number of lines from the end of the logs to check.
    :return: True if any error pattern is found; otherwise, False.
    """
    # Define a list of error patterns to look for in the logs.
    error_patterns = [
        "500: An error occurred while fetching data",
        "peer closed connection",
        "清理未完成的文件",
        "WARNING  第 1 次响应内容为空, 状态码: 200,"
        "WARNING  第 2 次响应内容为空, 状态码: 200,",
        "WARNING  第 3 次响应内容为空, 状态码: 200,",
        "程序出现异常，请检查错误信息。",
        "ERROR    无效响应类型。响应类型: <class 'NoneType'>"
    ]
    # Delegate to the helper function that checks logs for given error patterns.
    return check_docker_logs_for_errorPattern(error_patterns, since=since, tail=tail)


def check_docker_logs_for_ttwid_update_error():
    """
    Check Docker logs specifically for errors related to ttwid update issues.

    :return: True if an error pattern related to ttwid update is detected; otherwise, False.
    """
    # Define error patterns specific to ttwid update errors.
    error_patterns = [
        "WARNING  第 1 次响应内容为空, 状态码: 200,"
        "WARNING  第 2 次响应内容为空, 状态码: 200,",
        "WARNING  第 3 次响应内容为空, 状态码: 200,",
        "ERROR    无效响应类型。响应类型: <class 'NoneType'>",
        "400 Bad Request"
    ]
    return check_docker_logs_for_errorPattern(error_patterns, since=None, tail=None)


def check_docker_logs_for_ttwid_update_error2():
    """
    Check Docker logs for ttwid update errors with a different tail length for log retrieval.

    :return: True if an error pattern is detected; otherwise, False.
    """
    # This variant checks the last 25 lines of logs.
    error_patterns = [
        "WARNING  第 1 次响应内容为空, 状态码: 200,"
        "WARNING  第 2 次响应内容为空, 状态码: 200,",
        "WARNING  第 3 次响应内容为空, 状态码: 200,",
        "ERROR    无效响应类型。响应类型: <class 'NoneType'>",
        "400 Bad Request"
    ]
    return check_docker_logs_for_errorPattern(error_patterns, since=None, tail=25)


def check_docker_logs_strict():
    """
    Strictly check Docker logs for critical ttwid update errors.

    :return: True if critical error patterns are found; otherwise, False.
    """
    # This function looks for a set of very specific error messages.
    error_patterns = [
        "WARNING  第 1 次响应内容为空, 状态码: 200,",
        "WARNING  第 2 次响应内容为空, 状态码: 200,",
        "WARNING  第 3 次响应内容为空, 状态码: 200,",
        "ERROR    无效响应类型。响应类型: <class 'NoneType'>",
        "<class 'NoneType'>"
    ]
    return check_docker_logs_for_errorPattern(error_patterns, since=None, tail=25)


def check_docker_logs_for_errorPattern(errorPattern: List[str], since: int = None, tail: int = None) -> bool:
    """
    Check Docker logs for any occurrence of the specified error patterns.

    :param errorPattern: A list of strings representing error patterns to search for in the logs.
    :param since: Optional; Unix timestamp to filter logs.
    :param tail: Optional; number of lines from the end of the logs to consider.
    :return: True if any of the error patterns is found; otherwise, False.
    """
    try:
        # The container name is hardcoded here; update as needed.
        container_name = "douyin_tiktok_api"
        container = get_container_by_name(container_name)
        # Retrieve and decode the logs.
        logs = container.logs(since=since, tail=tail).decode("utf-8")
        # Loop through each error pattern and check if it is present in the logs.
        for pattern in errorPattern:
            if pattern in logs:
                print(f"Detected ttwid update error in Docker logs: {pattern}")
                return True
        return False
    except Exception as e:
        # If an error occurs while checking logs, print the error and return False.
        print("Error checking Docker logs:", e)
        return False


async def fetch_ttwid():
    """
    Asynchronously fetch the ttwid value from the Douyin API.

    :return: The ttwid value extracted from the API response.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
        # Define the API endpoint for generating ttwid.
        requestURL = "http://localhost/api/douyin/web/generate_ttwid"
        # Use the get_with_retry helper to fetch the response.
        response = await get_with_retry(client, requestURL, params={})
        # Extract and return the ttwid from the response data.
        return Fetcher.dataFromResponse(response)["ttwid"]


async def fetch_s_v_web_id():
    """
    Asynchronously fetch the s_v_web_id value from the Douyin API.

    :return: The s_v_web_id value extracted from the API response.
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
        requestURL = "http://localhost/api/douyin/web/generate_s_v_web_id"
        response = await get_with_retry(client, requestURL, params={})
        return Fetcher.dataFromResponse(response)["s_v_web_id"]


async def docker_restart(client: httpx.AsyncClient,
                         request_url: str,
                         params: dict,
                         max_retries: int = 5,
                         delay: int = 5,
                         backoff_factor: float = 2.0,
                         restarts: int = 0,
                         since: int = None) -> Optional[httpx.Response]:
    """
    Restart the Docker container if errors are detected in its logs, then reattempt the API request.

    :param client: An instance of httpx.AsyncClient used for making API requests.
    :param request_url: The URL of the API endpoint to retry.
    :param params: Dictionary of parameters to send with the API request.
    :param max_retries: The maximum number of retries allowed.
    :param delay: The initial delay between retries (in seconds).
    :param backoff_factor: The factor by which the delay increases after each retry.
    :param restarts: The current count of container restarts.
    :param since: Optional; a Unix timestamp to filter the Docker logs.
    :return: An httpx.Response object if the API request eventually succeeds.
    """
    container_name = "douyin_tiktok_api"  # This is the container we monitor.
    sinceTimestamp = since
    try:
        # Retrieve Docker logs since the specified timestamp.
        dLogs = get_docker_logs(container_name, since=sinceTimestamp)
        # Print the last line of the logs to help diagnose the issue.
        print(dLogs.splitlines()[-1])
        # If error patterns are found, proceed to restart the container.
        if check_docker_logs(container_name, since=sinceTimestamp) or (check_docker_logs_for_ttwid_update_error2()):
            print(
                "Error detected in Docker logs. \nCheck if error is of type: \nERROR    无效响应类型。响应类型: <class 'NoneType'>")
            if check_docker_logs_for_ttwid_update_error2():
                print("Detected specific ttwid update error in Docker logs.")
                print("Initiating update and restart of the container.")
                container = get_container_by_name(container_name)
                config_file_path = "/app/crawlers/douyin/web/config.yaml"
                # Retrieve the config file as a tar archive from the container.
                docker_file_stream, stat = container.get_archive(config_file_path)
                os.makedirs("docker_config_tar", exist_ok=True)
                with open("docker_config_tar/config.tar", "wb") as f:
                    for chunk in docker_file_stream:
                        f.write(chunk)
                # Unpack the tar archive to access the config.yaml file.
                shutil.unpack_archive("docker_config_tar/config.tar", "docker_config", "tar")
                # Open and read the configuration file.
                with open("docker_config/config.yaml", "r", encoding="utf-8") as f:
                    print("Reading config.yaml file...")
                    yamlLoad = yaml.safe_load(f)
                # The configuration contains a Cookie header with ttwid and s_v_web_id.
                cookieData = yamlLoad["TokenManager"]["douyin"]["headers"]["Cookie"]
                new_line = cookieData
                # Use regex to locate the current ttwid and s_v_web_id values.
                ttwid_regex = r"ttwid=[^;]+"
                s_v_web_id_regex = r"s_v_web_id=[^;]+"
                ttwid_match = re.search(ttwid_regex, cookieData)
                s_v_web_id_match = re.search(s_v_web_id_regex, cookieData)
                if ttwid_match:
                    old_ttwid = ttwid_match.group(0)
                    new_ttwid = await fetch_ttwid()
                    new_line = new_line.replace(old_ttwid, f"ttwid={new_ttwid}")
                if s_v_web_id_match:
                    old_s_v_web_id = s_v_web_id_match.group(0)
                    new_s_v_web_id = await fetch_s_v_web_id()
                    new_line = new_line.replace(old_s_v_web_id, f"s_v_web_id={new_s_v_web_id}")
                # Update the Cookie header in the configuration.
                yamlLoad["TokenManager"]["douyin"]["headers"]["Cookie"] = new_line
                # Write the updated configuration back to the file.
                with io.open("docker_config/config.yaml", "w", encoding="utf-8") as f:
                    yaml.safe_dump(yamlLoad, f, allow_unicode=True, default_flow_style=False)
                # Create a tar archive of the updated config file.
                tarPath = shutil.make_archive("docker_config/config", "tar", root_dir="docker_config",
                                              base_dir="config.yaml")
                with open(tarPath, "rb") as f:
                    tar_bytes = f.read()
                config_file_path_dir = config_file_path.rsplit("/", 1)[0]
                print(f"Uploading {tarPath} to {config_file_path_dir}...")
                filePut = container.put_archive(config_file_path_dir, tar_bytes)
                if filePut:
                    print("File updated in container.")
                # Restart the container and wait until it is running.
                timestamp2 = int(time.time())
                container.restart()
                print("Container restarted.")
                while True:
                    container.reload()
                    if container.status == "running":
                        print("Container is running.")
                        # Check if the container logs indicate that the application has started.
                        if "Application startup complete." in container.logs(since=timestamp2, tail=2).decode("utf-8"):
                            print("Application startup complete.")
                            await asyncio.sleep(3)
                            break
                    await asyncio.sleep(5)
                restarts += 1
                print(f"Restart count: {restarts}")
                print("Restarting the request...")
                # After the container has restarted, reattempt the API request.
                return await get_with_retry(client, request_url, params, max_retries=max_retries, delay=delay,
                                            backoff_factor=backoff_factor, restarts=restarts)
    except de.NotFound:
        print(f"Container '{container_name}' not found.")
    # Additional exception handling could be added here if desired.


# Updated fetch_video_stream_with_retry with increased timeout and explicit ReadTimeout handling.
async def fetch_video_stream_with_retry(client: httpx.AsyncClient,
                                        request_url: str,
                                        params: dict,
                                        max_retries: int = 10,
                                        delay: int = 5,
                                        backoff_factor: float = 2.0,
                                        restarts: int = 0) -> bytes:
    """
    Download video content as a stream with retry logic and exponential backoff.

    This function attempts to stream a video file from the provided URL. It retries the download if
    any issues occur (like timeouts or incomplete downloads) and checks Docker logs to determine if a
    container restart is necessary.

    :param client: An httpx.AsyncClient instance used to perform the HTTP request.
    :param request_url: The URL of the video download endpoint.
    :param params: A dictionary of parameters to pass with the request.
    :param max_retries: The maximum number of retry attempts.
    :param delay: The initial delay (in seconds) between retry attempts.
    :param backoff_factor: The multiplier to increase the delay after each failed attempt.
    :param restarts: The current count of container restarts.
    :return: The complete video content as bytes.
    :raises RuntimeWarning: If the maximum number of retries is reached.
    """
    try:
        sinceTimestamp = int(time.time())
        restart_count = restarts
        if restart_count > 5:
            print("Restart count exceeded. Exiting.")
            raise RuntimeWarning("Max restarts reached for video stream request")
        try:
            current_delay = delay
            # Attempt the download for a maximum number of retries.
            for attempt in range(max_retries):
                try:
                    sinceTimestamp = int(time.time())
                    # Use the client to stream the video content.
                    async with client.stream("GET", request_url, params=params) as response:
                        if response.status_code == 200:
                            expected = response.headers.get("Content-Length")
                            downloaded = 0
                            chunks = []
                            # Read the video stream in chunks.
                            async for chunk in response.aiter_bytes(chunk_size=8192):
                                downloaded += len(chunk)
                                chunks.append(chunk)
                            # Verify that the download is complete.
                            if expected and downloaded < int(expected):
                                raise Exception(f"Incomplete download: {downloaded} bytes (expected {expected})")
                            return b"".join(chunks)
                        else:
                            print(
                                f"Attempt {attempt + 1}: Received status code {response.status_code}. Retrying in {current_delay} seconds...")
                except (httpx.ReadTimeout, httpx.RequestError, Exception) as e:
                    print(f"Attempt {attempt + 1}: Exception occurred: {e}. Retrying in {current_delay} seconds...")
                await asyncio.sleep(current_delay)
                current_delay *= backoff_factor
                # If critical errors are detected in Docker logs, trigger a container restart.
                if check_docker_logs_strict():
                    await docker_restart(client, request_url, params, max_retries=max_retries, delay=delay,
                                         backoff_factor=backoff_factor, restarts=restart_count, since=sinceTimestamp)
            restart_count = 0
            raise RuntimeWarning("Max retries reached for video stream request")
        except RuntimeWarning as e:
            print(f"Error during video stream request: {e}")
            await docker_restart(client, request_url, params, max_retries=max_retries, delay=delay,
                                 backoff_factor=backoff_factor, restarts=restart_count, since=sinceTimestamp)
            await asyncio.sleep(600)
            try:
                await client.aclose()
            except Exception as e:
                print(f"Error closing client: {e}")
            del client
            client = httpx.AsyncClient(timeout=httpx.Timeout(9000.0))
            return await fetch_video_stream_with_retry(client, request_url, params, max_retries=max_retries,
                                                       delay=delay, backoff_factor=backoff_factor)
    except Exception as e:
        print(f"Exception: {e}")
        print(
            "This should only happen if 'NoneType' object has no attribute 'status_code'. \nThus, waiting for 10 minutes before retrying.")
        responseOut = None
        while responseOut is None:
            await asyncio.sleep(600)
            try:
                await client.aclose()
            except Exception as e:
                print(f"Error closing client: {e}")
            del client
            client = httpx.AsyncClient(timeout=httpx.Timeout(9000.0))
            responseOut = await fetch_video_stream_with_retry(client, request_url, params, max_retries=max_retries,
                                                              delay=delay, backoff_factor=backoff_factor)
            if responseOut is not None:
                break
            else:
                wn.warn("Response is None, retrying...")
        return responseOut


async def get_with_retry(client: httpx.AsyncClient,
                         request_url: str,
                         params: dict = None,
                         max_retries: int = 5,
                         delay: int = 5,
                         backoff_factor: float = 2.0,
                         restarts: int = 0) -> httpx.Response:
    """
    Send an HTTP GET request with retry logic and exponential backoff.

    If the request fails due to timeouts or non-200 status codes, the function will retry the request.
    It also checks Docker logs and may trigger a container restart if critical errors are detected.

    :param client: An instance of httpx.AsyncClient used to make the GET request.
    :param request_url: The URL to send the GET request to.
    :param params: Optional; a dictionary of query parameters for the request.
    :param max_retries: Maximum number of retry attempts.
    :param delay: Initial delay (in seconds) between retry attempts.
    :param backoff_factor: The factor by which the delay increases after each attempt.
    :param restarts: The current count of container restarts.
    :return: An httpx.Response object containing the API response.
    :raises RuntimeWarning: If the maximum number of retries is reached.
    """
    try:
        sinceTimestamp = int(time.time())
        restart_count = restarts
        if restart_count > 5:
            print("Restart count exceeded. Exiting.")
            raise RuntimeWarning("Max restarts reached for GET request")
        try:
            current_delay = delay
            # Attempt the GET request for a maximum number of retries.
            for attempt in range(max_retries):
                try:
                    sinceTimestamp = int(time.time())
                    response = await client.get(request_url, params=params)
                    if response.status_code == 200:
                        return response
                    else:
                        print(
                            f"Attempt {attempt + 1}: Received status code {response.status_code}. Retrying in {current_delay} seconds...")
                except (httpx.ReadTimeout, httpx.RequestError) as e:
                    print(f"Attempt {attempt + 1}: Exception occurred: {e}. Retrying in {current_delay} seconds...")
                await asyncio.sleep(current_delay)
                current_delay *= backoff_factor
                if check_docker_logs_strict():
                    await docker_restart(client, request_url, params, max_retries=max_retries, delay=delay,
                                         backoff_factor=backoff_factor, restarts=restart_count, since=sinceTimestamp)
            restart_count = 0
            raise RuntimeWarning("Max retries reached for GET request")
        except RuntimeWarning as e:
            print(f"Error during GET request: {e}")
            await docker_restart(client, request_url, params, max_retries=max_retries, delay=delay,
                                 backoff_factor=backoff_factor, restarts=restart_count, since=sinceTimestamp)
            await asyncio.sleep(600)
            try:
                await client.aclose()
            except Exception as e:
                print(f"Error closing client: {e}")
            del client
            client = httpx.AsyncClient(timeout=httpx.Timeout(9000.0))
            return await get_with_retry(client, request_url, params, max_retries=max_retries, delay=delay,
                                        backoff_factor=backoff_factor)
    except Exception as e:
        print(f"Exception: {e}")
        print(
            "This should only happen if 'NoneType' object has no attribute 'status_code'. \nThus, waiting for 10 minutes before retrying.")
        responseOut = None
        while responseOut is None:
            await asyncio.sleep(600)
            try:
                await client.aclose()
            except Exception as e:
                print(f"Error closing client: {e}")
            del client
            client = httpx.AsyncClient(timeout=httpx.Timeout(9000.0))
            responseOut = await get_with_retry(client, request_url, params, max_retries=max_retries, delay=delay,
                                               backoff_factor=backoff_factor)
            if responseOut is not None:
                break
            else:
                wn.warn("Response is None, retrying...")
        return responseOut


class Fetcher:
    """
    The Fetcher class orchestrates the process of downloading Douyin videos, extracting metadata,
    comments, and replies, and then updating the local database accordingly.

    It reads a CSV file that contains hashtags and video URLs, downloads the videos, fetches metadata,
    retrieves comments and replies via asynchronous API calls, and finally organizes and saves all the
    collected data for further processing.
    """

    def __init__(self, path: str, output_folder: str = None, api_url: str = None, main_scope: str = None):
        """
        Initialize a Fetcher instance with configuration for API endpoints, CSV input, and output directories.

        :param path: The file path to the CSV file containing Douyin video URLs and hashtags.
        :param output_folder: Optional; the folder where downloaded videos and related files will be saved.
        :param api_url: Optional; the base URL for the API. Defaults to "http://localhost/api/" if not provided.
        :param main_scope: Optional; the main API scope. Defaults to "douyin/web/" if not provided.
        :return: None
        """
        # Set default API URL if not provided.
        if api_url is None:
            api_url = "http://localhost/api/"
        self.api_url = api_url

        # Set default main scope if not provided.
        if main_scope is None:
            main_scope = "douyin/web/"
        self.main_scope = main_scope

        # Save the CSV path.
        self.path = path

        # Determine the output folder using the basePather helper function.
        self.output_folder = output_folder
        self.output_folder = basePather(self.output_folder)
        os.makedirs(self.output_folder, exist_ok=True)  # Create output folder if it doesn't exist.

        # Initialize the scraper for video processing.
        self.scraper = Scraper()

        # Read the CSV file into a pandas DataFrame, using only the first two columns.
        self.df = pd.read_csv(self.path, usecols=[0, 1])
        # Rename the columns for clarity.
        self.df.columns = ['hashtags', 'video_url']

        # Initialize the local database from the database module.
        self.db = database.Database()

    @staticmethod
    def dataFromResponse(response: requests.Response):
        """
        Extract the "data" field from an API response.

        :param response: The HTTP response received from an API call.
        :return: The JSON-parsed data if the response status is 200; otherwise, None.
        """
        if response.status_code == 200:
            return response.json()["data"]
        else:
            wn.warn(f"Error: \n Response Status Code: {response.status_code} \n Response Text: {response.text}")
            return None

    @staticmethod
    def routerFromResponse(response: requests.Response):
        """
        Extract the "router" field from an API response.

        :param response: The HTTP response from an API call.
        :return: The router information if the response status is 200; otherwise, None.
        """
        if response.status_code == 200:
            return response.json()["router"]
        else:
            wn.warn(f"Error: \n Response Status Code: {response.status_code} \n Response Text: {response.text}")
            return None

    def urlFromEndpoint(self, endpoint: str):
        """
        Construct the full API URL based on the provided endpoint and the Fetcher's configuration.

        If the endpoint does not start with common API prefixes, it is combined with the main_scope.

        :param endpoint: The API endpoint (e.g., "get_aweme_id").
        :return: A full URL string for the API request.
        """
        # Check if the endpoint already starts with a known prefix.
        if not any((endpoint.startswith("douyin"), endpoint.startswith("/douyin"),
                    endpoint.startswith("api"), endpoint.startswith("/api"),
                    endpoint.startswith("tiktok"), endpoint.startswith("/tiktok"),
                    endpoint.startswith("bilibili"), endpoint.startswith("/bilibili"),
                    endpoint.startswith("hybrid"), endpoint.startswith("/hybrid"),
                    endpoint.startswith("download"), endpoint.startswith("/download"))):
            # If not, append the main_scope in the appropriate way.
            middle = self.main_scope if not endpoint.startswith("/") else self.main_scope[:-1]
            return self.api_url + middle + endpoint
        if endpoint.startswith("/"):
            return self.api_url + endpoint[1:]
        return self.api_url + endpoint

    async def fetch_aweme_id(self, video_url: str):
        """
        Asynchronously fetch the aweme_id for a given video URL.

        :param video_url: The URL of the Douyin video.
        :return: The aweme_id extracted from the API response.
        """
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "get_aweme_id"
            requestURL = self.urlFromEndpoint(endpoint)
            # Retry the request until it succeeds.
            response = await get_with_retry(client, requestURL, {"url": video_url})
            return Fetcher.dataFromResponse(response)

    async def fetchVideoMetadata(self, aweme_id: str):
        """
        Asynchronously fetch the metadata for a video using its aweme_id.

        :param aweme_id: The unique identifier for the video.
        :return: The video metadata as extracted from the API response.
        """
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "fetch_one_video"
            requestURL = self.urlFromEndpoint(endpoint)
            response = await get_with_retry(client, requestURL, {"aweme_id": aweme_id})
            return Fetcher.dataFromResponse(response)

    async def fetchComments(self, aweme_id: str, commentCount: int = -1):
        """
        Asynchronously fetch comments for a given video.

        :param aweme_id: The video’s unique identifier.
        :param commentCount: The number of comments to fetch. If -1, the function will first determine
                             the comment count from video metadata.
        :return: A dictionary containing a list of comments.
        """
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "fetch_video_comments"
            requestURL = self.urlFromEndpoint(endpoint)
            # If commentCount is -1, determine it from video metadata.
            if commentCount == -1:
                commentCount = 50
            # If there are no comments, return an empty list.
            if commentCount == 0:
                return {"comments": []}
            response = await get_with_retry(client, requestURL, {"aweme_id": aweme_id, "count": commentCount})
            return Fetcher.dataFromResponse(response)

    async def fetchCommentReplies(self, aweme_id: str, comment_id: str, replyCount: int = -1):
        """
        Asynchronously fetch replies for a specific comment on a video.

        :param aweme_id: The unique identifier for the video.
        :param comment_id: The unique identifier for the comment.
        :param replyCount: The number of replies to fetch. If -1, a preliminary request will be made to determine the count.
        :return: A dictionary containing a list of reply dictionaries.
        """
        # If there are no replies, return an empty structure.
        if replyCount == 0:
            return {"comments": []}

        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "fetch_video_comment_replies"
            requestURL = self.urlFromEndpoint(endpoint)
            if replyCount == -1:
                # Make an initial request to determine the total number of replies.
                # First, request a small sample to learn the total reply count
                reply_count_response = await get_with_retry(client, requestURL,
                                                            params={"item_id": aweme_id, "comment_id": comment_id,
                                                                    "cursor": 0, "count": 20}
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
                response = await get_with_retry(client, requestURL,
                                                params={"item_id": aweme_id, "comment_id": comment_id, "cursor": 0,
                                                        "count": replyCount}
                                                )
            else:
                response = await get_with_retry(client, requestURL,
                                                params={"item_id": aweme_id, "comment_id": comment_id, "cursor": 0,
                                                        "count": replyCount}
                                                )
            return Fetcher.dataFromResponse(response)

    async def fetchVideoFile(self, video_url: str):
        """
        Asynchronously download the video file from the provided URL.

        :param video_url: The URL of the video to download.
        :return: The binary content of the downloaded video.
        """
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "download"
            requestURL = self.urlFromEndpoint(endpoint)
            response = await get_with_retry(client, requestURL,
                                            {"url": video_url, "prefix": False, "with_watermark": False})
            return response.content

    async def fetchUserHandler(self, sec_uid: str):
        """
        Asynchronously fetch user profile data using a user's secondary ID (sec_uid).

        :param sec_uid: The secondary user ID.
        :return: The user data extracted from the API response.
        """
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "handler_user_profile"
            requestURL = self.urlFromEndpoint(endpoint)
            response = await get_with_retry(client, requestURL, {"sec_user_id": sec_uid})
            return Fetcher.dataFromResponse(response)

    @staticmethod
    def parseVideoMetadata(metadata: dict) -> Dict[str, Any]:
        """
        Parse the raw video metadata into a structured dictionary.

        :param metadata: A dictionary containing raw video metadata.
        :return: A dictionary with key video information (e.g., aweme_id, caption, duration, etc.).
        """
        aweme_detail = metadata.get("aweme_detail", {})
        aweme_id = aweme_detail.get("aweme_id", "")
        sec_uid = aweme_detail.get("author", {}).get("sec_uid", "")
        caption = aweme_detail.get("caption", "")
        create_time = aweme_detail.get("create_time", "")
        desc = aweme_detail.get("desc", "")
        duration = aweme_detail.get("duration", "")
        item_title = aweme_detail.get("item_title", "")
        ocr_content = aweme_detail.get("seo_info", {}).get("ocr_content", "")
        statistics = aweme_detail.get("statistics", {})
        admire_count = statistics.get("admire_count", "")
        collect_count = statistics.get("collect_count", "")
        comment_count = statistics.get("comment_count", "")
        digg_count = statistics.get("digg_count", "")
        play_count = statistics.get("play_count", "")
        share_count = statistics.get("share_count", "")
        text_extra = aweme_detail.get("text_extra", [])
        video_tag = aweme_detail.get("video_tag", [])
        # Return a structured dictionary with the extracted data.
        return {"aweme_id": aweme_id, "sec_uid": sec_uid, "caption": caption, "create_time": create_time, "desc": desc,
                "duration": duration, "item_title": item_title, "ocr_content": ocr_content,
                "admire_count": admire_count,
                "collect_count": collect_count, "comment_count": comment_count, "digg_count": digg_count,
                "play_count": play_count, "share_count": share_count, "text_extra": text_extra, "video_tag": video_tag}

    @staticmethod
    def parseCommentData(comments: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse raw comment data into a list of structured comment dictionaries.

        :param comments: A dictionary containing comment data under the key "comments".
        :return: A list of dictionaries, each representing a comment with key fields.
        """
        comment_data = []
        commentList: List[Dict[str, Any]] = comments["comments"]
        # Iterate through each comment and extract relevant fields.
        for comment in commentList:
            sec_uid = comment.get("user", {}).get("sec_uid", "")
            cid = comment.get("cid", "")
            text = comment.get("text", "")
            create_time = comment.get("create_time", "")
            digg_count = comment.get("digg_count", "")
            user_digged = comment.get("user_digged", "")
            reply_comment_total = comment.get("reply_comment_total", "")
            is_author_digged = comment.get("is_author_digged", "")
            is_hot = comment.get("is_hot", "")
            is_note_comment = comment.get("is_note_comment", "")
            # Append the structured comment data.
            comment_data.append({"sec_uid": sec_uid, "cid": cid, "text": text, "create_time": create_time,
                                 "digg_count": digg_count, "user_digged": user_digged,
                                 "reply_comment_total": reply_comment_total,
                                 "is_author_digged": is_author_digged, "is_hot": is_hot,
                                 "is_note_comment": is_note_comment})
        return comment_data

    @staticmethod
    def parseReplyData(replies: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Parse raw reply data into a list of structured reply dictionaries.

        :param replies: A dictionary containing reply data under the key "comments".
        :return: A list of dictionaries, each representing a reply.
        """
        reply_data = []
        replyList: List[Dict[str, Any]] = replies["comments"]
        if replyList is None:
            return []
        # Process each reply to extract the key details.
        for reply in replyList:
            sec_uid = reply.get("user", {}).get("sec_uid", "")
            cid = reply.get("cid", "")
            text = reply.get("text", "")
            create_time = reply.get("create_time", "")
            digg_count = reply.get("digg_count", "")
            user_digged = reply.get("user_digged", "")
            is_author_digged = reply.get("is_author_digged", "")
            is_hot = reply.get("is_hot", "")
            is_note_comment = reply.get("is_note_comment", "")
            reply_data.append({"sec_uid": sec_uid, "cid": cid, "text": text, "create_time": create_time,
                               "digg_count": digg_count, "user_digged": user_digged,
                               "is_author_digged": is_author_digged,
                               "is_hot": is_hot, "is_note_comment": is_note_comment})
        return reply_data

    @staticmethod
    def parseHandlerData(handler: dict) -> Dict[str, Any]:
        """
        Parse raw user handler data into a structured dictionary of user information.

        :param handler: A dictionary containing raw user data.
        :return: A structured dictionary with user information (personal info, location, statistics, etc.).
        """
        user = handler.get("user", {})
        account_cert_info = user.get("account_cert_info", "")
        aweme_count = user.get("aweme_count", "")
        city = user.get("city", "")
        country = user.get("country", "")
        custom_verify = user.get("custom_verify", "")
        district = user.get("district", "")
        follower_count = user.get("follower_count", "")
        following_count = user.get("following_count", "")
        forward_count = user.get("forward_count", "")
        gender = user.get("gender", "")
        ip_location = user.get("ip_location", "")
        is_activity_user = user.get("is_activity_user", "")
        is_ban = user.get("is_ban", "")
        is_gov_media_vip = user.get("is_gov_media_vip", "")
        is_im_oversea_user = user.get("is_im_oversea_user", "")
        is_star = user.get("is_star", "")
        nickname = user.get("nickname", "")
        short_id = user.get("short_id", "")
        signature = user.get("signature", "")
        total_favorited = user.get("total_favorited", "")
        uid = user.get("uid", "")
        unique_id = user.get("unique_id", "")
        user_age = user.get("user_age", "")
        return {
            "account_cert_info": account_cert_info,
            "aweme_count": aweme_count,
            "city": city,
            "country": country,
            "custom_verify": custom_verify,
            "district": district,
            "follower_count": follower_count,
            "following_count": following_count,
            "forward_count": forward_count,
            "gender": gender,
            "ip_location": ip_location,
            "is_activity_user": is_activity_user,
            "is_ban": is_ban,
            "is_gov_media_vip": is_gov_media_vip,
            "is_im_oversea_user": is_im_oversea_user,
            "is_star": is_star,
            "nickname": nickname,
            "short_id": short_id,
            "signature": signature,
            "total_favorited": total_favorited,
            "uid": uid,
            "unique_id": unique_id,
            "user_age": user_age
        }

    @staticmethod
    def videoDataFormer(url: str, metadata: Dict[str, Any], comments: List[Dict[str, Any]],
                        replies: List[List[Dict[str, Any]]], index: int = None) -> Dict[str, Any]:
        """
        Combine video metadata, comments, and replies into a single structured dictionary.

        :param url: The URL of the video.
        :param metadata: A dictionary containing video metadata (e.g., aweme_id, caption, etc.).
        :param comments: A list of dictionaries, each representing a comment.
        :param replies: A list of lists where each inner list contains dictionaries representing replies for a comment.
        :param index: An integer used to generate file paths for saving the video and transcripts.
        :return: A dictionary containing the combined video data.
        :raises ValueError: If the index is not provided or if the length of comments and replies do not match.
        """
        if index is None:
            raise ValueError("An index value is required to form file paths.")

        # Generate file paths for the video and various transcript formats using the index.
        vidPath = f"Videos\\{index}.mp4"
        jsonPath = f"Transcriptions\\json\\{index}.json"
        srtPath = f"Transcriptions\\srt\\{index}.srt"
        tsvPath = f"Transcriptions\\tsv\\{index}.tsv"
        txtPath = f"Transcriptions\\txt\\{index}.txt"
        vttPath = f"Transcriptions\\vtt\\{index}.vtt"

        # Initialize an empty list to hold structured comment data.
        comment_list = []
        # Ensure that the number of comments matches the number of reply lists.
        if len(comments) != len(replies):
            raise ValueError("The length of comments and replies must match.")

        # Process each comment and its corresponding replies.
        for i, comment in enumerate(comments):
            reply_list = replies[i]
            reply_dict_list = []
            for reply in reply_list:
                # Structure the reply data.
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

            # Structure the comment data, including its replies.
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

        # Assemble the complete video data object.
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
                "url": url,
                "aweme_id": metadata["aweme_id"],
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
    def userDictFormer(sec_id: str, handler: Dict[str, Any], videoList: List[Dict[str, Any]],
                       commentList: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Combine user metadata, video data, and comment data into a single structured dictionary.

        :param sec_id: A string representing the user's secondary ID (sec_uid).
        :param handler: A dictionary containing user profile data.
        :param videoList: A list of dictionaries, each representing a video.
        :param commentList: A list of dictionaries, each representing a comment.
        :return: A dictionary combining all user-related data.
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
                    "total_favorited": handler["total_favorited"]
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

    async def newDownload(self, video_url: str, index: int, path_to: str = None, path_strict: bool = False) -> str:
        """
        Download the video from the provided URL and save it to a file.

        This method ensures the video is completely saved and also checks Docker logs for any errors
        during the download process. If errors are detected, it retries the download.

        :param video_url: The URL of the video to download.
        :param index: An integer used to generate a unique filename.
        :param path_to: Optional; the directory to save the video file.
        :param path_strict: If True, bypasses basePather and saves directly to the provided path.
        :return: The full file path where the video is saved.
        :raises RuntimeError: If errors are detected in Docker logs during download.
        """
        # Determine the appropriate output directory based on the path_strict flag.
        if not path_strict:
            if path_to is None:
                path_to = basePather(None)
            else:
                path_to = basePather(path_to)
        else:
            if path_to is None:
                path_to = downloadVideoPath(index)
            else:
                path_to = downloadVideoPath(index, path_to)
        os.makedirs(path_to, exist_ok=True)

        # Record current time to filter Docker logs later.
        sinceTimestamp = int(time.time())

        # Set a high timeout value for potentially large video downloads.
        timeout = httpx.Timeout(9000.0)  # Approximately 9000 seconds.
        async with httpx.AsyncClient(timeout=timeout) as client:
            endpoint = "download"
            request_url = self.urlFromEndpoint(endpoint)
            params = {"url": video_url, "prefix": False, "with_watermark": False}
            video_content = await fetch_video_stream_with_retry(client, request_url, params)
        # Determine file path based on path_strict.
        if path_strict:
            file_path = path_to
            os.makedirs(path_to, exist_ok=True)
        else:
            file_path = os.path.join(path_to, f"{index}.mp4")
        # Save the video content asynchronously.
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(video_content)

        # Wait a short period to ensure the file is completely written.
        for _ in range(10):
            if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
                break
            await asyncio.sleep(0.5)

        # Check Docker logs for errors during the download.
        container_name = "douyin_tiktok_api"  # Update with your container name if necessary.
        try:
            if check_docker_logs(container_name, since=sinceTimestamp):
                raise RuntimeError(f"Errors detected in Docker logs for video {video_url}")
        except RuntimeError as e:
            print(f"Error checking Docker logs: {e}")
            # If an error occurs, retry the download.
            return await self.newDownload(video_url, index, path_to)

        print(f"Downloaded video successfully: {video_url}")
        return file_path

    async def composeUserData(self, user_set: set, user_video_dict: Dict[str, Any], user_comment_dict: Dict[str, Any]):
        """
        Compose and insert user data into the local database by combining fetched video and comment data.

        :param user_set: A set of unique user sec_uid values.
        :param user_video_dict: A dictionary mapping user sec_uid to a list of video data.
        :param user_comment_dict: A dictionary mapping user sec_uid to a list of comment data.
        :return: None
        """
        for sec_uid in user_set:
            # Fetch and parse user handler data for the given user.
            handler = Fetcher.parseHandlerData(await self.fetchUserHandler(sec_uid))
            videoList = user_video_dict[sec_uid]
            commentList = user_comment_dict[sec_uid]
            # Combine all user-related data into one dictionary.
            userData = Fetcher.userDictFormer(sec_uid, handler, videoList, commentList)
            self.db.new_user(userData)

    def userDataUpdater(self, user_set: set, user_video_dict: Dict[str, Any], user_comment_dict: Dict[str, Any]):
        """
        Update existing user records in the database with new video and comment data.

        This method iterates through a set of user identifiers (sec_uid) and for each user, it checks the
        current videos and comments stored in the database. If a given video or comment is not already present,
        the corresponding update method is called to add the new data.

        :param user_set: A set of user sec_uid values to update.
        :param user_video_dict: A dictionary mapping each user sec_uid to a list of video data dictionaries.
        :param user_comment_dict: A dictionary mapping each user sec_uid to a list of comment data dictionaries.
        :return: None
        :raises ValueError: If a user with a given sec_uid is not found in the database.
        """
        # Iterate over each user identifier in the provided set.
        for sec_uid in user_set:
            # Process video data for the current user.
            for video in user_video_dict[sec_uid]:
                aweme_id = video["aweme_id"]
                # Retrieve the user's current video list from the database.
                user_videos = self.db.search_users(["sec_uid"], [sec_uid])[0].get("videos", [])
                # If user videos are found, check if the current video already exists.
                if user_videos is not None:
                    if len(user_videos) > 0:
                        # Loop through each video record for this user.
                        for user_video in user_videos:
                            # If the video already exists (matching aweme_id), do nothing.
                            if user_video["aweme_id"] == aweme_id:
                                break
                        else:
                            # If no matching video is found, update the user's video list with the new video.
                            self.db.update_user_videos(sec_uid, aweme_id)
                    else:
                        # If the video list is empty, update it with the new video.
                        self.db.update_user_videos(sec_uid, aweme_id)
                else:
                    # If no user record is found, raise an error.
                    raise ValueError(f"User {sec_uid} not found in database.")

            # Process comment data for the current user.
            for comment in user_comment_dict[sec_uid]:
                aweme_id = comment["aweme_id"]
                comment_id = comment["comment_id"]
                # Retrieve the user's current comment list from the database.
                user_comments = self.db.search_users(["sec_uid"], [sec_uid])[0].get("comments", [])
                # Check if the comment list exists.
                if user_comments is not None:
                    if len(user_comments) > 0:
                        # Loop through each comment record for this user.
                        for user_comment in user_comments:
                            # If the comment (both aweme_id and comment_id) already exists, do nothing.
                            if user_comment["aweme_id"] == aweme_id and user_comment["comment_id"] == comment_id:
                                break
                        else:
                            # If no matching comment is found, update the user's comment list with the new comment.
                            self.db.update_user_comments(sec_uid, aweme_id, comment_id)
                    else:
                        # If the comment list is empty, update it with the new comment.
                        self.db.update_user_comments(sec_uid, aweme_id, comment_id)
                else:
                    # If no user record is found, raise an error.
                    raise ValueError(f"User {sec_uid} not found in database.")

    async def downloader(self, video_url: str, index: int, output_folder: str = None) -> str:
        """
        Download a video and verify that the file was saved successfully.

        :param video_url: The URL of the video to download.
        :param index: An integer used to generate a unique filename.
        :param output_folder: Optional; the folder where the video file should be saved.
        :return: The full file path of the saved video.
        :raises RuntimeError: If the file does not exist or has zero size after download.
        """
        if output_folder is None:
            output_folder = downloadVideoPath(index)
        else:
            output_folder = downloadVideoPath(index, output_folder)
        os.makedirs(output_folder, exist_ok=True)

        # Delegate to newDownload for the actual download process.
        file_path = await self.newDownload(video_url, index, output_folder, path_strict=True)
        # Check that the file was saved correctly.
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            print(f"Downloaded video successfully: {video_url}")
        else:
            print(f"Failed to download video: {video_url}")
            raise RuntimeError(f"Failed to download video: {video_url}")
        return file_path

    async def fetch(self, dl: bool = True, collect_replies: bool = True, collect_commenter_data: bool = True):
        """
         High-level function to fetch videos, metadata, comments, and user data from Douyin.

         The method processes each video in the CSV file by:
          - Optionally downloading the video.
          - Fetching the unique aweme_id.
          - Retrieving video metadata.
          - Fetching comments and, if specified, their replies.
          - Aggregating user data from both video authors and commenters.
          - Saving the collected video data to the local database.
          - Periodically pickling user data as a failsafe.


         :param dl: If True, download the video files.
         :param collect_replies: If True, fetch replies for each comment.
         :param collect_commenter_data: If True, collect additional data for users who post comments.
         :return: None
         """
        # Temporary paths for pickling user data as a backup.
        tempPickBasePath = "tempPickles"
        tempUserSetPickPath = os.path.join(tempPickBasePath, "tempUserSet.pickle")
        tempUserVideoDictPickPath = os.path.join(tempPickBasePath, "tempUserVideoDict.pickle")
        tempUserCommentDictPickPath = os.path.join(tempPickBasePath, "tempUserCommentDict.pickle")

        # Initialize data structures to store user-related data.
        userSet = set()
        userVideoDict = {}
        userCommentDict = {}
        newUserSet = set()  # To track newly encountered users.
        usersToUpdate = set()  # To track users that already exist in the database.

        # Load existing users from the database.
        existing_users = self.db.get_all_users()
        for user in existing_users:
            sec_uid = user["sec_uid"]
            userSet.add(sec_uid)
            userVideoDict[sec_uid] = []
            userCommentDict[sec_uid] = []

        def userAdding(sec_uid: str, isAuthor: bool, awe_id: str, cid: str = None):
            """
            Add a user's sec_uid to the local user data structures.

            If the user does not exist, initialize their video and comment lists. Otherwise, mark them for update.

            :param sec_uid: The user's secondary ID.
            :param isAuthor: True if the user is the video author; otherwise, False.
            :param awe_id: The video identifier.
            :param cid: Optional; the comment identifier, required for non-author entries.
            :return: None
            :raises ValueError: If a comment ID is missing for non-author entries.
            """
            if sec_uid not in userSet:
                userSet.add(sec_uid)
                userVideoDict[sec_uid] = []
                userCommentDict[sec_uid] = []
            else:
                usersToUpdate.add(sec_uid)
            if isAuthor:
                userVideoDict[sec_uid].append({"aweme_id": awe_id})
            else:
                if cid is not None:
                    userCommentDict[sec_uid].append({"aweme_id": awe_id, "comment_id": cid})
                else:
                    raise ValueError("Comment ID is required for non-author entries.")

        size_at_start = self.db.videoSize()
        # Process each row from the CSV file.
        for index, row in self.df.iterrows():
            index: int = index
            index = int(index)
            current_index = self.db.videoSize()  # Use the current database size as the unique index.
            print(f"Processing video {current_index} of {len(self.df) + size_at_start} ({index}/{len(self.df)})")
            video_url = row['video_url']
            hashtags = row['hashtags']
            if dl:
                # Download the video file if requested.
                await self.downloader(video_url, current_index)
            # Retrieve the video’s unique identifier.
            aweme_id = await self.fetch_aweme_id(video_url)
            # Fetch the video metadata and parse it.
            metadata = Fetcher.parseVideoMetadata(await self.fetchVideoMetadata(aweme_id))
            author_sec_uid = metadata["sec_uid"]
            # Add the video author to the user data structures.
            userAdding(author_sec_uid, True, aweme_id)
            comment_count = metadata["comment_count"]
            # I found that Douyin API limits the comment count to 50, so we set a cap.
            if comment_count > 50:
                comment_count = 50
            # Fetch and parse the video comments.
            comments = Fetcher.parseCommentData(await self.fetchComments(aweme_id, comment_count))
            replyList = []
            # Process each comment.
            for comment in comments:
                sec_uid = comment["sec_uid"]
                cid = comment["cid"]
                if collect_commenter_data:
                    userAdding(sec_uid, False, aweme_id, cid)
                replies = []
                if collect_replies:
                    reply_count = comment["reply_comment_total"]
                    replies = Fetcher.parseReplyData(await self.fetchCommentReplies(aweme_id, cid, reply_count))
                replyList.append(replies)
                if collect_replies:
                    for reply in replies:
                        rep_sec_uid = reply["sec_uid"]
                        userAdding(rep_sec_uid, False, aweme_id, cid)
            # Combine all video data into a single dictionary.
            video_data = self.videoDataFormer(video_url, metadata, comments, replyList, current_index)
            # Save the video data into the database.
            self.db.new_video(video_data, search_query=hashtags)
            os.makedirs(tempPickBasePath, exist_ok=True)
            # Save the user data structures as pickle files for backup.
            pd.to_pickle(userSet, tempUserSetPickPath)
            pd.to_pickle(userVideoDict, tempUserVideoDictPickPath)
            pd.to_pickle(userCommentDict, tempUserCommentDictPickPath)

        # Insert new user data into the database.
        await self.composeUserData(newUserSet, userVideoDict, userCommentDict)
        # Update existing user data in the database.
        self.userDataUpdater(usersToUpdate, userVideoDict, userCommentDict)

    # Example usage in an async main (for testing purposes):


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

    fetcher = Fetcher("videos_trim_1.csv", "output_folder")
    asyncio.run(fetcher.fetch(dl=False, collect_replies=False, collect_commenter_data=False))
