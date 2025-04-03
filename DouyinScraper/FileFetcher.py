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
    if basePath is None:
        basePath = f"videoData\\{(str(dt.now()).replace(" ","_").replace(":", "-").replace(".", "_"))}\\"
        return basePath
    else:
        return basePath
    
def downloadVideoPath(index: int, basePath: str = None) -> str:
    """
    This function takes an index and a base path and returns the path to the video file.
    :param index: The index of the video.
    :param basePath: The base path to the video folder.
    :return: The path to the video file.
    """
    if basePath is None:
        db_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.joinpath("Database")
        os.makedirs(db_path, exist_ok=True)
        return f"{basePath}\\Videos\\{index}.mp4"
    else:
        return f"{basePath}\\Videos\\{index}.mp4"

def get_docker_client():
    """
    Get the Docker client.

    :return: Docker client instance.
    """
    try:
        client = docker.from_env()
        return client
    except Exception as e:
        print("Error connecting to Docker:", e)
        return None

def get_container_by_name(container_name: str):
    """
    Get a Docker container by its name.

    :param container_name: Name of the container to find.
    :return: Docker container instance or None if not found.
    """
    client = get_docker_client()
    if client is None:
        return None
    try:
        container = client.containers.get(container_name)
        return container
    except de.NotFound:
        print(f"Container '{container_name}' not found.")
        return None

def get_docker_logs(container_name: str, since: int = None) -> str:
    logs = ""
    try:
        container = get_container_by_name(container_name)
        # Get logs (decode from bytes to string)
        logs = container.logs(since=since).decode("utf-8")
        return logs
    except Exception as e:
        print("Error checking Docker logs:", e)
        return logs

def check_docker_logs(container_name: str, since: int = None, tail: int  = None) -> bool:
    """
    Check the Docker logs of the specified container for error patterns.

    :param container_name: Name or ID of the container running the API.
    :param since: Optional Unix timestamp to filter logs.
    :param tail: Optional number of lines to read from the end of the logs.
    :return: True if an error pattern is detected, otherwise False.
    """

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
    return check_docker_logs_for_errorPattern(error_patterns, since=since, tail=tail)

def check_docker_logs_for_ttwid_update_error():
    """
    Check the Docker logs for errors related to ttwid update.

    :return: True if an error pattern is detected, otherwise False.
    """
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
    Check the Docker logs for errors related to ttwid update.

    :return: True if an error pattern is detected, otherwise False.
    """
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
    Check the Docker logs for errors related to ttwid update.

    :return: True if an error pattern is detected, otherwise False.
    """
    error_patterns = [
        "WARNING  第 1 次响应内容为空, 状态码: 200,"
        "WARNING  第 2 次响应内容为空, 状态码: 200,",
        "WARNING  第 3 次响应内容为空, 状态码: 200,",
        "ERROR    无效响应类型。响应类型: <class 'NoneType'>",
        "<class 'NoneType'>"
    ]
    return check_docker_logs_for_errorPattern(error_patterns, since=None, tail=25)
    
def check_docker_logs_for_errorPattern(errorPattern: List[str], since: int = None, tail: int = None) -> bool:
    """
    Check the Docker logs for errors related to errorPattern.

    :return: True if an error pattern is detected, otherwise False.
    """
    try:
        container_name = "douyin_tiktok_api"  # Update to your container's name
        container = get_container_by_name(container_name)
        logs = container.logs(since=since, tail=tail).decode("utf-8")
        error_patterns = errorPattern
        for pattern in error_patterns:
            if pattern in logs:
                print(f"Detected ttwid update error in Docker logs: {pattern}")
                return True
        return False
    except Exception as e:
        print("Error checking Docker logs:", e)
        return False

async def fetch_ttwid():
    """
    Fetch the ttwid value from the douyin api.
    :return:
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
        requestURL = "http://localhost/api/douyin/web/generate_ttwid"
        response = await get_with_retry(client, requestURL, params={})
        return Fetcher.dataFromResponse(response)["ttwid"]

async def fetch_s_v_web_id():
    """
    Fetch the s_v_web_id value from the douyin api.
    :return:
    """
    async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
        requestURL = "http://localhost/api/douyin/web/generate_s_v_web_id"
        response = await get_with_retry(client, requestURL, params={})
        return Fetcher.dataFromResponse(response)["s_v_web_id"]

async def docker_restart(client: httpx.AsyncClient, request_url: str, params: dict,
                         max_retries: int = 5, delay: int = 5, backoff_factor: float = 2.0, restarts: int = 0, since: int = None) -> Optional[ httpx.Response]:
    """
    Restart the Docker container if an error is detected in the logs.

    :param client: httpx.AsyncClient instance.
    :param request_url: URL of the API endpoint to check.
    :param params: Parameters to send with the request.
    :param max_retries: Maximum number of retries for the request.
    :param delay: Initial delay between retries.
    :param backoff_factor: Factor by which to increase the delay after each retry.
    :param restarts: Number of times the container has been restarted.
    :param since: Optional Unix timestamp to filter logs.
    :return:
    """
    container_name = "douyin_tiktok_api"  # Update to your container's name
    sinceTimestamp = since
    try:
        dLogs = get_docker_logs(container_name, since=sinceTimestamp)
        # print last line of logs
        print(dLogs.splitlines()[-1])
        if check_docker_logs(container_name, since=sinceTimestamp) or (check_docker_logs_for_ttwid_update_error2()):
            print("Error detected in Docker logs. \nCheck if error is of type: \nERROR    无效响应类型。响应类型: <class 'NoneType'>")
            if check_docker_logs_for_ttwid_update_error2():
                # We need to generate new ttwid, and s_v_web_id, then update container/app/crawlers/douyin/web/config.yaml with new values
                print("Detected \"ERROR    无效响应类型。响应类型: <class 'NoneType'>\" in Docker logs.")
                print("Initiating update, and restart of the container.")
                container = get_container_by_name(container_name)
                # Get "/app/crawlers/douyin/web/config.yaml" file from the container
                config_file_path = "/app/crawlers/douyin/web/config.yaml"
                # get file from docker container as tar archive
                docker_file_stream, stat = container.get_archive(config_file_path)
                # Create a dedicated directory to save the file
                os.makedirs("docker_config_tar", exist_ok=True)
                # Save the file to the local filesystem
                with open("docker_config_tar/config.tar", "wb") as f:
                    for chunk in docker_file_stream:
                        f.write(chunk)
                # unpack the tar file
                shutil.unpack_archive("docker_config_tar/config.tar", "docker_config", "tar")
                # Now we need to update the config.yaml file with new ttwid, and s_v_web_id
                with open("docker_config/config.yaml", "r",encoding="utf-8") as f:
                    print("Reading config.yaml file...")
                    yamlLoad = yaml.safe_load(f)
                # Update the config with new ttwid, and s_v_web_id both values are stored on a single line:
                # "      Cookie: ttwid=1%7CJssbjvuUQM1BPJKFN3PNh5ej0gUiBjrNc83Zw8a2R_c%7C1743198163%7Cced07d156fd017cb6d3c9a28187298548179fcf743301b83dcdc8f2f7187e0cd; UIFID_TEMP=209b86ca33829c7d9c7b7c40c5eb89f829cca190227d7391efa903940cb34a3cb2eaee5fa34ca946d1c974997d1e93692b87442ccb9f686a2e8aca8d4aa4cb501b43b3210207779c39feed0d37c6a1bb; hevc_supported=true; IsDouyinActive=true; home_can_add_dy_2_desktop=%220%22; dy_swidth=4096; dy_sheight=1152; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A4096%2C%5C%22screen_height%5C%22%3A1152%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A16%2C%5C%22device_memory%5C%22%3A0%2C%5C%22downlink%5C%22%3A%5C%22%5C%22%2C%5C%22effective_type%5C%22%3A%5C%22%5C%22%2C%5C%22round_trip_time%5C%22%3A0%7D%22; volume_info=%7B%22isUserMute%22%3Atrue%2C%22isMute%22%3Atrue%2C%22volume%22%3A0.5%7D; stream_player_status_params=%22%7B%5C%22is_auto_play%5C%22%3A0%2C%5C%22is_full_screen%5C%22%3A0%2C%5C%22is_full_webscreen%5C%22%3A0%2C%5C%22is_mute%5C%22%3A1%2C%5C%22is_speed%5C%22%3A1%2C%5C%22is_visible%5C%22%3A1%7D%22; xgplayer_user_id=889208878171; fpk1=U2FsdGVkX1+pANPboYSOHYx0HudreojO8elUNxGbPOYJXkqcxSrSF1ld+iNZNPr3WZ9oQm7SEuBwB/uq9WD4SA==; fpk2=b0fc1a0934e7ea864f39ca0a0b863cfa; s_v_web_id=verify_m8tb8e26_xBk6z0gS_FCyn_4JUK_9S4S_sLzVrxgypowe; FORCE_LOGIN=%7B%22videoConsumedRemainSeconds%22%3A180%2C%22isForcePopClose%22%3A1%7D; passport_csrf_token=ac6461520516e67114ba9f2ef8a16060; passport_csrf_token_default=ac6461520516e67114ba9f2ef8a16060; __security_mc_1_s_sdk_crypt_sdk=ef677104-4e9d-aeb5; __security_mc_1_s_sdk_cert_key=aa542711-47ad-bc81; __security_mc_1_s_sdk_sign_data_key_web_protect=345a6279-4b89-814c; bd_ticket_guard_client_web_domain=2; UIFID=209b86ca33829c7d9c7b7c40c5eb89f829cca190227d7391efa903940cb34a3c3123432fde60cbaf0f05fd441cf18cf4d698ce7d3ab61ce5e0baf2c01481b5da70865d7467d7a5b41cb2050f82802767248bdfc243d90cf70c917effa07a322eddf302995a1e82e0c7984beb29c770e554777406bc89b3556a72c512e807f63115f2fb33eb5e2b28eae764f4a5b05f14c4ff4bb869501bc9907daef3bc3f24d8; __security_mc_1_s_sdk_sign_data_key_sso=913ec76f-41f4-8f67; odin_tt=e8e2dc8d10a064126c77fffb728b4dc87a5a11a3d067b4ee82af74f7f4cde41737ac2d07924525ffb03fccd80e618a652379b1daca460a8aaa922cf4386587c5b0f43a660dc223ddaaedea6a34f8526f; is_dash_user=1; SEARCH_RESULT_LIST_TYPE=%22single%22; WallpaperGuide=%7B%22showTime%22%3A1742070635298%2C%22closeTime%22%3A0%2C%22showCount%22%3A3%2C%22cursor1%22%3A47%2C%22cursor2%22%3A14%7D; strategyABtestKey=%221742070308.114%22; device_web_cpu_core=16; architecture=amd64; xg_device_score=7.501291248034187; douyin.com; device_web_memory_size=-1; csrf_session_id=e404af928480f0832e8d7ec8b2e250fa; biz_trace_id=b845cd9c; x-web-secsdk-uid=38327e99-2ba4-45f6-b9cf-6b4268aed836; __ac_nonce=067d5e221003aa2a1ceb1; __ac_signature=_02B4Z6wo00f01uX8RGQAAIDAXFPy7CsVSvrlzEDAAN6475; sdk_source_info=7e276470716a68645a606960273f276364697660272927676c715a6d6069756077273f276364697660272927666d776a68605a607d71606b766c6a6b5a7666776c7571273f275e58272927666a6b766a69605a696c6061273f27636469766027292762696a6764695a7364776c6467696076273f275e5827292771273f2735363037343635323537313234272927676c715a75776a716a666a69273f2763646976602778; bit_env=MTRjoBA90rhhNuVeuDF-FcaJe866A-g1QTyDOVi-AXoN4g8tjAmaHxzqXYtZoFl4d78qVN1mcbHzRbo-B30tDdL-iYB5pQHk6ICa4983L5pVS28kSbfSyENII5dsYUrof2QPrYYoCmw1VwSAf1J3dPTQv6FRIxw8QQ3AK2XtuBHwRwf6BNeSJh9Mk263Gl3mlc9u3DiKijTMmmqN-huniqXsiWLvEGyMnY39hz4bZyVUtKWB-qrCOt8qGTt6kpWCbgWLK2g3UXPA-e41JxOURI0q1yKzKNUvKNj24V-S0x1iFcUMXe89_DjmO5NE8x4dsLe2lsiBHMSxyA6R3AaY-ar1jygPF9_mZume4_GcybW8RvbxxXmxct4w0AMKnWZvRdh-BaDpKwjUtxC1kRA8mpsbgvXVFtgEqYwlGpMur-kUlmzGAhorvFSogC-AweMf3nZdcFnPZkg_rNjXkw7ihA%3D%3D; gulu_source_res=eyJwX2luIjoiM2Y3NGJhZDgxMzc3OThkNmVkN2U5ZjM3NDMzNGJkYjMwNzRhYjI0ZWJhMDZkMzdmYWNiNjgzNTY2ZjY0OGUyNCJ9; passport_auth_mix_state=zh8k3stgvoy8xkqxrm40scbxu5xmtz1eq8rckznvdlmb8uy9; download_guide=%222%2F20250315%2F0%22; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCSVRUY2F5b1IvblUyZmFTWWphbTRhWTR3QitrNE0xalhPNkhhTCtLVTFyREQyT0JSVlRSVmZtZnNVUmdTN1pBaXhzSEpUb2ZnY3hLTmJMTy9wVmQ0N1E9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoyfQ%3D%3D; passport_mfa_token=CjXyIxZUIeR0cFgOyBhlnYCHWvXe8rF2zHwDcSb67M2G12wX%2Bfg4fuXhZLujtsAss%2FxLmo4mqBpKCjwAAAAAAAAAAAAATsJ%2BhQx3PsD%2BDeRcDEKoblVvFgPjzTVlBzCsitfoTx8Tk9PZzK3uC7JxFXH8Xlbv3tUQipDsDRj2sdFsIAIiAQMBIz9o; d_ticket=f32930cb6da460ed67f51890df66ff671cee4; passport_assist_user=Cjzi4BdmWjVaFw2fgXH4Q2lRZdqw1CweDbDyFh0w-e9rOUQW2loIjiZxwc9RxFyi3ANG5a--wgW8eZMelrkaSgo8AAAAAAAAAAAAAE7CoJeN77a2yl6QKyfkHWa-OX6bq85v1QHhDL4H4dVdp0dkSBD3Dzjxm9Uh-oUl48jNEIqQ7A0Yia_WVCABIgEDH93qDg%3D%3D; n_mh=Q3TRjqTKcLq20aEd6hL6QQZ2YXXOKD-DcFlCjs0VPJE; passport_auth_status=a416d525d088943521c27317db68b5dd%2C; passport_auth_status_ss=a416d525d088943521c27317db68b5dd%2C; sid_guard=7f165ab6f8a949f13c5eae488661dddb%7C1742070518%7C5184000%7CWed%2C+14-May-2025+20%3A28%3A38+GMT; uid_tt=a1feb19606f82314ebff1cea261f291d; uid_tt_ss=a1feb19606f82314ebff1cea261f291d; sid_tt=7f165ab6f8a949f13c5eae488661dddb; sessionid=7f165ab6f8a949f13c5eae488661dddb; sessionid_ss=7f165ab6f8a949f13c5eae488661dddb; is_staff_user=false; sid_ucp_v1=1.0.0-KDM2ZDA0MDZmYmMzMDNjMWVlNjkxOTFjYmY5NmUxMmI4NDlkZDA0MDcKHwiV0PPB5QIQ9sXXvgYY7zEgDDDPjtrVBTgCQPEHSAQaAmxmIiA3ZjE2NWFiNmY4YTk0OWYxM2M1ZWFlNDg4NjYxZGRkYg; ssid_ucp_v1=1.0.0-KDM2ZDA0MDZmYmMzMDNjMWVlNjkxOTFjYmY5NmUxMmI4NDlkZDA0MDcKHwiV0PPB5QIQ9sXXvgYY7zEgDDDPjtrVBTgCQPEHSAQaAmxmIiA3ZjE2NWFiNmY4YTk0OWYxM2M1ZWFlNDg4NjYxZGRkYg; store-region=us; store-region-src=uid; login_time=1742070517274; publish_badge_show_info=%220%2C0%2C0%2C1742070518085%22; SelfTabRedDotControl=%5B%5D; _bd_ticket_crypt_doamin=2; _bd_ticket_crypt_cookie=9a42dc9a748568960ba53df9edb43a77; __security_server_data_status=1"
                cookieData = yamlLoad["TokenManager"]["douyin"]["headers"]["Cookie"]
                new_line = cookieData
                # Now we need to update the line with new ttwid, and s_v_web_id
                # Lets use regex to find the ttwid, and s_v_web_id
                ttwid_regex = r"ttwid=[^;]+"
                s_v_web_id_regex = r"s_v_web_id=[^;]+"
                # Lets find the ttwid, and s_v_web_id
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
                # Now we need to update the line in the file
                yamlLoad["TokenManager"]["douyin"]["headers"]["Cookie"] = new_line
                # CLose the file
                # Now we need to write the file back to the container
                with io.open("docker_config/config.yaml", "w", encoding="utf-8") as f:
                    yaml.safe_dump(yamlLoad, f, allow_unicode=True, default_flow_style=False)
                # Create a tar archive of the file
                tarPath = shutil.make_archive("docker_config/config", "tar", root_dir="docker_config", base_dir="config.yaml")
                with open(tarPath, "rb") as f:
                    tar_bytes = f.read()
                config_file_path_dir = config_file_path.rsplit("/", 1)[0]
                print(f"Uploading {tarPath} to {config_file_path_dir}...")
                filePut = container.put_archive(config_file_path_dir, tar_bytes)
                if filePut:
                    print("File updated in container.")
                # Now we need to restart the container
                timestamp2 = int(time.time())
                container.restart()
                print("Container restarted.")
                # wait until the container is ready
                while True:
                    container.reload()
                    if container.status == "running":
                        print("Container is running.")
                        #check to see if docker logs say "Application startup complete."
                        if "Application startup complete." in container.logs(since=timestamp2, tail=2).decode("utf-8"):
                            print("Application startup complete.")
                            await asyncio.sleep(3)
                            break
                    await asyncio.sleep(5)
                restarts += 1
                print(f"Restart count: {restarts}")
                print("Restarting the request...")
                # Restart the request
                return await get_with_retry(client, request_url, params, max_retries=max_retries, delay=delay, backoff_factor=backoff_factor, restarts=restarts)
    except     de.NotFound:
        print(f"Container '{container_name}' not found.")
    """except Exception as e:
        print("Error checking Docker logs:", e)"""

# Updated fetch_video_stream_with_retry with increased timeout and explicit ReadTimeout handling.
async def fetch_video_stream_with_retry(client: httpx.AsyncClient, request_url: str, params: dict,
                                        max_retries: int = 10, delay: int = 5, backoff_factor: float = 2.0,
                                        restarts: int = 0) -> bytes:
    try:
        sinceTimestamp = int(time.time())
        restart_count = restarts
        if restart_count > 5:
            print("Restart count exceeded. Exiting.")
            raise RuntimeWarning("Max restarts reached for video stream request")
        try:
            current_delay = delay
            for attempt in range(max_retries):
                try:
                    sinceTimestamp = int(time.time())
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
                            print(f"Attempt {attempt+1}: Received status code {response.status_code}. Retrying in {current_delay} seconds...")
                except (httpx.ReadTimeout, httpx.RequestError, Exception) as e:
                    print(f"Attempt {attempt+1}: Exception occurred: {e}. Retrying in {current_delay} seconds...")

                await asyncio.sleep(current_delay)
                current_delay *= backoff_factor
                if check_docker_logs_strict():
                    await docker_restart(client, request_url, params, max_retries=max_retries, delay=delay, backoff_factor=backoff_factor, restarts=restart_count, since=sinceTimestamp)
            restart_count = 0
            raise RuntimeWarning("Max retries reached for video stream request")
        except RuntimeWarning as e:
            # noinspection DuplicatedCode
            print(f"Error during video stream request: {e}")
            await docker_restart(client, request_url, params, max_retries=max_retries, delay=delay, backoff_factor=backoff_factor, restarts=restart_count, since=sinceTimestamp)
            await asyncio.sleep(600)
            # delete the client
            try:
                await client.aclose()
            except Exception as e:
                print(f"Error closing client: {e}")
            del client  # delete the client
            # create a new client
            client = httpx.AsyncClient(timeout=httpx.Timeout(9000.0))
            return await fetch_video_stream_with_retry(client, request_url, params, max_retries=max_retries, delay=delay, backoff_factor=backoff_factor)
    except Exception as e:
        print(f"Exception: {e}")
        print("This should only happen if \"'NoneType' object has no attribute 'status_code'\". \n"
              "Thus lets wait for 10 minutes, and try again.")
        responseOut = None
        while responseOut is None:
            await asyncio.sleep(600)
            # delete the client
            try:
                await client.aclose()
            except Exception as e:
                print(f"Error closing client: {e}")
            del client  # delete the client
            # create a new client
            client = httpx.AsyncClient(timeout=httpx.Timeout(9000.0))
            responseOut = await fetch_video_stream_with_retry(client, request_url, params, max_retries=max_retries, delay=delay, backoff_factor=backoff_factor)
            if responseOut is not None:
                break
            else:
                wn.warn("Response is None, retrying...")
        return responseOut

async def get_with_retry(client: httpx.AsyncClient, request_url: str, params: dict = None,
                         max_retries: int = 5, delay: int = 5, backoff_factor: float = 2.0, restarts: int = 0) -> httpx.Response:
    try:
        sinceTimestamp = int(time.time())
        restart_count = restarts
        if restart_count > 5:
            print("Restart count exceeded. Exiting.")
            raise RuntimeWarning("Max restarts reached for GET request")
        try:
            current_delay = delay
            for attempt in range(max_retries):
                try:
                    sinceTimestamp = int(time.time())
                    response = await client.get(request_url, params=params)
                    if response.status_code == 200:
                        return response
                    else:
                        print(f"Attempt {attempt+1}: Received status code {response.status_code}. Retrying in {current_delay} seconds...")
                except (httpx.ReadTimeout, httpx.RequestError) as e:
                    print(f"Attempt {attempt+1}: Exception occurred: {e}. Retrying in {current_delay} seconds...")
                await asyncio.sleep(current_delay)
                current_delay *= backoff_factor
                # Lets run the docker restart function
                if check_docker_logs_strict():
                    await docker_restart(client, request_url, params, max_retries=max_retries, delay=delay, backoff_factor=backoff_factor, restarts=restart_count, since=sinceTimestamp)
            restart_count = 0
            raise RuntimeWarning("Max retries reached for GET request")
        except RuntimeWarning as e:
            # noinspection DuplicatedCode
            print(f"Error during GET request: {e}")
            await docker_restart(client, request_url, params, max_retries=max_retries, delay=delay, backoff_factor=backoff_factor, restarts=restart_count, since=sinceTimestamp)
            await asyncio.sleep(600)
            # delete the client
            try:
                await client.aclose()
            except Exception as e:
                print(f"Error closing client: {e}")
            del client  # delete the client
            # create a new client
            client = httpx.AsyncClient(timeout=httpx.Timeout(9000.0))
            return await get_with_retry(client, request_url, params, max_retries=max_retries, delay=delay, backoff_factor=backoff_factor)
    except Exception as e:
        print(f"Exception: {e}")
        print("This should only happen if \"'NoneType' object has no attribute 'status_code'\". \n"
              "Thus lets wait for 10 minutes, and try again.")
        responseOut = None
        while responseOut is None:
            await asyncio.sleep(600)
            # delete the client
            try:
                await client.aclose()
            except Exception as e:
                print(f"Error closing client: {e}")
            del client  # delete the client
            # create a new client
            client = httpx.AsyncClient(timeout=httpx.Timeout(9000.0))
            responseOut = await get_with_retry(client, request_url, params, max_retries=max_retries, delay=delay, backoff_factor=backoff_factor)
            if responseOut is not None:
                break
            else:
                wn.warn("Response is None, retrying...")
        return responseOut


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
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "get_aweme_id"
            requestURL = self.urlFromEndpoint(endpoint)
            response = await get_with_retry(client, requestURL, {"url": video_url})
            return Fetcher.dataFromResponse(response)

    async def fetchVideoMetadata(self, aweme_id: str):
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "fetch_one_video"
            requestURL = self.urlFromEndpoint(endpoint)
            response = await get_with_retry(client, requestURL, {"aweme_id": aweme_id})
            return Fetcher.dataFromResponse(response)

    async def fetchComments(self, aweme_id: str, commentCount: int = -1):
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "fetch_video_comments"
            requestURL = self.urlFromEndpoint(endpoint)
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
            response = await get_with_retry(client, requestURL, {"aweme_id": aweme_id, "count": commentCount})
            return Fetcher.dataFromResponse(response)

    async def fetchCommentReplies(self, aweme_id: str, comment_id: str, replyCount: int = -1):
        # If there are zero replies, return an empty structure immediately.
        if replyCount == 0:
            return {"comments": []}

        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "fetch_video_comment_replies"
            requestURL = self.urlFromEndpoint(endpoint)
            if replyCount == -1:
                # First, request a small sample to learn the total reply count
                reply_count_response = await get_with_retry(client, requestURL,
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
                response = await get_with_retry(client, requestURL,
                    params={"item_id": aweme_id, "comment_id": comment_id, "cursor": 0, "count": replyCount}
                )
            else:
                response = await get_with_retry(client, requestURL,
                    params={"item_id": aweme_id, "comment_id": comment_id, "cursor": 0, "count": replyCount}
                )
            return Fetcher.dataFromResponse(response)

    async def fetchVideoFile(self, video_url: str):
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "download"
            requestURL = self.urlFromEndpoint(endpoint)
            response = await get_with_retry(client, requestURL, {"url": video_url, "prefix": False, "with_watermark": False})
            return response.content

    async def fetchUserHandler(self, sec_uid: str):
        async with httpx.AsyncClient(timeout=httpx.Timeout(9000.0)) as client:
            endpoint = "handler_user_profile"
            requestURL = self.urlFromEndpoint(endpoint)
            response = await get_with_retry(client, requestURL, {"sec_user_id": sec_uid})
            return Fetcher.dataFromResponse(response)

    @staticmethod
    def parseVideoMetadata(metadata: dict) -> Dict[str, Any]:
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
        return {"aweme_id": aweme_id, "sec_uid": sec_uid, "caption": caption, "create_time": create_time, "desc": desc,
                "duration": duration, "item_title": item_title, "ocr_content": ocr_content, "admire_count": admire_count,
                "collect_count": collect_count, "comment_count": comment_count, "digg_count": digg_count,
                "play_count": play_count, "share_count": share_count, "text_extra": text_extra, "video_tag": video_tag}

    @staticmethod
    def parseCommentData(comments: Dict[str, Any]) -> List[Dict[str, Any]]:
        comment_data = []
        commentList: List[Dict[str, Any]] = comments["comments"]
        # noinspection DuplicatedCode
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
        # noinspection DuplicatedCode
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
                               "digg_count": digg_count, "user_digged": user_digged, "is_author_digged": is_author_digged,
                               "is_hot": is_hot, "is_note_comment": is_note_comment})
        return reply_data

    @staticmethod
    def parseHandlerData(handler: dict) -> Dict[str, Any]:
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
    def videoDataFormer(url: str, metadata: Dict[str, Any], comments: List[Dict[str, Any]], replies: List[List[Dict[str, Any]]], index: int = None) -> Dict[str, Any]:
        """
        Combine video metadata, comments, and replies into a single structured dictionary.

        :param url: The URL of the video.
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
        Download the video, ensuring the file is fully saved before finishing.
        Checks Docker logs for errors to ensure the download was successful.

        :param video_url: URL of the video to download.
        :param index: An integer used to generate a filename.
        :param path_to: Directory to save the video file.
        :param path_strict: if true dont pass through basePather and make sure to save to path directly
        :return: The full file path of the saved video.
        """
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

        # Record the current timestamp for Docker log filtering.
        sinceTimestamp = int(time.time())

        # Increase the timeout for large video downloads.
        timeout = httpx.Timeout(9000.0)  # 300 seconds
        async with httpx.AsyncClient(timeout=timeout) as client:
            endpoint = "download"
            request_url = self.urlFromEndpoint(endpoint)
            params = {"url": video_url, "prefix": False, "with_watermark": False}
            video_content = await fetch_video_stream_with_retry(client, request_url, params)
        if path_strict:
            file_path = path_to
            os.makedirs(path_to, exist_ok=True)
        else:
            file_path = os.path.join(path_to, f"{index}.mp4")
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
            return await self.newDownload(video_url, index, path_to)

        print(f"Downloaded video successfully: {video_url}")
        return file_path

    async def composeUserData(self, user_set: set, user_video_dict: Dict[str, Any], user_comment_dict: Dict[str, Any]):
        for sec_uid in user_set:
            handler = Fetcher.parseHandlerData(await self.fetchUserHandler(sec_uid))
            videoList = user_video_dict[sec_uid]
            commentList = user_comment_dict[sec_uid]
            userData = Fetcher.userDictFormer(sec_uid, handler, videoList, commentList)
            self.db.new_user(userData)

    def userDataUpdater(self, user_set: set, user_video_dict: Dict[str, Any], user_comment_dict: Dict[str, Any]):
        for sec_uid in user_set:
            # update the userVideoDict and userCommentDict
            for video in user_video_dict[sec_uid]:
                aweme_id = video["aweme_id"]
                # Check if the video already exists in the database
                # ahaha not done yet
                self.db.update_user_videos(sec_uid, aweme_id)
            for comment in user_comment_dict[sec_uid]:
                aweme_id = comment["aweme_id"]
                comment_id = comment["comment_id"]
                # Check if the comment already exists in the database
                # ahaha not done yet
                self.db.update_user_comments(sec_uid, aweme_id, comment_id)

            
    async def downloader(self, video_url: str, index: int, output_folder: str = None):
        """
        Download the video, ensuring the file is fully saved before finishing.

        :param video_url: URL of the video to download.
        :param index: An integer used to generate a filename.
        :param output_folder: Directory to save the video file.
        :return: The full file path of the saved video.
        """
        if output_folder is None:
            output_folder = downloadVideoPath(index) 
        else:
            output_folder = downloadVideoPath(index, output_folder)
        os.makedirs(output_folder, exist_ok=True)

        # Call the newDownload method to handle the download.
        file_path = await self.newDownload(video_url, index, output_folder, path_strict=True)
        # Check if the file was downloaded successfully
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            print(f"Downloaded video successfully: {video_url}")
        else:
            print(f"Failed to download video: {video_url}")
            raise RuntimeError(f"Failed to download video: {video_url}")


    async def fetch(self, dl: bool = True, collect_replies: bool = True, collect_commenter_data: bool = True):
        """
        Fetch video metadata, comments, and replies from the Douyin API.
        :param dl:
        :param collect_replies:
        :param collect_commenter_data:
        :return:
        """
        """# check if pickle exists for userSet, userVideoDict, userCommentDict if so load them, else create them
        userPickleBasePath = "userInfoPickles"
        userSetPickPath = os.path.join(userPickleBasePath, "userSet.pickle")
        userVideoDictPickPath = os.path.join(userPickleBasePath, "userVideoDict.pickle")
        userCommentDictPickPath = os.path.join(userPickleBasePath, "userCommentDict.pickle")"""
        tempPickBasePath = "tempPickles"
        tempUserSetPickPath = os.path.join(tempPickBasePath, "tempUserSet.pickle")
        tempUserVideoDictPickPath = os.path.join(tempPickBasePath, "tempUserVideoDict.pickle")
        tempUserCommentDictPickPath = os.path.join(tempPickBasePath, "tempUserCommentDict.pickle")

        """if os.path.exists(userSetPickPath) and os.path.exists(userVideoDictPickPath) and os.path.exists(userCommentDictPickPath):
            userSet = pd.read_pickle(userSetPickPath)
            userVideoDict = pd.read_pickle(userVideoDictPickPath)
            userCommentDict = pd.read_pickle(userCommentDictPickPath)
        else:
            # Load data from database if pickles do not exist
            userSet = set()
            userVideoDict = {}
            userCommentDict = {}"""

        # Initialize user data structures
        userSet = set()
        userVideoDict = {}
        userCommentDict = {}
        newUserSet = set()
        usersToUpdate = set()

        # Load user set from database
        existing_users = self.db.get_all_users()
        for user in existing_users:
            sec_uid = user["sec_uid"]
            userSet.add(sec_uid)
            userVideoDict[sec_uid] = []
            userCommentDict[sec_uid] = []


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
            else:
                usersToUpdate.add(sec_uid)
            if isAuthor:
                userVideoDict[sec_uid].append({"aweme_id": awe_id})
            else:
                if cid is not None:
                    userCommentDict[sec_uid].append({"aweme_id": awe_id, "comment_id": cid})
                else:
                    raise ValueError("Comment ID is required for non-author entries.")

        # Check if processSucceded pickle exists, if so, load and check if True, if not load tempPickles and continue
        """processSucceeded = False
        processSucceededPath = "processSucceded.pickle"
        if os.path.exists(processSucceededPath):
            processSucceeded = pd.read_pickle(processSucceededPath)
            if processSucceeded:
                print("Process already succeeded, skipping.")
                return
            else:
                if os.path.exists(tempUserSetPickPath) and os.path.exists(tempUserVideoDictPickPath) and os.path.exists(tempUserCommentDictPickPath):
                    userSet = pd.read_pickle(tempUserSetPickPath)
                    userVideoDict = pd.read_pickle(tempUserVideoDictPickPath)
                    userCommentDict = pd.read_pickle(tempUserCommentDictPickPath)
                else:
                    raise ValueError("Process failed previously, but tempPickles are missing.")"""

        size_at_start = self.db.videoSize()
        for index, row in self.df.iterrows():
            index: int = index
            index = int(index)
            # Determine the current index for logging
            current_index = self.db.videoSize()
            print(f"Processing video {current_index} of {len(self.df) + size_at_start} ({index}/{len(self.df)})")
            video_url = row['video_url']
            hashtags = row['hashtags']
            if dl:
                # Download the video
                await self.downloader(video_url, current_index)
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
                if collect_commenter_data:
                    # add sec_uid to userSet
                    userAdding(sec_uid, False, aweme_id, cid)
                # Fetch reply count if collect_replies is True
                replies = []
                if collect_replies:
                    reply_count = comment["reply_comment_total"]
                    # print(f"Reply count: {reply_count}")
                    # Fetch replies
                    replies = Fetcher.parseReplyData(await self.fetchCommentReplies(aweme_id, cid, reply_count))
                replyList.append(replies)
                # Fetch and store reply_sec_uid if collect_replies is True
                if collect_replies:
                    for reply in replies:
                        rep_sec_uid = reply["sec_uid"]
                        userAdding(rep_sec_uid, False, aweme_id, cid)
            # Form video data object
            video_data = self.videoDataFormer(video_url, metadata, comments, replyList, current_index)
            # Save video data to the database
            self.db.new_video(video_data, search_query=hashtags)
            # make dirs for pickle files
            os.makedirs(tempPickBasePath, exist_ok=True)
            # pickle userSet, userVideoDict, userCommentDict as failsafe in case of error
            pd.to_pickle(userSet, tempUserSetPickPath)
            pd.to_pickle(userVideoDict, tempUserVideoDictPickPath)
            pd.to_pickle(userCommentDict, tempUserCommentDictPickPath)

        # Save user data to the database
        await self.composeUserData(newUserSet, userVideoDict, userCommentDict)
        # Update existing user data in the database
        self.userDataUpdater(usersToUpdate, userVideoDict, userCommentDict)

        """# If everything succeeded, store process status to pickle
        processSucceeded = True
        pd.to_pickle(processSucceeded, processSucceededPath)"""










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

    fetcher = Fetcher("videos_trim_1.csv", "output_folder")
    asyncio.run(fetcher.fetch(dl=False, collect_replies=False, collect_commenter_data=False))
