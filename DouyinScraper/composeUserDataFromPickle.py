from FileFetcher import *
import pickle
import json
import asyncio
from typing import Dict, Any, Set
import os


# noinspection DuplicatedCode
def composeUserDataFromTempPickles(basePicklePath: str, from_index: int = 0, to_index: int = -1) -> None:
    """
    This function reads user data from a pickle file and writes it to database files.

    :param basePicklePath: The path to the temp pickle folder containing user data, specifically tempUserSet.pickle, tempUserCommentDict.pickle, and tempUserVideoDict.pickle.
    :param from_index: The starting index for the user data to be processed. Default is 0.
    :param to_index: The ending index for the user data to be processed. Default is -1, which means all data will be processed.
    """
    picklePath = os.path.join(basePicklePath, "tempUserSet.pickle")
    commentDictPath = os.path.join(basePicklePath, "tempUserCommentDict.pickle")
    videoDictPath = os.path.join(basePicklePath, "tempUserVideoDict.pickle")
    # Check if the pickle files exist
    if not os.path.exists(picklePath):
        raise FileNotFoundError(f"Pickle file not found: {picklePath}")
    if not os.path.exists(commentDictPath):
        raise FileNotFoundError(f"Comment dictionary file not found: {commentDictPath}")
    if not os.path.exists(videoDictPath):
        raise FileNotFoundError(f"Video dictionary file not found: {videoDictPath}")
    # Load the pickle files
    with open(picklePath, "rb") as f:
        userSet: Set[str] = pickle.load(f)
    with open(commentDictPath, "rb") as f:
        userCommentDict: Dict[str, list] = pickle.load(f)
    with open(videoDictPath, "rb") as f:
        userVideoDict:  Dict[str, list] = pickle.load(f)
    # Check if the userSet is empty
    if not userSet:
        raise ValueError("User set is empty.")
    # Check if the userCommentDict is empty
    if not userCommentDict:
        raise ValueError("User comment dictionary is empty.")
    # Check if the userVideoDict is empty
    if not userVideoDict:
        raise ValueError("User video dictionary is empty.")

    # Check if the from_index and to_index are valid
    if from_index < 0 or from_index >= len(userSet):
        raise IndexError(f"from_index is out of range: {from_index}")
    if to_index < -1 or to_index >= len(userSet):
        raise IndexError(f"to_index is out of range: {to_index}")
    if to_index == -1:
        to_index = len(userSet)
    if from_index >= to_index:
        raise ValueError(f"from_index is greater than or equal to to_index: {from_index} >= {to_index}")
    # Create a new userSet with the specified range
    # load the original database as json file and get the sec_uids corresponding to the set range
    database_path = "C:\\Users\\schif\\Documents\\Coding\\Yanjun\\Database\\db.json"
    with open(database_path, "r", encoding="utf-8") as f:
        db: Dict[str, Any] = json.load(f)
    users: Dict[str, dict] = db["Users"]
    sec_uids = [user["sec_uid"] for user in users.values()]
    selected_users = sec_uids[from_index:to_index]
    # create a set to subtract the selected users from the userSet
    selected_user_set = set(selected_users)
    # create a new userSet with the selected users
    userSetSub = userSet.difference(selected_user_set)
    # create a new userCommentDict with the userSetSub
    userCommentDictSub = {}
    for user in userSetSub:
        if user in userCommentDict:
            userCommentDictSub[user] = userCommentDict[user]
    # create a new userVideoDict with the userSetSub
    userVideoDictSub = {}
    for user in userSetSub:
        if user in userVideoDict:
            userVideoDictSub[user] = userVideoDict[user]

    # Create the FileFetcher object and feed it a valid csv file path, necessary because i didn't want to change the code in FileFetcher.py
    # to accept a list of dictionaries instead of a csv file path
    fileFetcher = Fetcher("videos_trim_2.csv", "output_folder")

    # Use the composeUserData function from Fetcher to write the user data to the database
    # remember that composeUserData is a coroutine

    asyncio.run(fileFetcher.composeUserData(userSetSub, userVideoDictSub, userCommentDictSub))


# noinspection DuplicatedCode
if __name__ == "__main__":
    # drop user table in database
    """dbb = database.Database()
    composeUserDataFromTempPickles("tempPickles")"""
    # load pickles to check
    basePicklePath = "tempPickles"
    picklePath = os.path.join(basePicklePath, "tempUserSet.pickle")
    commentDictPath = os.path.join(basePicklePath, "tempUserCommentDict.pickle")
    videoDictPath = os.path.join(basePicklePath, "tempUserVideoDict.pickle")
    # Check if the pickle files exist
    if not os.path.exists(picklePath):
        raise FileNotFoundError(f"Pickle file not found: {picklePath}")
    if not os.path.exists(commentDictPath):
        raise FileNotFoundError(f"Comment dictionary file not found: {commentDictPath}")
    if not os.path.exists(videoDictPath):
        raise FileNotFoundError(f"Video dictionary file not found: {videoDictPath}")
    # Load the pickle files
    with open(picklePath, "rb") as f:
        userSet: Set[str] = pickle.load(f)
    with open(commentDictPath, "rb") as f:
        userCommentDict: Dict[str, list] = pickle.load(f)
    with open(videoDictPath, "rb") as f:
        userVideoDict:  Dict[str, list] = pickle.load(f)
    # load database as jsonfile
    database_path = "C:\\Users\\schif\\Documents\\Coding\\Yanjun\\Database\\db.json"
    with open(database_path, "r", encoding="utf-8") as f:
        db: Dict[str, Any] = json.load(f)
    users: Dict[str, dict] = db["Users"]
    # get the sec_uids corresponding to the userSet
    sec_uids = [user["sec_uid"] for user in users.values()]
    # subtract the sec_uids from the userSet
    userSetSub = userSet.difference(set(sec_uids))
    # create a new userCommentDict with the userSetSub
    userCommentDictSub = {}
    for user in userSetSub:
        if user in userCommentDict:
            userCommentDictSub[user] = userCommentDict[user]
    # create a new userVideoDict with the userSetSub
    userVideoDictSub = {}
    for user in userSetSub:
        if user in userVideoDict:
            userVideoDictSub[user] = userVideoDict[user]
    # save the userSetSub, userCommentDictSub and userVideoDictSub to a new pickle file base math modPickle//
    bPP = "modPickle"
    os.makedirs(bPP, exist_ok=True)
    picklePath = os.path.join(bPP, "tempUserSet.pickle")
    commentDictPath = os.path.join(bPP, "tempUserCommentDict.pickle")
    videoDictPath = os.path.join(bPP, "tempUserVideoDict.pickle")
    # save the userSetSub to a pickle file
    with open(picklePath, "wb") as f:
        # noinspection PyTypeChecker
        pickle.dump(userSetSub, f)
    # save the userCommentDictSub to a pickle file
    with open(commentDictPath, "wb") as f:
        # noinspection PyTypeChecker
        pickle.dump(userCommentDictSub, f)
    # save the userVideoDictSub to a pickle file
    with open(videoDictPath, "wb") as f:
        # noinspection PyTypeChecker
        pickle.dump(userVideoDictSub, f)
    # check if the pickle files exist
    if not os.path.exists(picklePath):
        raise FileNotFoundError(f"Pickle file not found: {picklePath}")
    if not os.path.exists(commentDictPath):
        raise FileNotFoundError(f"Comment dictionary file not found: {commentDictPath}")
    if not os.path.exists(videoDictPath):
        raise FileNotFoundError(f"Video dictionary file not found: {videoDictPath}")
    composeUserDataFromTempPickles(bPP)


