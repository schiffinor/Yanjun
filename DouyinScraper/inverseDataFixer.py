"""

"""

import json
from typing import Dict, Any, List
import sys
import os
import warnings as wn
import database

def fixDatabase() -> None:
    """
    This function reads user data from a json file and writes it to a new json file with fixed integer string keys.
    """
    # Load the db.json file
    database_path = "C:\\Users\\schif\\Documents\\Coding\\Yanjun\\Database\\db.json"
    with open(database_path, "r", encoding="utf-8") as f:
        db: Dict[str, Any] = json.load(f)

    dbVidList: List[dict] = db["Videos"]
    dbUserList: List[dict] = db["Users"]
    dbVidDict: Dict[str, dict] = {}
    dbUserDict: Dict[str, dict] = {}

    # iterate over the dbVidList and create a dictionary with integer keys equal to index + 1
    for index, video in enumerate(dbVidList):
        dbVidDict[str(index + 1)] = video
    # iterate over the dbUserList and create a dictionary with integer keys equal to index + 1
    for index, user in enumerate(dbUserList):
        dbUserDict[str(index + 1)] = user
    # create modified json
    modified_db = {
        "Videos": dbVidDict,
        "Users": dbUserDict
    }
    # Override the original db.json file with the modified one
    with open(database_path, "w", encoding="utf-8") as f:
        json.dump(modified_db, f, ensure_ascii=False, indent=4)
    print("Database fixed successfully.")


if __name__ == "__main__":
    fixDatabase()