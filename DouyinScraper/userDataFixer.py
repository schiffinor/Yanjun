"""
I made a typo in the userDictFormer function, which caused the userDict to be incorrect.
I stored the entire handler to:
 "user_data": {
                "statistics": {
                    "total_favorited"
                }
            }
instead of the actual value of total_favorited.
This caused the userDict to be incorrect and the database to be incorrect.
I will fix this by creating a new function that will fix the userDict and then write it to the database.
"""

import json
from typing import Dict, Any, List
import sys
import os
import warnings as wn
import database

def userDictFormer() -> None:
    """
    This function takes a userDict and returns a new userDict with the correct values.
    :param userDict: The userDict to be fixed.
    :return: The fixed userDict.
    """
    # Load the db.json file
    database_path = "C:\\Users\\schif\\Documents\\Coding\\Yanjun\\Database\\db.json"
    with open(database_path, "r", encoding="utf-8") as f:
        db: Dict[str, Any] = json.load(f)
    # db["Users"] is a dict with keys 1, 2, 3, ... and values are dicts with keys "user" and "user_data"
    # create list dataList by turning db["Users"] into a list of dicts with keys "user" and "user_data"
    dataList: List[Dict[str, Any]] = []
    for user in db["Users"].keys():
        dataList.append(db["Users"][user])

    for user in dataList:
        # Check if the user has a "user_data" key
        # print the user to make sure it is correct
        print(user)
        if "user_data" in user:
            # Check if the user has a "statistics" key
            if "statistics" in user["user_data"]:
                # Check if the user has a "total_favorited" key
                if "total_favorited" in user["user_data"]["statistics"]:
                    # Get the value of total_favorited
                    handler = user["user_data"]["statistics"]["total_favorited"]
                    # Check if the value is a dictionary
                    if isinstance(handler, dict):
                        # Check if the handler has key "total_favorited"
                        if "total_favorited" in handler:
                            # Get the value of total_favorited
                            total_favorited = handler["total_favorited"]
                            # Set the value of total_favorited to the correct value
                            user["user_data"]["statistics"]["total_favorited"] = total_favorited
                        else:
                            wn.warn(f"Key 'total_favorited' not found in handler: {handler}")
                    else:
                        wn.warn(f"Handler is not a dictionary: {handler}")
                else:
                    wn.warn(f"Key 'total_favorited' not found in user: {user}")
            else:
                wn.warn(f"Key 'statistics' not found in user: {user}")
        else:
            wn.warn(f"Key 'user_data' not found in user: {user}")
    input("Press Enter to continue...")
    # print the fixed userDict to make sure it is correct
    print(json.dumps(db, indent=4, ensure_ascii=False))
    # get user approval to overwrite the database
    user_approval = input("Do you want to overwrite the database? (y/n): ")
    if user_approval.lower() == "y":
        # write the fixed userDict to the database
        with open(database_path, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=4, ensure_ascii=False)
        print("Database overwritten.")
    else:
        print("Database not overwritten.")

if __name__ == "__main__":
    # drop user table in database
    db = database.Database()
    # call the userDictFormer function to fix the userDict
    userDictFormer()



