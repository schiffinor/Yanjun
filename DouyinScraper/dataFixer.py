"""
database is currently formatted as json instead of tinyDB so lets fix that, ie turn fixed integer string keys into a list of dictionaries
"""

import json
from typing import Dict, Any, List

def fixDatabase() -> None:
    """
    This function reads user data from a json file and writes it to a new json file with fixed integer string keys.
    """
    # Load the db.json file
    database_path = "C:\\Users\\schif\\Documents\\Coding\\Yanjun\\Database\\db.json"
    with open(database_path, "r", encoding="utf-8") as f:
        db: Dict[str, Any] = json.load(f)

    dbVidDict: Dict[str, dict] = db["Videos"]
    dbUserDict: Dict[str, dict] = db["Users"]
    dbVidList: List[dict] = []
    dbUserList: List[dict] = []
    # iterate over the dbVidDict.values(sorted by key value as int from smallest to greatest)
    for key in sorted(dbVidDict.keys(), key=lambda x: int(x)):
        dbVidList.append(dbVidDict[key])
    # iterate over the dbUserDict.values(sorted by key value as int from smallest to greatest)
    for key in sorted(dbUserDict.keys(), key=lambda x: int(x)):
        dbUserList.append(dbUserDict[key])
    # create modified json
    modified_db = {
        "Videos": dbVidList,
        "Users": dbUserList
    }
    # Override the original db.json file with the modified one
    with open(database_path, "w", encoding="utf-8") as f:
        # noinspection PyTypeChecker
        json.dump(modified_db, f, ensure_ascii=False, indent=4)
    print("Database fixed successfully.")


if __name__ == "__main__":
    fixDatabase()