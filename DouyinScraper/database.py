import pathlib

from tinydb import TinyDB, Query, where
from tinydb.operations import delete, increment, add, subtract, set
import uuid
import json
import os
from pathlib import Path
from typing import Union, List, Dict, Any

class Database:
    def __init__(self, db_path: str = None, db_name: str = None, clear: bool = False):
        """

        :param db_path:
        :param db_name:
        """
        # if path is none then use the "Database" folder in the repository root, ie the directory containing the cwd or the current working directory's parent directory
        if db_path is None:
            db_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.joinpath("Database")
        self.db_path = db_path
        os.makedirs(self.db_path, exist_ok=True)
        if db_name is None:
            db_name = "db.json"
        self.db_name = db_name
        self.full_path = self.db_path.joinpath(self.db_name)
        os.makedirs(self.db_path, exist_ok=True)
        # make our json if it doesn't exist
        if not os.path.exists(self.full_path):
            with open(self.full_path, "w") as f:
                json.dump({}, f)
        self.db = TinyDB(self.full_path)
        self.query = Query()
        if clear:
            self.db.drop_tables()
        self.video_table = self.db.table("Videos")
        self.user_table = self.db.table("Users")

    def new_video(self, dataDump: Dict[str, Any] = None, search_query: str = None):
        """
        Create a new video object in the database
        :param dataDump:
        :param search_query:
        :return:
        """
        # We will create a videoObj with just an index
        # Get current length of the table
        index = len(self.video_table)
        video_record = dataDump
        # Insert the video record (using the generated video_id as the key)
        self.video_table.insert({"index": index, **video_record, "search_query": search_query})

    def new_user(self, dataDump: Dict[str, Any] = None):
        """
        Create a new user object in the database
        :param dataDump:
        :return:
        """
        # We will create a videoObj with just an index
        # Get current length of the table
        index = len(self.user_table)
        user_record = dataDump
        # Insert the video record (using the generated video_id as the key)
        self.user_table.insert({"index": index, **user_record})

    def videoSize(self):
        """
        Get the number of videos in the database
        :return:
        """
        return len(self.video_table)

    def userSize(self):
        """
        Get the number of users in the database
        :return:
        """
        return len(self.user_table)

    def get_video(self, index: int) -> Dict[str, Any]:
        """
        Get a video object from the database
        :param index:
        :return:
        """
        pass








if __name__ == "__main__":
    """# We will create a videoObj with just an index
        # Get current length of the table
        index = len(self.video_table)
        videoObj = {
            "index": index,
            "videoFiles": [],
            "metadata": [],
            "comments": []
        }
        self.video_table.insert(videoObj)
        self.video_table.update(add("videoFiles", [{"video_file": "", "transcripts": [{"srt": "", "vtt": "", "txt": ""}]}]))
        self.video_table.update(add("metadata", [{"video_metadata_json": "", "poster_id": ""}]))
        self.video_table.update(add("comments", [{"comments_json": "", "comments_csv": ""}]))"""
    # find out what directory this script is executing from
    print(os.getcwd())
    db = Database(clear=True)
    db.new_video()