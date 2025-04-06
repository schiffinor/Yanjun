"""
database.py
===========

Overview:
---------
This module defines a Database class along with several helper functions to manage a simple
JSON-based database using TinyDB. It stores and queries video and user data, and even allows
data export to CSV or Excel files. The design strives to hide complexity, making it approachable
for users with very limited technical or computer science background.

Purpose:
--------
The primary goal of this file is to provide an easy-to-use interface for storing, retrieving, and
exporting video and user data. It is part of a larger program developed for a friend who is not
very tech-savvy. The module abstracts many of the intricate details of database management so that
the end user can perform operations without needing to understand the underlying code.

Key Features:
-------------
- **Initialization and Persistence**: Automatically sets up a JSON database file in a designated
  folder. If no path or file name is provided, it defaults to creating a "Database" folder and a file
  named "db.json".
- **Record Insertion**: Methods such as `new_video` and `new_user` allow for easy insertion of new
  video and user records. Each record is assigned an index automatically.
- **Record Retrieval**: Provides methods to retrieve individual records (`get_video`, `get_user`),
  all records (`get_all_videos`, `get_all_users`), and even the total count of records.
- **Record Updates**: Contains methods to update user records by appending video IDs and comment IDs,
  ensuring that new data can be added to existing records without starting over.
- **Query Construction**: Implements advanced query construction with support for logical operators
  (AND/OR) and custom comparison functions. This includes handling nested dictionaries and converting
  them into a flat structure for simpler querying.
- **Data Export**: Offers functionality to export data from the database to CSV or Excel files. This
  is particularly useful for sharing data with users who prefer viewing it in a familiar format.
- **Utility Functions**: Helper functions like `flatten_dict`, `recursive_fetch`, and `parse_indices`
  simplify operations on nested data structures, ensuring that even complex data can be handled in a
  straightforward manner.

Usage:
------
This module is intended to be simple enough for non-technical users while still offering flexibility
and power under the hood. Here is an example of how to use it:

    from database import Database

    # Create a new Database instance. The database will be created in the default folder if none is specified.
    db = Database()

    # Insert a new video record with some sample metadata and a search query.
    db.new_video(dataDump={"metadata": {"url": "http://example.com", "aweme_id": "12345", "sec_uid": "abcde",
                                          "video_metadata": {"primary": {"item_title": "Sample Video",
                                                                         "caption": "An example caption",
                                                                         "desc": "This is an example description"},
                                                             "basic_temporal": {"create_time": "2021-01-01", "duration": 120},
                                                             "engagement": {"play_count": 1000, "digg_count": 100,
                                                                            "comment_count": 50, "share_count": 10,
                                                                            "admire_count": 5, "collect_count": 3},
                                                             "additional": {"ocr_content": "Some OCR text"}}}},
                 search_query="funny cats")

    # Retrieve and display the number of videos stored.
    print(f"Total videos: {db.videoSize()}")

    # Export all video data to a CSV file.
    db.create_video_data_csv(to_excel=False)

Technical Details:
------------------
- **TinyDB Integration**: TinyDB is used for a lightweight, file-based database. This is ideal for
  small projects and for users who do not require a full-fledged relational database.
- **Nested Data Handling**: The module includes functions to flatten nested dictionaries, making
  it easier to build queries and export data in a tabular format.
- **Dynamic Querying**: Users can construct complex queries with logical combinations (AND/OR) and
  even define custom comparison operators to suit their needs.
- **Data Export**: The module leverages pandas for data export, ensuring that the output can be
  easily handled in common tools like Microsoft Excel or any CSV viewer.

Note to the User:
-----------------
This code is designed with simplicity and clarity in mind. It is extensively documented with inline
comments and verbose docstrings so that even someone with very limited knowledge of programming can
get an idea of how the system works. While there are advanced features built into the query construction,
the interface is designed to be as straightforward as possible for everyday use.

Author:
-------
Román Schiffino
GitHub: https://github.com/schiffinor

Date:
-----
2025-04-02

Docstring Note:
---------------
I'm a wee bit lazy when it comes to docstrings, so this is ai-generated. But hey, it does the job, right?
Similarly, the comments in the code have been fleshed out with ai, because I can't be bothered to write them all myself.
Basically, AI took my fragmented comments and made something helpful to people who might not know what the code does.
Just remember, if you find any typos or weird phrases, it's probably because I didn't proofread it. Cheers!
Contact me regarding any issues or questions you might have.
"""

import csv
import difflib
import json
import os
import pathlib
import re
from typing import List, Union, Dict, Callable, Any, Optional

import pandas as pd
from tinydb import TinyDB, Query
from tinydb.operations import add
from tinydb.queries import QueryLike
from tinydb.table import Table as tdTab

import Operands as Op
from And_Or import AND_OR as AO
from CommentTree import parseOpFunc
from Operands import RelOperator

OperatorTypes = Union[str, Op.RelOperator, Callable[[Any, Any], bool]]
OperatorList = List[OperatorTypes]
GeneralOperators = Union[OperatorTypes, List[OperatorTypes]]


def parseOpFuncList(opList: OperatorList, cast_type: bool = True) -> List[Callable[[Any, Any], bool]]:
    """
    Convert a list of operator definitions into a list of callable functions.

    :param opList: A list of operator definitions (strings, custom operator objects, or callables).
    :param cast_type: If True, cast the first argument to the type of the second before applying the operator.
    :return: A list of binary operator functions.
    :raises TypeError: If any operator cannot be parsed or is not callable.
    """
    parsed_ops = []  # Prepare a list to hold our freshly parsed operators.
    for op in opList:
        try:
            # Convert the operator definition into a callable function.
            parsed_op = parseOpFunc(op, cast_type=cast_type)
        except TypeError as e:
            # This means that the operator is not valid
            raise TypeError(f"Operator '{op}' is not valid. \n{e}")
        if not callable(parsed_op):
            # This is not a function; we expected something we can call.
            raise TypeError(f"Operator '{op}' is not callable.")
        parsed_ops.append(parsed_op)
    return parsed_ops


def flatten_dict(d: dict) -> dict:
    """
    Recursively flatten a nested dictionary into a single-level dictionary with dot-separated keys.

    :param d: The dictionary to flatten.
    :return: A flattened dictionary where nested keys are concatenated with a dot.
    """
    flat_keys = {}

    def flatten(d: dict, prefix: str = ""):
        # Iterate over each key-value pair in the dictionary.
        for k, v in d.items():
            # Build a new key: if there's a prefix, join it with the current key.
            new_key = f"{prefix}.{k}" if prefix else k
            if isinstance(v, dict):
                # Recursively flatten the sub-dictionary.
                flatten(v, new_key)
            else:
                # Terminal value; record it in the flat dictionary.
                flat_keys[new_key] = v

    flatten(d)
    return flat_keys


def guess_variable_name(var: str, d: dict) -> str:
    """
    Attempt to find the closest matching key in the dictionary (flattened by dot notation) for a given variable name.

    :param var: The variable name to match.
    :param d: The dictionary (expected to be already flattened) in which to search.
    :return: The value corresponding to the closest matching key.
    :raises ValueError: If no close match is found.
    """
    # Use difflib to attempt a match; we don't want to guess wildly, but a good guess is better than nothing.
    # Attempt to get the closest match
    matches = difflib.get_close_matches(var, d.keys(), n=1, cutoff=0.6)
    if matches:
        print(f"Guessed variable for '{var}' as '{matches[0]}'")
        return d[matches[0]]
    else:
        # If the guess fails, we prefer a clear error over a wrong guess.
        raise ValueError(f"Could not guess variable name for '{var}'.")


def get_data_keys(data: tdTab):
    """
    Retrieve the keys from the first record of a TinyDB table by flattening the record.

    :param data: A TinyDB table object.
    :return: A list of keys from the flattened first record.
    :raises ValueError: If the table is empty or the first record is not a dictionary.
    """
    dataList = data.all()  # Get all records; we only need the first one.
    if len(dataList) > 0:
        # Get the first element
        first_element = dataList[0]
        # Check if the first element is a dictionary
        if isinstance(first_element, dict):
            # Flatten the first record to extract all keys, because nested keys deserve their moment.
            flattened_dict = flatten_dict(first_element)
            # Get the keys of the flattened dictionary
            keys = list(flattened_dict.keys())
        else:
            raise ValueError("The first element is not a dictionary.")
    else:
        raise ValueError("The data is empty.")
    # Check if the first element
    return keys


def recursive_fetch(data: dict, var: str):
    """
    Retrieve a value from a nested dictionary using a dot-separated key string.

    :param data: The dictionary to search.
    :param var: A dot-separated key string (e.g., 'metadata.video_metadata.primary.caption').
    :return: The value corresponding to the nested key, or None if any key in the chain is missing.
    """
    # Split the variable name into its components
    v_components = var.split(".")
    output = data.get(v_components[0], None)
    if output is None:
        return None  # Top-level key not found; better to return nothing than crash.
    if not isinstance(output, dict):
        return output  # If the first value isn't a dict, just return it.
    # Traverse deeper into the dictionary.
    for comp in v_components[1:]:
        output = output[comp]
        if output is None:
            return None  # Oops, key path breaks here.
        if not isinstance(output, dict):
            if isinstance(output, list):
                # If we encounter a list, convert each element to a string for uniformity.
                for i in range(len(output)):
                    output[i] = str(output[i])
                output = str(output)
            return output  # Return the non-dictionary value as soon as it's reached.
    return output


def parse_indices(indices, data_length: int) -> List[int]:
    """
    Parse various types of index selectors for a dataset of a given length.

    :param indices: An index selector, which may be None, a list of integers, a slice object, or a slice string in the form 'start:stop:step'.
    :param data_length: The total number of elements in the dataset.
    :return: A list of indices selected based on the provided selector.
    :raises ValueError: If the slice string format is incorrect or if an unsupported type is provided.
    """
    if indices is None:
        # No indices provided; default to all indices.
        return list(range(data_length))
    elif isinstance(indices, list):
        # Only include indices that fall within the valid range.
        return [i for i in indices if 0 <= i < data_length]
    elif isinstance(indices, slice):
        # Apply the slice to the full range.
        return list(range(data_length))[indices]
    elif isinstance(indices, str):
        # Split the string by ':' expecting exactly three parts.
        parts = indices.split(':')
        if len(parts) != 3:
            raise ValueError("Slice string must be in the form 'start:stop:step'.")
        # Convert non-empty parts to integers; empty parts become None.
        start = int(parts[0]) if parts[0] != "" else None
        stop = int(parts[1]) if parts[1] != "" else None
        step = int(parts[2]) if parts[2] != "" else None
        slice_obj = slice(start, stop, step)
        return list(range(data_length))[slice_obj]
    else:
        raise ValueError(
            "Indices must be either a list of ints, a slice object, or a slice string in the form 'a:b:c'.")


# noinspection PyTypeChecker
class Database:
    def __init__(self, db_path: str = None, db_name: str = None, clear: bool = False):
        """
        Initialize the Database instance and set up the TinyDB JSON file.

        :param db_path: The folder path where the database will be stored. If None, defaults to a "Database" folder in the repository root.
        :param db_name: The name of the JSON database file. Defaults to "db.json" if None.
        :param clear: If True, drops all tables (i.e. clears the database) upon initialization.
        :return: None
        """
        # Determine the database path if not provided.
        if db_path is None:
            db_path = pathlib.Path(os.path.abspath(__file__)).parent.parent.joinpath("Database")
        self.db_path = pathlib.Path(db_path)
        os.makedirs(self.db_path, exist_ok=True)  # Create directory if it doesn't exist.
        # Set the database file name.
        if db_name is None:
            db_name = "db.json"
        self.db_name = db_name
        self.full_path = self.db_path.joinpath(self.db_name)
        os.makedirs(self.db_path, exist_ok=True)
        # If the JSON file doesn't exist, create an empty JSON object.
        if not os.path.exists(self.full_path):
            with open(self.full_path, "w", encoding="utf-8") as f:
                # noinspection PyTypeChecker
                json.dump({}, f, indent=4)
        # Initialize TinyDB using the JSON file.
        self.db = TinyDB(self.full_path, indent=4, encoding="utf-8", ensure_ascii=False)
        self.query = Query()
        if clear:
            self.db.drop_tables()  # Clear all tables if requested.
        self.video_table = self.db.table("Videos")  # Table for video records.
        self.user_table = self.db.table("Users")  # Table for user records.

    def new_video(self, dataDump: Dict[str, Any] = None, search_query: str = None):
        """
        Create and insert a new video record into the database.

        :param dataDump: A dictionary containing video metadata.
        :param search_query: The search query associated with this video.
        :return: None
        """
        # Use the current count as the index.
        index = len(self.video_table)
        video_record = dataDump  # Data to be inserted.
        # Insert the new video record with its index and search query.
        self.video_table.insert({"index": index, **video_record, "search_query": search_query})

    def new_user(self, dataDump: Dict[str, Any] = None):
        """
        Create and insert a new user record into the database.

        :param dataDump: A dictionary containing user metadata.
        :return: None
        """
        # Use the current count as the index.
        index = len(self.user_table)
        user_record = dataDump
        # Insert the new user record with its index.
        self.user_table.insert({"index": index, **user_record})

    def videoSize(self):
        """
        Retrieve the total number of video records in the database.

        :return: An integer count of video records.
        """
        return len(self.video_table)

    def userSize(self):
        """
        Retrieve the total number of user records in the database.

        :return: An integer count of user records.
        """
        return len(self.user_table)

    def get_video(self, index: int) -> Dict[str, Any]:
        """
        Retrieve a video record by its index.

        :param index: The index of the video record.
        :return: A dictionary representing the video record, or an empty dictionary if not found.
        """
        query = Query()
        video = self.video_table.get(query.index == index)
        if video is None:
            # If no record is found, return an empty dictionary (graceful failure).
            return {}
        else:
            return video

    def get_user(self, index: int) -> Dict[str, Any]:
        """
        Retrieve a user record by its index.

        :param index: The index of the user record.
        :return: A dictionary representing the user record, or an empty dictionary if not found.
        """
        query = Query()
        user = self.user_table.get(query.index == index)
        if user is None:
            return {}
        else:
            return user

    def get_all_videos(self) -> List[Dict[str, Any]]:
        """
        Retrieve all video records from the database.

        :return: A list of dictionaries, each representing a video record.
        """
        return self.video_table.all()

    def get_all_users(self) -> List[Dict[str, Any]]:
        """
        Retrieve all user records from the database.

        :return: A list of dictionaries, each representing a user record.
        """
        return self.user_table.all()

    def update_user_videos(self, sec_uid: str, aweme_id: str) -> None:
        """
        Append a video ID to a user's video list.

        :param sec_uid: The secondary user ID used to identify the user.
        :param aweme_id: The video ID to be appended.
        :return: None
        """
        # Use TinyDB's add operation to append to the "videos" list.
        self.user_table.update(add("videos", [{"aweme_id": aweme_id}]), self.query.sec_uid == sec_uid)

    def update_user_comments(self, sec_uid: str, aweme_id: str, comment_id: str) -> None:
        """
        Append a comment (associated with a video) to a user's comment list.

        :param sec_uid: The secondary user ID used to identify the user.
        :param aweme_id: The video ID to which the comment belongs.
        :param comment_id: The comment ID to be appended.
        :return: None
        """
        # Append a dictionary with both video and comment IDs.
        self.user_table.update(add("comments", [{"aweme_id": aweme_id, "comment_id": comment_id}]),
                               self.query.sec_uid == sec_uid)

    @staticmethod
    def guery_constructor(queryObj: Query,
                          variables: str | List[str],
                          values: Any | List[Any],
                          and_or: Optional[AO | str] = AO.AND,
                          operatorCode: Optional[GeneralOperators] = "==",
                          topVarNest: Optional[dict] = None,
                          unambiguous_vars_strict: Optional[dict] = None,
                          unambiguous_vars_nicknames: Optional[dict] = None,
                          ambiguous_var_choices: Optional[dict] = None,
                          cast_data: bool = True,
                          complex_logic: str = None) -> QueryLike:
        """
        Construct a complex query expression based on provided variables, values, and optional logical composition.

        :param queryObj: The base Query object.
        :param variables: A variable name or list of variable names to query.
        :param values: A value or list of values corresponding to the variables.
        :param and_or: Logical connector, either AO.AND or AO.OR (or their string equivalents).
        :param operatorCode: A single operator code or list of operator codes (e.g., "==", "<") for comparisons.
        :param topVarNest: A nested mapping for variable names to their full paths.
        :param unambiguous_vars_strict: Dictionary for strict variable name mapping.
        :param unambiguous_vars_nicknames: Dictionary mapping variable nicknames to standard names.
        :param ambiguous_var_choices: Dictionary for handling ambiguous variable names.
        :param cast_data: If True, cast the data types during comparison.
        :param complex_logic: An optional string defining complex logical relationships among conditions.
        :return: A QueryLike object that can be used to query TinyDB.
        :raises ValueError: If any inconsistency or invalid format is detected.
        """
        # 1. Normalize variables and values to lists.
        # If 'variables' is a single string, convert it to a list for uniform processing.
        if isinstance(variables, str):
            variables = [variables]
            # If 'variables' is a string, then 'values' must also be a string.
            if not isinstance(values, str):
                raise ValueError("If variables is a string, values must also be a string.")
        # Similarly, if 'values' is a single string, convert it to a list.
        if isinstance(values, str):
            values = [values]
        # Ensure that the number of variables matches the number of provided values.
        if len(variables) != len(values):
            raise ValueError("Length of variables and values must be the same.")

        # 2. Normalize and validate the logical connector ('and_or').
        # If provided as a string, clean it up and convert to a standardized AO constant.
        if isinstance(and_or, str):
            # accept both AND and OR as strings aswell as different capitalizations and any spacing differences, also parse "a" and "o" as AND and OR
            and_or = and_or.strip().lower()  # Remove leading/trailing whitespace and convert to lowercase.
            if and_or in ["and", "a"]:
                and_or = AO.AND
            elif and_or in ["or", "o"]:
                and_or = AO.OR
            else:
                raise ValueError("and_or must be either AND or OR.")
        # Double-check that the connector is valid.
        if not and_or in [AO.AND, AO.OR]:
            raise ValueError("and_or must be either AND or OR.")

        # 3. Validate that all provided variable names are strings.
        if not all(isinstance(var, str) for var in variables):
            raise ValueError("All variables must be strings.")

        # This list will hold the parsed and resolved variable names (or their mapped versions).
        var_parse = []

        # 4. Process each variable name:
        #    - Sanitize the name (remove spaces, unwanted characters, etc.).
        #    - Validate that it is a proper identifier.
        #    - Attempt to resolve the full variable path using several mappings.
        for var in variables:
            varParsed = False  # A flag to indicate if the variable has been successfully parsed/resolved.

            # Clean up the variable name: trim whitespace, replace problematic characters with underscores.
            var = var.strip().replace(" ", "_").replace("-", "_").replace(":", "_").replace(";", "_").replace(",", "_")
            # Remove any leading or trailing underscores
            var = var.lstrip("_").rstrip("_")
            # Make lowercase
            var = var.lower()
            # Check if the cleaned variable name is a valid Python identifier.
            if not var.isidentifier():
                raise ValueError(f"Variable name '{var}' is not valid.")
            # 4a. If the variable name contains a dot, assume it's a nested path.
            if "." in var:
                try:
                    var_components = var.split(".")  # Split by dot to traverse nested keys.
                    var_address = topVarNest  # Start at the top of the nested structure.
                    for component in var_components:
                        if component not in var_address:
                            raise ValueError(f"Variable name '{var}' is not valid.")
                        var_address = var_address[component]  # Traverse deeper into the nested dictionary.
                    var_parse.append(var_address)  # Append the final resolved variable.
                    continue  # Skip to the next variable since we've resolved this one.
                except ValueError:
                    print(f"Variable name '{var}' is not valid. Will attempt to guess the variable name.")
            # 4b. If the variable was not successfully parsed as nested, try various mappings.
            if not varParsed:
                # Check in the strict mapping dictionary.
                if var in unambiguous_vars_strict:
                    # Add the address to the list of variables
                    var_parse.append(unambiguous_vars_strict[var])
                    continue
                # Check in the nickname mapping dictionary.
                elif var in unambiguous_vars_nicknames:
                    # Add the address to the list of variables
                    var_parse.append(unambiguous_vars_nicknames[var])
                    continue
                # Check if the variable exists in the ambiguous choices mapping.
                elif var in ambiguous_var_choices:
                    # Add the address to the list of variables
                    var_parse.append(ambiguous_var_choices[var])
                    continue
                # If the variable contains a dot but hasn't been resolved, try a "flattened" guess.
                elif "." in var:
                    # check if the variable name is in the ambiguous_var_choices dictionary
                    try:
                        f_dict = flatten_dict(topVarNest)
                        var_address = guess_variable_name(var, f_dict)
                        # Add the address to the list of variables
                        var_parse.append(var_address)
                        continue
                    except ValueError:
                        print(f"Variable name '{var}' is not valid.")
                        print(f"Last ditch effort to guess the variable name.")
                        print(
                            f"Checking if the variable name is close to entry in the unambiguous_vars_strict dictionary.")
                # Try guessing the variable name from strict mapping.
                try:
                    var_address = guess_variable_name(var, unambiguous_vars_strict)
                    # Add the address to the list of variables
                    var_parse.append(var_address)
                    continue
                except ValueError:
                    print(f"Variable name '{var}' is not valid.")
                    print(
                        f"Checking if the variable name is close to entry in the unambiguous_vars_nicknames dictionary.")
                # Try guessing the variable name from the nickname mapping.
                try:
                    var_address = guess_variable_name(var, unambiguous_vars_nicknames)
                    # Add the address to the list of variables
                    var_parse.append(var_address)
                    continue
                except ValueError:
                    print(f"Variable name '{var}' is not valid.")
                    print(f"Checking if the variable name is close to entry in the ambiguous_var_choices dictionary.")
                # Finally, try guessing using the ambiguous choices mapping.
                try:
                    var_address = guess_variable_name(var, ambiguous_var_choices)
                    # Add the address to the list of variables
                    var_parse.append(var_address)
                    continue
                except ValueError:
                    print(f"Variable name '{var}' is not valid.")
                    print(f"Will not attempt to guess the variable name.")
            # If after all attempts the variable is still unresolved, raise an error.
            if not varParsed:
                raise ValueError(
                    f"Variable name '{var}' is not valid. \nTried all options. Please check the variable name and try again.")
                # Move on to next step

        # 5. Ensure the number of parsed variables matches the number of values.
        if len(var_parse) != len(values):
            raise ValueError("Length of variables and values must be the same.")

        # 6. Normalize the operatorCode parameter:
        # If a single operator code is provided, expand it to a list so there is one operator per variable.
        if not isinstance(operatorCode, list):
            operatorCode = [operatorCode] * len(variables)
        # Validate that the list of operator codes matches the length of values.
        if len(operatorCode) != len(values):
            raise ValueError("Length of operatorCode and values must be the same.")

        # 7. Parse each operator code into its corresponding callable function.
        # The function 'parseOpFuncList' is assumed to handle conversion, potentially using cast_data for type casting.
        operatorCode = parseOpFuncList(operatorCode, cast_type=cast_data)

        # 8. Define a helper function to recursively fetch a nested query component using dot notation.
        def recursive_fetch(qObj: Query, var: str):
            """
            Retrieve a nested query component from the Query object using dot notation.

            :param qObj: The base Query object.
            :param var: A dot-separated string indicating the nested field.
            :return: The nested query component.
            """
            # Split the variable name by dot to access nested fields.
            v_components = var.split(".")
            # Start by accessing the first component.
            output = qObj[v_components[0]]
            # If there is only one component, return it immediately.
            if len(v_components) == 1:
                return output
            # For each subsequent component, navigate deeper into the nested structure.
            for comp in v_components[1:]:
                output = output[comp]
            return output

        # 9. Define a helper function to construct a final query expression from a complex logic string.
        def construct_query_from_logic(c_logic: str, conditions: List[Any]) -> Any:
            """
            Construct a final query expression from a complex logic string and a list of condition expressions.

            :param c_logic: A string defining logic, e.g., "(((q1)&(q2)&(q3))|(~((~(q4))&(q5)&(q6))))".
            :param conditions: A list of query conditions corresponding to each 'q' in the logic.
            :return: The evaluated query expression.
            :raises ValueError: If the number of placeholders does not match the number of conditions.
            """

            # Replace occurrences of "q" followed by a number with a placeholder.
            # For example, q1 -> __q_0__, q2 -> __q_1__, etc.
            # Define a replacement function to swap out placeholders like q1, q2, etc.
            def replacer(match):
                # Extract the index number from the match and convert to zero-based index.
                index = int(match.group(1)) - 1
                return f"__q_{index}__"

            # Replace all occurrences of q<number> with a unique placeholder.
            transformed_logic = re.sub(r"q(\d+)", replacer, c_logic)

            # Find all placeholders in the transformed logic string.
            placeholders = re.findall(r"__q_\d+__", transformed_logic)
            # Ensure that the number of placeholders matches the number of provided conditions.
            if len(placeholders) != len(conditions):
                raise ValueError(
                    f"Expected {len(conditions)} conditions, but found {len(placeholders)} placeholders in the logic string.")

            # Prepare an evaluation environment mapping each placeholder to its corresponding condition.
            eval_env = {f"__q_{i}__": cond for i, cond in enumerate(conditions)}

            # Evaluate the expression in a restricted namespace (assuming that the operators &, |, and ~ are overloaded)
            try:
                # Safely evaluate the transformed logic expression.
                q = eval(transformed_logic, {"__builtins__": None}, eval_env)
            except Exception as e:
                raise ValueError(f"Error evaluating the logic expression: {e}")
            return q

        # 10. Check if a complex_logic string is provided to define a custom logical relationship between conditions.
        if complex_logic is not None:
            """
            Complex logic is a string that contains the logic for the query
            The logic is in the form of a string with the following format:
            For 6 variables:
            (((q)&(q)&(q))|(~((~(q))&(q)&(q))))
            where q is a variable and & is AND, | is OR, and ~ is NOT
            Each q must be wrapped in parentheses before the operator
            thus if & is used it must be in the form of (q)&(q) and thus that (q)$(q) is a valid query so is (q)&(q)&(q)
            The whole query must be wrapped in parentheses too.
            """
            # Validate that the provided complex_logic is indeed a string.
            if not isinstance(complex_logic, str):
                raise ValueError("complex_logic must be a string.")
            # Check for balanced parentheses in the logic string.
            if complex_logic.count("(") != complex_logic.count(")"):
                raise ValueError("complex_logic must be valid; mismatched parentheses.")
            # Ensure the logic string is wrapped in parentheses.
            if complex_logic[0] != "(" or complex_logic[-1] != ")":
                raise ValueError("complex_logic must be wrapped in parentheses.")
            # Count how many query placeholders (q) are present in the logic string.
            query_count = complex_logic.count("q")
            if query_count != len(var_parse):
                raise ValueError(
                    f"complex_logic must contain as many 'q's as there are variables. {query_count} != {len(var_parse)}")
            # Ensure each placeholder is wrapped in its own set of parentheses.
            matches = re.findall(r"\(q\)", complex_logic)
            if len(matches) != query_count:
                raise ValueError(
                    f"Each 'q' must be wrapped in parentheses; found {len(matches)} instead of {query_count}.")
            # Replace each occurrence of "(q)" with uniquely numbered placeholders "(q1)", "(q2)", etc.
            for i in range(query_count):
                complex_logic = complex_logic.replace("(q)", f"(q{i + 1})", 1)
                # Build a list of condition expressions by fetching the query components and testing them with operator and value.
            condition_list = []
            for var, curOpp, val in zip(var_parse, operatorCode, values):
                # 'recursive_fetch' gets the nested component and then '.test' applies the operator and value.
                condition_list.append(recursive_fetch(queryObj, var).test(curOpp, val))
            # Use the helper function to construct the final query using the complex logic string and condition list.
            query = construct_query_from_logic(complex_logic, condition_list)

        # 11. If no complex_logic is provided, construct the query using a simple AND/OR combination.
        elif and_or == AO.AND:
            # Loop through each variable, its corresponding operator, and value.
            query = None
            for var, curOpp, val in zip(var_parse, operatorCode, values):
                # Build the individual condition for each variable.
                current_condition = (recursive_fetch(queryObj, var).test(curOpp, val))
                # Combine conditions with an AND operation; if 'query' is None, initialize it with the first condition.
                query = (query & current_condition) if query is not None else current_condition
        elif and_or == AO.OR:
            # Loop through each variable, its corresponding operator, and value.
            query = None
            for var, curOpp, val in zip(var_parse, operatorCode, values):
                # Build the individual condition for each variable.
                current_condition = (recursive_fetch(queryObj, var).test(curOpp, val))
                # Combine conditions with an OR operation; if 'query' is None, initialize it with the first condition.
                query = (query | current_condition) if query is not None else current_condition
        else:
            # This branch should never be reached due to prior validation.
            raise ValueError("and_or must be either AND or OR.")

        # 12. Return the final query object that combines all the individual conditions.
        return query

    @staticmethod
    def video_query_constructor(queryObj: Query,
                                variables: str | List[str],
                                values: Any | List[Any],
                                and_or: Optional[AO | str] = AO.AND,
                                operatorCode: Optional[GeneralOperators] = "==",
                                cast_data: bool = True,
                                complex_logic: str = None) -> QueryLike:
        """
        Construct a query for video records using pre-defined nested mappings for video metadata.

        :param queryObj: The base Query object.
        :param variables: A string or list of variable names to filter on.
        :param values: A value or list of values corresponding to the variables.
        :param and_or: Logical connector (AO.AND or AO.OR) or their string equivalents.
        :param operatorCode: Operator(s) to use for comparing values.
        :param cast_data: If True, cast data types during comparison.
        :param complex_logic: Optional complex logic string to combine query conditions.
        :return: A QueryLike object representing the video query.
        """
        # Define mapping for video-related fields.
        topVarNest = {
            "index": "index",
            "file_folder": {
                "video_file": "file_folder.video_file",
                "transcripts": {
                    "json": "file_folder.transcripts.json",
                    "srt": "file_folder.transcripts.srt",
                    "tsv": "file_folder.transcripts.tsv",
                    "txt": "file_folder.transcripts.txt",
                    "vtt": "file_folder.transcripts.vtt"
                }
            },
            "metadata": {
                "url": "metadata.url",
                "aweme_id": "metadata.aweme_id",
                "sec_uid": "metadata.sec_uid",
                "video_metadata": {
                    "primary": {
                        "item_title": "metadata.video_metadata.primary.item_title",
                        "caption": "metadata.video_metadata.primary.caption",
                        "desc": "metadata.video_metadata.primary.desc"
                    },
                    "basic_temporal": {
                        "create_time": "metadata.video_metadata.basic_temporal.create_time",
                        "duration": "metadata.video_metadata.basic_temporal.duration"
                    },
                    "engagement": {
                        "play_count": "metadata.video_metadata.engagement.play_count",
                        "digg_count": "metadata.video_metadata.engagement.digg_count",
                        "comment_count": "metadata.video_metadata.engagement.comment_count",
                        "share_count": "metadata.video_metadata.engagement.share_count",
                        "admire_count": "metadata.video_metadata.engagement.admire_count",
                        "collect_count": "metadata.video_metadata.engagement.collect_count"
                    },
                    "additional": {
                        "ocr_content": "metadata.video_metadata.additional.ocr_content",
                        "text_extra": {
                            "caption_end": "metadata.video_metadata.additional.text_extra.caption_end",
                            "caption_start": "metadata.video_metadata.additional.text_extra.caption_start",
                            "end": "metadata.video_metadata.additional.text_extra.end",
                            "hashtag_id": "metadata.video_metadata.additional.text_extra.hashtag_id",
                            "hashtag_name": "metadata.video_metadata.additional.text_extra.hashtag_name",
                            "is_commerce": "metadata.video_metadata.additional.text_extra.is_commerce",
                            "start": "metadata.video_metadata.additional.text_extra.start",
                            "type": "metadata.video_metadata.additional.text_extra.type"
                        },
                        "video_tag": {
                            "level": "metadata.video_metadata.additional.video_tag.level",
                            "tag_id": "metadata.video_metadata.additional.video_tag.tag_id",
                            "tag_name": "metadata.video_metadata.additional.video_tag.tag_name"
                        }
                    }
                }
            },
            "comments": {
                "sec_uid": "comments.sec_uid",
                "comment_data": {
                    "cid": "comments.comment_data.cid",
                    "content": "comments.comment_data.content",
                    "metadata": {
                        "temporal": {
                            "create_time": "comments.comment_data.metadata.temporal.create_time"
                        },
                        "engagement": {
                            "digg_count": "comments.comment_data.metadata.engagement.digg_count",
                            "reply_comment_total": "comments.comment_data.metadata.engagement.reply_comment_total"
                        },
                        "reaction_flags": {
                            "user_digged": "comments.comment_data.metadata.reaction_flags.user_digged",
                            "is_author_digged": "comments.comment_data.metadata.reaction_flags.is_author_digged"
                        },
                        "comment_flags": {
                            "is_hot": "comments.comment_data.metadata.comment_flags.is_hot",
                            "is_note_comment": "comments.comment_data.metadata.comment_flags.is_note_comment"
                        }
                    },
                    "replies": {
                        "sec_uid": "comments.comment_data.replies.sec_uid",
                        "cid": "comments.comment_data.replies.cid",
                        "text": "comments.comment_data.replies.text",
                        "create_time": "comments.comment_data.replies.create_time",
                        "digg_count": "comments.comment_data.replies.digg_count",
                        "user_digged": "comments.comment_data.replies.user_digged",
                        "is_author_digged": "comments.comment_data.replies.is_author_digged",
                        "is_hot": "comments.comment_data.replies.is_hot",
                        "is_note_comment": "comments.comment_data.replies.is_note_comment"
                    }
                }
            },
            "search_query": "search_query"
        }
        unambiguous_vars_strict = {
            "index": "index",
            "file_folder": "file_folder",
            "comments": "comments",
            "search_query": "search_query",
            "video_file": "file_folder.video_file",
            "transcripts": "file_folder.transcripts",
            "json": "file_folder.transcripts.json",
            "srt": "file_folder.transcripts.srt",
            "tsv": "file_folder.transcripts.tsv",
            "txt": "file_folder.transcripts.txt",
            "vtt": "file_folder.transcripts.vtt",
            "url": "metadata.url",
            "aweme_id": "metadata.aweme_id",
            "video_metadata": "metadata.video_metadata",
            "comment_data": "comments.comment_data",
            "replies": "comments.comment_data.replies",
            "primary": "metadata.video_metadata.primary",
            "basic_temporal": "metadata.video_metadata.basic_temporal",
            "additional": "metadata.video_metadata.additional",
            "content": "comments.comment_data.content",
            "item_title": "metadata.video_metadata.primary.item_title",
            "caption": "metadata.video_metadata.primary.caption",
            "desc": "metadata.video_metadata.primary.desc",
            "duration": "metadata.video_metadata.basic_temporal.duration",
            "play_count": "metadata.video_metadata.engagement.play_count",
            "comment_count": "metadata.video_metadata.engagement.comment_count",
            "share_count": "metadata.video_metadata.engagement.share_count",
            "admire_count": "metadata.video_metadata.engagement.admire_count",
            "collect_count": "metadata.video_metadata.engagement.collect_count",
            "ocr_content": "metadata.video_metadata.additional.ocr_content",
            "text_extra": "metadata.video_metadata.additional.text_extra",
            "video_tag": "metadata.video_metadata.additional.video_tag",
            "temporal": "comments.comment_data.metadata.temporal",
            "reaction_flags": "comments.comment_data.metadata.reaction_flags",
            "comment_flags": "comments.comment_data.metadata.comment_flags",
            "text": "comments.comment_data.replies.text",
            "caption_end": "metadata.video_metadata.additional.text_extra.caption_end",
            "caption_start": "metadata.video_metadata.additional.text_extra.caption_start",
            "end": "metadata.video_metadata.additional.text_extra.end",
            "hashtag_id": "metadata.video_metadata.additional.text_extra.hashtag_id",
            "hashtag_name": "metadata.video_metadata.additional.text_extra.hashtag_name",
            "is_commerce": "metadata.video_metadata.additional.text_extra.is_commerce",
            "start": "metadata.video_metadata.additional.text_extra.start",
            "type": "metadata.video_metadata.additional.text_extra.type",
            "level": "metadata.video_metadata.additional.video_tag.level",
            "tag_id": "metadata.video_metadata.additional.video_tag.tag_id",
            "tag_name": "metadata.video_metadata.additional.video_tag.tag_name",
            "reply_comment_total": "comments.comment_data.metadata.engagement.reply_comment_total"
        }
        unambiguous_vars_nicknames = {
            "index": "index",
            "file_folder": "file_folder",
            "files": "file_folder",
            "comments": "comments",
            "search_query": "search_query",
            "search": "search_query",
            "query": "search_query",
            "s_query": "search_query",
            "s_q": "search_query",
            "video_file": "file_folder.video_file",
            "video": "file_folder.video_file",
            "transcripts": "file_folder.transcripts",
            "scripts": "file_folder.transcripts",
            "json": "file_folder.transcripts.json",
            "srt": "file_folder.transcripts.srt",
            "tsv": "file_folder.transcripts.tsv",
            "txt": "file_folder.transcripts.txt",
            "vtt": "file_folder.transcripts.vtt",
            "url": "metadata.url",
            "aweme_id": "metadata.aweme_id",
            "aweme": "metadata.aweme_id",
            "video_id": "metadata.aweme_id",
            "vid_id": "metadata.aweme_id",
            "video_metadata": "metadata.video_metadata",
            "video_meta": "metadata.video_metadata",
            "v_metadata": "metadata.video_metadata",
            "v_meta": "metadata.video_metadata",
            "v_meta_data": "metadata.video_metadata",
            "v_data": "metadata.video_metadata",
            "comment_data": "comments.comment_data",
            "c_data": "comments.comment_data",
            "replies": "comments.comment_data.replies",
            "primary": "metadata.video_metadata.primary",
            "basic_temporal": "metadata.video_metadata.basic_temporal",
            "b_temporal": "metadata.video_metadata.basic_temporal",
            "b_temp": "metadata.video_metadata.basic_temporal",
            "additional": "metadata.video_metadata.additional",
            "add": "metadata.video_metadata.additional",
            "content": "comments.comment_data.content",
            "item_title": "metadata.video_metadata.primary.item_title",
            "title": "metadata.video_metadata.primary.item_title",
            "caption": "metadata.video_metadata.primary.caption",
            "desc": "metadata.video_metadata.primary.desc",
            "description": "metadata.video_metadata.primary.desc",
            "duration": "metadata.video_metadata.basic_temporal.duration",
            "dur": "metadata.video_metadata.basic_temporal.duration",
            "play_count": "metadata.video_metadata.engagement.play_count",
            "play": "metadata.video_metadata.engagement.play_count",
            "plays": "metadata.video_metadata.engagement.play_count",
            "comment_count": "metadata.video_metadata.engagement.comment_count",
            "c_count": "metadata.video_metadata.engagement.comment_count",
            "share_count": "metadata.video_metadata.engagement.share_count",
            "shares": "metadata.video_metadata.engagement.share_count",
            "share": "metadata.video_metadata.engagement.share_count",
            "s_count": "metadata.video_metadata.engagement.share_count",
            "admire_count": "metadata.video_metadata.engagement.admire_count",
            "admire": "metadata.video_metadata.engagement.admire_count",
            "adm": "metadata.video_metadata.engagement.admire_count",
            "collect_count": "metadata.video_metadata.engagement.collect_count",
            "collect": "metadata.video_metadata.engagement.collect_count",
            "collects": "metadata.video_metadata.engagement.collect_count",
            "collects_count": "metadata.video_metadata.engagement.collect_count",
            "ocr_content": "metadata.video_metadata.additional.ocr_content",
            "ocr": "metadata.video_metadata.additional.ocr_content",
            "text_extra": "metadata.video_metadata.additional.text_extra",
            "textra": "metadata.video_metadata.additional.text_extra",
            "t_extra": "metadata.video_metadata.additional.text_extra",
            "video_tag": "metadata.video_metadata.additional.video_tag",
            "v_tag": "metadata.video_metadata.additional.video_tag",
            "tag": "metadata.video_metadata.additional.video_tag",
            "vid_tag": "metadata.video_metadata.additional.video_tag",
            "video_tags": "metadata.video_metadata.additional.video_tag",
            "tags": "metadata.video_metadata.additional.video_tag",
            "v_tags": "metadata.video_metadata.additional.video_tag",
            "temporal": "comments.comment_data.metadata.temporal",
            "c_temporal": "comments.comment_data.metadata.temporal",
            "c_temp": "comments.comment_data.metadata.temporal",
            "reaction_flags": "comments.comment_data.metadata.reaction_flags",
            "reaction": "comments.comment_data.metadata.reaction_flags",
            "r_flags": "comments.comment_data.metadata.reaction_flags",
            "comment_flags": "comments.comment_data.metadata.comment_flags",
            "c_flags": "comments.comment_data.metadata.comment_flags",
            "text": "comments.comment_data.replies.text",
            "caption_end": "metadata.video_metadata.additional.text_extra.caption_end",
            "c_end": "metadata.video_metadata.additional.text_extra.caption_end",
            "caption_start": "metadata.video_metadata.additional.text_extra.caption_start",
            "c_start": "metadata.video_metadata.additional.text_extra.caption_start",
            "end": "metadata.video_metadata.additional.text_extra.end",
            "hashtag_id": "metadata.video_metadata.additional.text_extra.hashtag_id",
            "htag_id": "metadata.video_metadata.additional.text_extra.hashtag_id",
            "ht_id": "metadata.video_metadata.additional.text_extra.hashtag_id",
            "htid": "metadata.video_metadata.additional.text_extra.hashtag_id",
            "hashtag_name": "metadata.video_metadata.additional.text_extra.hashtag_name",
            "hashtag": "metadata.video_metadata.additional.text_extra.hashtag_name",
            "htag_name": "metadata.video_metadata.additional.text_extra.hashtag_name",
            "hname": "metadata.video_metadata.additional.text_extra.hashtag_name",
            "htname": "metadata.video_metadata.additional.text_extra.hashtag_name",
            "ht_name": "metadata.video_metadata.additional.text_extra.hashtag_name",
            "is_commerce": "metadata.video_metadata.additional.text_extra.is_commerce",
            "commerce": "metadata.video_metadata.additional.text_extra.is_commerce",
            "start": "metadata.video_metadata.additional.text_extra.start",
            "type": "metadata.video_metadata.additional.text_extra.type",
            "level": "metadata.video_metadata.additional.video_tag.level",
            "tag_id": "metadata.video_metadata.additional.video_tag.tag_id",
            "tag_name": "metadata.video_metadata.additional.video_tag.tag_name",
            "reply_comment_total": "comments.comment_data.metadata.engagement.reply_comment_total",
            "r_total": "comments.comment_data.metadata.engagement.reply_comment_total",
            "r_count": "comments.comment_data.metadata.engagement.reply_comment_total",
            "rcount": "comments.comment_data.metadata.engagement.reply_comment_total"
        }
        ambiguous_var_choices = {
            "sec_uid": "metadata.sec_uid",
            "engagement": "metadata.video_metadata.engagement",
            "cid": "comments.comment_data.cid",
            "create_time": "metadata.video_metadata.basic_temporal.create_time",
            "digg_count": "metadata.video_metadata.engagement.digg_count",
            "metadata": "metadata",
            "reaction_flags": "comments.comment_data.metadata.reaction_flags",
            "comment_flags": "comments.comment_data.metadata.comment_flags",
            "user_digged": "comments.comment_data.metadata.reaction_flags.user_digged",
            "is_author_digged": "comments.comment_data.metadata.reaction_flags.is_author_digged",
            "is_hot": "comments.comment_data.metadata.comment_flags.is_hot",
            "is_note_comment": "comments.comment_data.metadata.comment_flags.is_note_comment"
        }

        return Database.guery_constructor(queryObj, variables, values, and_or, operatorCode, topVarNest,
                                          unambiguous_vars_strict, unambiguous_vars_nicknames, ambiguous_var_choices,
                                          cast_data, complex_logic)

    @staticmethod
    def user_query_constructor(queryObj: Query,
                               variables: str | List[str],
                               values: Any | List[Any],
                               and_or: Optional[AO | str] = AO.AND,
                               operatorCode: Optional[GeneralOperators] = "==",
                               cast_data: bool = True,
                               complex_logic: str = None) -> QueryLike:
        """
        Construct a query for user records using pre-defined nested mappings for user metadata.

        :param queryObj: The base Query object.
        :param variables: A string or list of variable names to filter on.
        :param values: A value or list of values corresponding to the variables.
        :param and_or: Logical connector (AO.AND or AO.OR) or their string equivalents.
        :param operatorCode: Operator(s) to use for comparing values.
        :param cast_data: If True, cast data types during comparison.
        :param complex_logic: Optional complex logic string to combine query conditions.
        :return: A QueryLike object representing the user query.
        """
        # Define mapping for user-related fields.
        topVarNest = {
            "index": "index",
            "sec_uid": "sec_uid",
            "user_data": {
                "personal_info": {
                    "nickname": "user_data.personal_info.nickname",
                    "short_id": "user_data.personal_info.short_id",
                    "signature": "user_data.personal_info.signature",
                    "user_age": "user_data.personal_info.user_age",
                    "gender": "user_data.personal_info.gender"
                },
                "location_and_verification": {
                    "city": "user_data.location_and_verification.city",
                    "district": "user_data.location_and_verification.district",
                    "country": "user_data.location_and_verification.country",
                    "ip_location": "user_data.location_and_verification.ip_location",
                    "account_cert_info": "user_data.location_and_verification.account_cert_info",
                    "custom_verify": "user_data.location_and_verification.custom_verify"
                },
                "statistics": {
                    "video_count": "user_data.statistics.video_count",
                    "follower_count": "user_data.statistics.follower_count",
                    "following_count": "user_data.statistics.following_count",
                    "forward_count": "user_data.statistics.forward_count",
                    "total_favorited": "user_data.statistics.total_favorited"
                },
                "flags": {
                    "is_activity_user": "user_data.flags.is_activity_user",
                    "is_ban": "user_data.flags.is_ban",
                    "is_gov_media_vip": "user_data.flags.is_gov_media_vip",
                    "is_im_oversea_user": "user_data.flags.is_im_oversea_user",
                    "is_star": "user_data.flags.is_star"
                },
                "identifiers": {
                    "uid": "user_data.identifiers.uid",
                    "unique_id": "user_data.identifiers.unique_id"
                }
            },
            "videos": {
                "aweme_id": "videos.aweme_id"
            },
            "comments": {
                "aweme_id": "comments.aweme_id",
                "comment_id": "comments.comment_id"
            }
        }
        unambiguous_vars_strict = {
            "index": "index",
            "sec_uid": "sec_uid",
            "user_data": "user_data",
            "personal_info": "user_data.personal_info",
            "location_and_verification": "user_data.location_and_verification",
            "statistics": "user_data.statistics",
            "flags": "user_data.flags",
            "identifiers": "user_data.identifiers",
            "videos": "videos",
            "comments": "comments",
            "nickname": "user_data.personal_info.nickname",
            "short_id": "user_data.personal_info.short_id",
            "signature": "user_data.personal_info.signature",
            "user_age": "user_data.personal_info.user_age",
            "gender": "user_data.personal_info.gender",
            "city": "user_data.location_and_verification.city",
            "district": "user_data.location_and_verification.district",
            "country": "user_data.location_and_verification.country",
            "ip_location": "user_data.location_and_verification.ip_location",
            "account_cert_info": "user_data.location_and_verification.account_cert_info",
            "custom_verify": "user_data.location_and_verification.custom_verify",
            "video_count": "user_data.statistics.video_count",
            "follower_count": "user_data.statistics.follower_count",
            "following_count": "user_data.statistics.following_count",
            "forward_count": "user_data.statistics.forward_count",
            "total_favorited": "user_data.statistics.total_favorited",
            "is_activity_user": "user_data.flags.is_activity_user",
            "is_ban": "user_data.flags.is_ban",
            "is_gov_media_vip": "user_data.flags.is_gov_media_vip",
            "is_im_oversea_user": "user_data.flags.is_im_oversea_user",
            "is_star": "user_data.flags.is_star",
            "uid": "user_data.identifiers.uid",
            "unique_id": "user_data.identifiers.unique_id",
            "comment_id": "comments.comment_id"
        }
        unambiguous_vars_nicknames = {
            "index": "index",
            "sec_uid": "sec_uid",
            "secuid": "sec_uid",
            "s_uid": "sec_uid",
            "suid": "sec_uid",
            "user_data": "user_data",
            "user": "user_data",
            "u_data": "user_data",
            "udata": "user_data",
            "personal_info": "user_data.personal_info",
            "personal": "user_data.personal_info",
            "p_info": "user_data.personal_info",
            "pinfo": "user_data.personal_info",
            "location_and_verification": "user_data.location_and_verification",
            "location": "user_data.location_and_verification",
            "location_verification": "user_data.location_and_verification",
            "location_verif": "user_data.location_and_verification",
            "location_ver": "user_data.location_and_verification",
            "location_veri": "user_data.location_and_verification",
            "loc_ver": "user_data.location_and_verification",
            "statistics": "user_data.statistics",
            "stat": "user_data.statistics",
            "statistic": "user_data.statistics",
            "stats": "user_data.statistics",
            "flags": "user_data.flags",
            "identifiers": "user_data.identifiers",
            "id": "user_data.identifiers",
            "ident": "user_data.identifiers",
            "identif": "user_data.identifiers",
            "idents": "user_data.identifiers",
            "videos": "videos",
            "vids": "videos",
            "vid": "videos",
            "comments": "comments",
            "cmt": "comments",
            "cmt_data": "comments",
            "c_data": "comments",
            "comms": "comments",
            "nickname": "user_data.personal_info.nickname",
            "nick": "user_data.personal_info.nickname",
            "nick_name": "user_data.personal_info.nickname",
            "name": "user_data.personal_info.nickname",
            "short_id": "user_data.personal_info.short_id",
            "shortid": "user_data.personal_info.short_id",
            "short": "user_data.personal_info.short_id",
            "s_id": "user_data.personal_info.short_id",
            "sid": "user_data.personal_info.short_id",
            "signature": "user_data.personal_info.signature",
            "sig": "user_data.personal_info.signature",
            "sign": "user_data.personal_info.signature",
            "user_age": "user_data.personal_info.user_age",
            "age": "user_data.personal_info.user_age",
            "u_age": "user_data.personal_info.user_age",
            "gender": "user_data.personal_info.gender",
            "gen": "user_data.personal_info.gender",
            "city": "user_data.location_and_verification.city",
            "district": "user_data.location_and_verification.district",
            "dist": "user_data.location_and_verification.district",
            "country": "user_data.location_and_verification.country",
            "cnt": "user_data.location_and_verification.country",
            "ip_location": "user_data.location_and_verification.ip_location",
            "ip": "user_data.location_and_verification.ip_location",
            "iploc": "user_data.location_and_verification.ip_location",
            "ip_loc": "user_data.location_and_verification.ip_location",
            "account_cert_info": "user_data.location_and_verification.account_cert_info",
            "account_cert": "user_data.location_and_verification.account_cert_info",
            "acc_cert_info": "user_data.location_and_verification.account_cert_info",
            "acc_cert": "user_data.location_and_verification.account_cert_info",
            "custom_verify": "user_data.location_and_verification.custom_verify",
            "custom": "user_data.location_and_verification.custom_verify",
            "c_verify": "user_data.location_and_verification.custom_verify",
            "c_ver": "user_data.location_and_verification.custom_verify",
            "c_verif": "user_data.location_and_verification.custom_verify",
            "c_verification": "user_data.location_and_verification.custom_verify",
            "video_count": "user_data.statistics.video_count",
            "vid_count": "user_data.statistics.video_count",
            "v_count": "user_data.statistics.video_count",
            "follower_count": "user_data.statistics.follower_count",
            "follower": "user_data.statistics.follower_count",
            "following_count": "user_data.statistics.following_count",
            "following": "user_data.statistics.following_count",
            "forward_count": "user_data.statistics.forward_count",
            "forward": "user_data.statistics.forward_count",
            "total_favorited": "user_data.statistics.total_favorited",
            "total_fav": "user_data.statistics.total_favorited",
            "is_activity_user": "user_data.flags.is_activity_user",
            "is_activity": "user_data.flags.is_activity_user",
            "is_act": "user_data.flags.is_activity_user",
            "is_ban": "user_data.flags.is_ban",
            "is_banned": "user_data.flags.is_ban",
            "is_banned_user": "user_data.flags.is_ban",
            "banned": "user_data.flags.is_ban",
            "ban": "user_data.flags.is_ban",
            "is_gov_media_vip": "user_data.flags.is_gov_media_vip",
            "is_gov_media": "user_data.flags.is_gov_media_vip",
            "is_gov": "user_data.flags.is_gov_media_vip",
            "is_media": "user_data.flags.is_gov_media_vip",
            "is_media_vip": "user_data.flags.is_gov_media_vip",
            "is_media_vip_user": "user_data.flags.is_gov_media_vip",
            "is_media_user": "user_data.flags.is_gov_media_vip",
            "is_vip": "user_data.flags.is_gov_media_vip",
            "is_vip_user": "user_data.flags.is_gov_media_vip",
            "is_vip_media": "user_data.flags.is_gov_media_vip",
            "is_vip_media_user": "user_data.flags.is_gov_media_vip",
            "gov_media": "user_data.flags.is_gov_media_vip",
            "gov_media_vip": "user_data.flags.is_gov_media_vip",
            "gov": "user_data.flags.is_gov_media_vip",
            "media": "user_data.flags.is_gov_media_vip",
            "media_vip": "user_data.flags.is_gov_media_vip",
            "vip": "user_data.flags.is_gov_media_vip",
            "is_im_oversea_user": "user_data.flags.is_im_oversea_user",
            "is_im_oversea": "user_data.flags.is_im_oversea_user",
            "is_im": "user_data.flags.is_im_oversea_user",
            "is_oversea": "user_data.flags.is_im_oversea_user",
            "is_oversea_user": "user_data.flags.is_im_oversea_user",
            "oversea": "user_data.flags.is_im_oversea_user",
            "oversea_user": "user_data.flags.is_im_oversea_user",
            "is_star": "user_data.flags.is_star",
            "star": "user_data.flags.is_star",
            "uid": "user_data.identifiers.uid",
            "unique_id": "user_data.identifiers.unique_id",
            "comment_id": "comments.comment_id",
            "cid": "comments.comment_id",
            "c_id": "comments.comment_id"
        }
        ambiguous_var_choices = {
            "aweme_id": "videos.aweme_id",
        }
        return Database.guery_constructor(queryObj, variables, values, and_or, operatorCode, topVarNest,
                                          unambiguous_vars_strict, unambiguous_vars_nicknames, ambiguous_var_choices,
                                          cast_data, complex_logic)

    def search(self,
               data_to_query: tdTab,
               queryObj: Query,
               variables: str | List[str],
               values: Any | List[Any],
               and_or: Optional[AO | str] = AO.AND,
               operatorCode: Optional[GeneralOperators] = "==",
               cast_data: bool = True,
               complex_logic: str = None) -> List[Dict[str, Any]]:
        """
        Execute a query on a specified TinyDB table.

        :param data_to_query: The TinyDB table to search.
        :param queryObj: The base Query object for constructing query expressions.
        :param variables: A variable name or list of names to filter on.
        :param values: A value or list of values corresponding to the variables.
        :param and_or: Logical connector (AO.AND or AO.OR).
        :param operatorCode: Operator(s) (e.g., "==", "<") to use for comparisons.
        :param cast_data: If True, cast data types before comparison.
        :param complex_logic: Optional complex logic string for combining conditions.
        :return: A list of records matching the constructed query.
        """
        if data_to_query is self.video_table:
            # Construct a query for video records.
            q_con = Database.video_query_constructor
        elif data_to_query is self.user_table:
            # Construct a query for user records.
            q_con = Database.user_query_constructor
        else:
            raise ValueError("data_to_query must be either video_table or user_table.")

        query = q_con(queryObj, variables, values, and_or, operatorCode, cast_data,
                                             complex_logic)
        print(f"Query: {query}")
        return data_to_query.search(query)

    def search_videos(self,
                      variables: str | List[str],
                      values: Any | List[Any],
                      and_or: Optional[AO | str] = AO.AND,
                      operatorCode: Optional[GeneralOperators] = "==",
                      cast_data: bool = True,
                      complex_logic: str = None) -> List[Dict[str, Any]]:
        """
        Query the video table for records matching specified conditions.

        :param variables: A variable name or list of names to filter on.
        :param values: A value or list of values corresponding to the variables.
        :param and_or: Logical connector (AO.AND or AO.OR).
        :param operatorCode: Operator(s) to use for comparisons.
        :param cast_data: If True, cast data types before comparing.
        :param complex_logic: Optional string for complex condition logic.
        :return: A list of video records that match the query.
        """
        return self.search(self.video_table, self.query, variables, values, and_or, operatorCode, cast_data,
                           complex_logic)

    def search_users(self,
                     variables: str | List[str],
                     values: Any | List[Any],
                     and_or: Optional[AO | str] = AO.AND,
                     operatorCode: Optional[GeneralOperators] = "==",
                     cast_data: bool = True,
                     complex_logic: str = None) -> List[Dict[str, Any]]:
        """
        Query the database for users
        :param variables:
        :param values:
        :param and_or:
        :param operatorCode:
        :param cast_data:
        :param complex_logic:
        :return:
        """
        return self.search(self.user_table, self.query, variables, values, and_or, operatorCode, cast_data,
                           complex_logic)

    def create_video_data_csv(self, to_excel: bool = False):
        """
        Export video metadata to a CSV or Excel file.

        :param to_excel: If True, export the data as an Excel file (.xlsx); otherwise, export as CSV (.csv).
        :return: None
        """
        # Load video data from the JSON database.
        with open(self.full_path, "r", encoding="utf-8") as f:
            data = json.load(f)["Videos"]
        # Define the structure for the DataFrame.
        pdData = {
            "index": [],
            "url": [],
            "aweme_id": [],
            "sec_uid": [],
            "item_title": [],
            "caption": [],
            "desc": [],
            "create_time": [],
            "duration": [],
            "play_count": [],
            "digg_count": [],
            "comment_count": [],
            "share_count": [],
            "admire_count": [],
            "collect_count": [],
            "ocr_content": [],
            "search_query": []
        }
        iter_vars = [
            "index", "metadata.url", "metadata.aweme_id", "metadata.sec_uid",
            "metadata.video_metadata.primary.item_title",
            "metadata.video_metadata.primary.caption", "metadata.video_metadata.primary.desc",
            "metadata.video_metadata.basic_temporal.create_time", "metadata.video_metadata.basic_temporal.duration",
            "metadata.video_metadata.engagement.play_count", "metadata.video_metadata.engagement.digg_count",
            "metadata.video_metadata.engagement.comment_count", "metadata.video_metadata.engagement.share_count",
            "metadata.video_metadata.engagement.admire_count", "metadata.video_metadata.engagement.collect_count",
            "metadata.video_metadata.additional.ocr_content", "search_query"
        ]

        # Extract data for each video record.
        for key in sorted(data.keys()):
            video = data[key]
            for var in iter_vars:
                # get the value of the variable
                val = recursive_fetch(video, var)
                # append the value to the list
                pdData[var.split(".")[-1]].append(val if not isinstance(val, str) else val.replace("\n", ""))
        # Create a dataframe
        df = pd.DataFrame(pdData)
        # Ensure the output folder exists.
        csv_folder = "C:/Users/schif/Documents/Coding/Yanjun/Database/CSVs"
        os.makedirs(csv_folder, exist_ok=True)
        csv_file = csv_folder + "/video_data" + (".xlsx" if to_excel else ".csv")
        df.to_csv(csv_file, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL) if not to_excel else df.to_excel(
            csv_file, index=False)
        print(f"Data exported to {csv_file}")

    def create_user_data_csv(self, to_excel: bool = False):
        """
        Export user metadata to a CSV or Excel file.

        :param to_excel: If True, export the data as an Excel file (.xlsx); otherwise, export as CSV (.csv).
        :return: None
        """
        # Load video data from the JSON database.
        with open(self.full_path, "r", encoding="utf-8") as f:
            data = json.load(f)["Users"]
        # Define the structure for the DataFrame.
        pdData = {
            "index": [],
            "sec_uid": [],
            "nickname": [],
            "short_id": [],
            "signature": [],
            "user_age": [],
            "gender": [],
            "city": [],
            "district": [],
            "country": [],
            "ip_location": [],
            "account_cert_info": [],
            "custom_verify": [],
            "video_count": [],
            "follower_count": [],
            "following_count": [],
            "forward_count": [],
            "total_favorited": [],
            "is_activity_user": [],
            "is_ban": [],
            "is_gov_media_vip": [],
            "is_im_oversea_user": [],
            "is_star": [],
            "uid": [],
            "unique_id": [],
            "videos": []
        }
        iter_vars = [
            "index", "sec_uid", "user_data.personal_info.nickname", "user_data.personal_info.short_id",
            "user_data.personal_info.signature", "user_data.personal_info.user_age", "user_data.personal_info.gender",
            "user_data.location_and_verification.city", "user_data.location_and_verification.district",
            "user_data.location_and_verification.country", "user_data.location_and_verification.ip_location",
            "user_data.location_and_verification.account_cert_info",
            "user_data.location_and_verification.custom_verify",
            "user_data.statistics.video_count", "user_data.statistics.follower_count",
            "user_data.statistics.following_count", "user_data.statistics.forward_count",
            "user_data.statistics.total_favorited", "user_data.flags.is_activity_user",
            "user_data.flags.is_ban", "user_data.flags.is_gov_media_vip", "user_data.flags.is_im_oversea_user",
            "user_data.flags.is_star", "user_data.identifiers.uid", "user_data.identifiers.unique_id",
            "videos.aweme_id"
        ]

        # Extract data for each user record.
        for key in sorted(data.keys()):
            user = data[key]
            for var in iter_vars:
                # get the value of the variable
                val = recursive_fetch(user, var)
                # append the value to the list
                if var == "videos.aweme_id":
                    var += ".videos"
                pdData[var.split(".")[-1]].append(val if not isinstance(val, str) else val.replace("\n", ""))
        # create a dataframe
        df = pd.DataFrame(pdData)
        # Ensure the output folder exists.
        csv_folder = "C:/Users/schif/Documents/Coding/Yanjun/Database/CSVs"
        os.makedirs(csv_folder, exist_ok=True)
        csv_file = csv_folder + "/user_data" + (".xlsx" if to_excel else ".csv")
        df.to_csv(csv_file, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL) if not to_excel else df.to_excel(
            csv_file, index=False)
        print(f"Data exported to {csv_file}")

    def get_comment_data_csv(self, to_excel: bool = False):
        """
        Export comment metadata for each video to individual CSV or Excel files.

        :param to_excel: If True, export as Excel files (.xlsx); otherwise, export as CSV (.csv).
        :return: None
        """
        #  Load video data from the JSON database.
        with open(self.full_path, "r", encoding="utf-8") as f:
            data = json.load(f)["Videos"]

        # For each video, export its comments.
        for index, key in enumerate(data.keys()):
            video = data[key]
            # Define the structure for the DataFrame.
            pdData = {
                "sec_uid": [],
                "cid": [],
                "content": [],
                "create_time": [],
                "digg_count": [],
                "reply_comment_total": [],
                "user_digged": [],
                "is_author_digged": [],
                "is_hot": [],
                "is_note_comment": []
            }
            iter_vars = [
                "sec_uid", "comment_data.cid", "comment_data.content",
                "comment_data.metadata.temporal.create_time",
                "comment_data.metadata.engagement.digg_count",
                "comment_data.metadata.engagement.reply_comment_total",
                "comment_data.metadata.reaction_flags.user_digged",
                "comment_data.metadata.reaction_flags.is_author_digged",
                "comment_data.metadata.comment_flags.is_hot",
                "comment_data.metadata.comment_flags.is_note_comment"
            ]

            # Extract comment data for the current video.
            for comment in video["comments"]:
                for var in iter_vars:
                    # get the value of the variable
                    val = recursive_fetch(comment, var)
                    # append the value to the list
                    # remove newline characters from the value if string
                    pdData[var.split(".")[-1]].append(val if not isinstance(val, str) else val.replace("\n", ""))
            # create a dataframe
            df = pd.DataFrame(pdData)
            # Ensure the output folder exists.
            csv_folder = "C:/Users/schif/Documents/Coding/Yanjun/Database/CSVs/Comments"
            os.makedirs(csv_folder, exist_ok=True)
            name = f"comment_data_{index}.csv" if not to_excel else f"comment_data_{index}.xlsx"
            csv_file = csv_folder + "/" + name
            df.to_csv(csv_file, index=False, encoding="utf-8",
                      quoting=csv.QUOTE_MINIMAL) if not to_excel else df.to_excel(csv_file, index=False)
            print(f"Data exported to {csv_file}")

    def get_data_keys(self, table_name: str) -> List[str]:
        """
        Retrieve the keys from the first record of a specified table.

        :param table_name: The name of the table ("Videos" or "Users").
        :return: A list of keys (using dot notation for nested keys).
        :raises ValueError: If the specified table does not exist.
        """
        table = self.video_table if table_name == "Videos" else self.user_table if table_name == "Users" else None
        if table is None:
            raise ValueError(f"Table {table_name} does not exist")
        return get_data_keys(table)

    def get_data_key_type_dict(self, table_name: str) -> Dict[str, type]:
        """
        Construct a dictionary mapping each key in the specified table to its data type.

        :param table_name: The name of the table ("Videos" or "Users").
        :return: A dictionary with keys as field names and values as the corresponding data types.
        """
        try:
            keys = self.get_data_keys(table_name)
            d = {key: type(recursive_fetch(self.db.table(table_name).all()[0], key)) for key in keys}
            return d
        except Exception as e:
            print(f"Error: {e}")
            return {}

    def data_to_csv(self,
                    table_name: str,
                    query: Optional[QueryLike] = None,
                    preIndices: Optional[Union[List[int], str, slice]] = None,
                    postIndices: Optional[Union[List[int], str, slice]] = None,
                    to_excel: bool = False,
                    basePath: str = None) -> None:
        """
        Convert the data from a given table to a CSV (or Excel) file.

        For table_name:
          - "Videos" or "Users" refer to the actual tables.
          - "comments" is a virtual table that, for each video record in the Videos table, exports its comments to a separate CSV file.

        preIndices is applied to the full dataset before filtering with query,
        and postIndices is applied after query filtering. They support either a list of indices,
        a slice string "a:b:c", or a slice object.

        :param table_name: "Videos", "Users", or "comments"
        :param query: A QueryLike object (callable) to filter records.
        :param preIndices: Indices (or slice) to select from the full dataset before applying the query.
        :param postIndices: Indices (or slice) to select from the query result.
        :param to_excel: If True, output to an Excel file instead of CSV.
        :param basePath: Base path for saving the file. If None, defaults to "C:/Users/schif/Documents/Coding/Yanjun/Database/CSVs".
        :return: None
        :raises ValueError: If the specified table does not exist.
        """
        # Use default base path if not provided
        if basePath is None:
            basePath = "C:/Users/schif/Documents/Coding/Yanjun/Database/CSVs"
        os.makedirs(basePath, exist_ok=True)

        # Define a helper to flatten a record (assuming you already have a flatten_dict function)
        def flatten_record(rec: dict) -> dict:
            return flatten_dict(rec)

        # Handle the case for Videos or Users
        # noinspection DuplicatedCode
        if table_name.lower() in ["videos", "users"]:
            table = None
            if table_name.lower() == "videos":
                table = self.video_table
            elif table_name.lower() == "users":
                table = self.user_table

            data = table.all()
            full_length = len(data)
            if full_length == 0:
                print("No data to export.")
                return

            # Apply preIndices on full dataset using your external parseIndices
            pre_idx = parse_indices(preIndices, full_length)
            data = [data[i] for i in pre_idx]

            # Apply query if provided
            if query is not None:
                data = [record for record in data if query(record)]

            # Apply postIndices on the filtered data
            post_idx = parse_indices(postIndices, len(data))
            data = [data[i] for i in post_idx]

            if not data:
                print("No data found for CSV export after filtering.")
                return

            # Flatten each record
            flat_data = [flatten_record(rec) for rec in data]
            # Create a DataFrame and export
            df = pd.DataFrame(flat_data)
            filename = ("video_" if table_name.lower() == "videos" else "user_") + (
                "data.xlsx" if to_excel else "data.csv")
            filepath = os.path.join(basePath, filename)
            if to_excel:
                df.to_excel(filepath, index=False)
            else:
                df.to_csv(filepath, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
            print(f"Data exported to {filepath}")

        # Handle the virtual "comments" table
        elif table_name.lower() == "comments":
            video_data = self.video_table.all()
            # Create a subfolder for comments if it doesn't exist
            comments_folder = os.path.join(basePath, "Comments")
            os.makedirs(comments_folder, exist_ok=True)
            if not video_data:
                print("No video data available to export comments.")
                return

            # Iterate over each video record
            # noinspection DuplicatedCode
            for vid_record in video_data:
                # Optionally, you might want to filter each video's comments using query, preIndices, postIndices.
                comments = vid_record.get("comments", [])
                if not comments:
                    continue  # Skip videos without comments

                # Optionally apply preIndices to the list of comments for this video
                full_length = len(comments)
                pre_idx = parse_indices(preIndices, full_length)
                filtered_comments = [comments[i] for i in pre_idx]

                # Apply query if provided (assuming query is callable on a comment record)
                if query is not None:
                    filtered_comments = [comm for comm in filtered_comments if query(comm)]

                # Apply postIndices on the filtered comments
                post_idx = parse_indices(postIndices, len(filtered_comments))
                filtered_comments = [filtered_comments[i] for i in post_idx]

                if not filtered_comments:
                    continue

                # Flatten comments for this video
                flat_comments = [flatten_record(comm) for comm in filtered_comments]
                df = pd.DataFrame(flat_comments)
                # Construct a filename using the video's index or a unique identifier (e.g., aweme_id)
                vid_index = vid_record.get("index", "unknown")
                vid_aweme = vid_record.get("metadata", {}).get("aweme_id", "unknown")
                filename = f"comments_{vid_index}_{vid_aweme}.xlsx" if to_excel else f"comments_{vid_index}_{vid_aweme}.csv"
                filepath = os.path.join(comments_folder, filename)
                if to_excel:
                    df.to_excel(filepath, index=False)
                else:
                    df.to_csv(filepath, index=False, encoding="utf-8", quoting=csv.QUOTE_MINIMAL)
                print(f"Comments for video {vid_index} exported to {filepath}")

        else:
            raise ValueError(f"Table {table_name} does not exist.")


# noinspection DuplicatedCode
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
    dab = Database()
    """# test get video
    video = dab.get_video(0)
    print(f"Video: {video}")
    # test video query
    videos = dab.query_videos(dab.query, ["digg_count", "play_count"], [8000, 10000], aO.AND, ["<", "<"])
    print(f"Videos: {videos}")
    videos = dab.query_videos(dab.query, ["cid"], [7481513452818957113], aO.OR, ["=="])
    print(f"Videos: {videos}")"""

    """# test csv creation
    dab.create_video_data_csv()
    dab.create_user_data_csv()
    dab.get_comment_data_csv()
    # Test xlsx creation
    dab.create_video_data_csv(to_excel=True)
    dab.create_user_data_csv(to_excel=True)
    dab.get_comment_data_csv(to_excel=True)"""

    # test search with complex query
    videos = dab.search_videos(["play_count", "comment_count", "digg_count"],
                               [10000, 1000, 1000],
                               AO.AND,
                               ["<", ">", "<"],
                               True,
                               "((q)&((q)|(~(q))))")
    video_ids_and_check_data = [[f"aweme_id: {video["metadata"]["aweme_id"]}",
                                 f"play_count: {video["metadata"]["video_metadata"]["engagement"]["play_count"]}",
                                 f"comment_count: {video["metadata"]["video_metadata"]["engagement"]["comment_count"]}",
                                 f"digg_count: {video["metadata"]["video_metadata"]["engagement"]["digg_count"]}"] for
                                video in videos]
    print(f"Videos: {video_ids_and_check_data}")
    videos = dab.search_videos(["digg_count", "play_count"], [8000, 10000], AO.AND, ["<", "<"])
    video_ids_and_check_data = [[f"aweme_id: {video["metadata"]["aweme_id"]}",
                                 f"play_count: {video["metadata"]["video_metadata"]["engagement"]["play_count"]}",
                                 f"comment_count: {video["metadata"]["video_metadata"]["engagement"]["comment_count"]}",
                                 f"digg_count: {video["metadata"]["video_metadata"]["engagement"]["digg_count"]}"] for
                                video in videos]
    print(f"Videos: {video_ids_and_check_data}")
    videos = dab.search_videos(["digg_count", "play_count"], [8000, 10000], AO.AND, ["<", "<"], True, "((q)&(q))")
    video_ids_and_check_data = [[f"aweme_id: {video["metadata"]["aweme_id"]}",
                                 f"play_count: {video["metadata"]["video_metadata"]["engagement"]["play_count"]}",
                                 f"comment_count: {video["metadata"]["video_metadata"]["engagement"]["comment_count"]}",
                                 f"digg_count: {video["metadata"]["video_metadata"]["engagement"]["digg_count"]}"] for
                                video in videos]
    print(f"Videos: {video_ids_and_check_data}")
    any_comment_less = RelOperator(lambda a, b: any([len(comm["comment_data"]["content"]) < b for comm in a]))
    videos = dab.search_videos(["play_count", "comment_count", "digg_count", "comments"],
                               [10000, 1000, 1000, 10],
                               AO.AND,
                               ["<", ">", "<", any_comment_less],
                               False,
                               "(((q)|(q)|(~(q)))&(q))")
    video_ids_and_check_data = [[f"aweme_id: {video["metadata"]["aweme_id"]}",
                                 f"play_count: {video["metadata"]["video_metadata"]["engagement"]["play_count"]}",
                                 f"comment_count: {video["metadata"]["video_metadata"]["engagement"]["comment_count"]}",
                                 f"digg_count: {video["metadata"]["video_metadata"]["engagement"]["digg_count"]}",
                                 f"comment_length: {min([len(comment["comment_data"]["content"]) for comment in video["comments"]])}"]
                                for video in videos]
    print(f"Videos: {video_ids_and_check_data}")
    all_comment_greater = RelOperator(lambda a, b: all([len(comm["comment_data"]["content"]) > b for comm in a]))
    videos = dab.search_videos(["play_count", "comment_count", "digg_count", "comments"],
                               [10000, 1000, 1000, 4],
                               AO.AND,
                               ["<", ">", "<", all_comment_greater],
                               False,
                               "(((q)|(q)|(~(q)))&(q))")
    video_ids_and_check_data = [[f"aweme_id: {video["metadata"]["aweme_id"]}",
                                 f"play_count: {video["metadata"]["video_metadata"]["engagement"]["play_count"]}",
                                 f"comment_count: {video["metadata"]["video_metadata"]["engagement"]["comment_count"]}",
                                 f"digg_count: {video["metadata"]["video_metadata"]["engagement"]["digg_count"]}",
                                 f"comment_length: {min([len(comment["comment_data"]["content"]) for comment in video["comments"]])}"]
                                for video in videos]
    print(f"Videos: {video_ids_and_check_data}")
    # test search_users
    users = dab.search_users(["sec_uid"], ["MS4wLjABAAAARglgZSh09oRWurRs2re9pNY3qXmE_UzqLS50-bl0ZSQ"], AO.AND, ["=="], cast_data=False)
    print(f"Users: {users}")

    # test get_data_keys
    keys = get_data_keys(dab.video_table)
    print(f"Video Table Keys: {keys}")
    # test get_data_key_type_dict
    key_types = dab.get_data_key_type_dict("Videos")
    print(f"Video Table Key Types: {key_types}")

    # test data_to_csv save to different folder
    dab.data_to_csv("Videos", to_excel=True, basePath="C:/Users/schif/Documents/Coding/Yanjun/Database/slop")
    dab.data_to_csv("Users", to_excel=True, basePath="C:/Users/schif/Documents/Coding/Yanjun/Database/slop")
    dab.data_to_csv("comments", to_excel=True, basePath="C:/Users/schif/Documents/Coding/Yanjun/Database/slop")
