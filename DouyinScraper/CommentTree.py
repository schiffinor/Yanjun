"""
CommentTree Module

This module provides a set of utility functions and global definitions used for building and
managing a tree-like data structure for comments. It supports operations such as flattening nested lists,
mapping operator strings to functions, and parsing custom operator inputs.

Global Variables:
    - NestedSelf: A recursive type definition for a nested list of Nodes (or similar elements).
    - NestedNode: Similar to NestedSelf, representing nested Node structures.
    - varTypeDict: A dictionary mapping variable names (as strings) to the corresponding data types
      from DTyp2. Used for type checking and validating data.
    - varTypeRDict: A reverse mapping from DTyp members to their string names for display purposes.

Dependencies:
    - The module depends on standard libraries (inspect, operator, random, uuid, warnings, datetime, functools, typing)
      and custom modules such as Operands, AccountType, DataTypeTypes, DataTypes, and Gender.
"""

import inspect
import operator as opp
import os
import random
import uuid
import warnings as wn
import pickle
from datetime import datetime as dt
from datetime import date as dtd
from datetime import time as dtt
from functools import cmp_to_key
from typing import Tuple, List, Union, Dict, Callable, Any, Optional, Self

import numpy as np

import Operands as Op
from AccountType import AcctType as AccT
from DataTypeTypes import DtType2 as DTyp2
from DataTypes import DtTypes as DTyp
from Gender import DouyinGender as DGen
from Operands import InOperator

# Recursive type definitions for nested structures.
NestedSelf = List[Union[Self, "NestedSelf"]]
NestedNode = List[Union["Node", NestedSelf]]

# Mapping of variable names (as strings) to their data types (from DTyp2).
varTypeDict = {
    "cText": DTyp2.cText,
    "cUserLink": DTyp2.cUserLink,
    "cLikes": DTyp2.cLikes,
    "uName": DTyp2.uName,
    "uLink": DTyp2.uLink,
    "uID": DTyp2.uID,
    "IPTerritory": DTyp2.IPTerritory,
    "uFollowers": DTyp2.uFollowers,
    "uFollowing": DTyp2.uFollowing,
    "uLikesReceived": DTyp2.uLikesReceived,
    "uVideoCount": DTyp2.uVideoCount,
    "uGender": DTyp2.uGender,
    "uAge": DTyp2.uAge,
    "uBio": DTyp2.uBio,
    "uAccountType": DTyp2.uAccountType
}

# Reverse mapping from DTyp members to their string representations.
varTypeRDict = {
    DTyp.cText: "cText",
    DTyp.cUserLink: "cUserLink",
    DTyp.cLikes: "cLikes",
    DTyp.uName: "uName",
    DTyp.uLink: "uLink",
    DTyp.uID: "uID",
    DTyp.IPTerritory: "IPTerritory",
    DTyp.uFollowers: "uFollowers",
    DTyp.uFollowing: "uFollowing",
    DTyp.uLikesReceived: "uLikesReceived",
    DTyp.uVideoCount: "uVideoCount",
    DTyp.uGender: "uGender",
    DTyp.uAge: "uAge",
    DTyp.uBio: "uBio",
    DTyp.uAccountType: "uAccountType"
}


def recursiveFlatten(inList: List[List[Any]]) -> List[Any]:
    """
    Recursively flattens a nested list.

    This function takes a list that may contain other lists (of arbitrary depth) and returns a single list
    containing all the non-list elements in their original order.

    :param inList: A nested list of elements.
    :return: A flat list with all nested elements extracted.
    """
    outList = []
    for element in inList:
        if isinstance(element, list):
            outList.extend(recursiveFlatten(element))
        else:
            outList.append(element)
    return outList


def get_operator(op_input):
    """
    Map an operator input to a corresponding binary operator function.

    If the input is a string (e.g., "==", "!=", "<", "<=", ">", ">="), this function returns the corresponding
    operator function from the operator module. If the input is already a callable, it is returned directly.
    Additionally, it verifies that the resulting function accepts exactly two parameters.

    :param op_input: An operator as a string or a callable function.
    :return: A binary operator function.
    :raises ValueError: If the string does not correspond to a supported operator.
    :raises TypeError: If the input is not a string or callable, or if the resulting function does not accept exactly two parameters.
    """
    # If the input is a string, map it to an actual operator function.
    if isinstance(op_input, str):
        mapping = {
            "==": opp.eq,
            "!=": opp.ne,
            "<": opp.lt,
            "<=": opp.le,
            ">": opp.gt,
            ">=": opp.ge
        }
        try:
            op_func = mapping[op_input]
        except KeyError:
            raise ValueError(f"Unsupported operator string: {op_input}")
    elif callable(op_input):
        op_func = op_input
    else:
        raise TypeError("Operator must be either a string or a callable function.")

    # Verify that the operator function accepts exactly two parameters.
    sig = inspect.signature(op_func)
    if len(sig.parameters) != 2:
        raise TypeError("Operator function must accept exactly two parameters.")

    return op_func


def parseOpFunc(op_input: Union[str, Op.RelOperator, Callable[[Any, Any], bool]], cast_type: bool = True) -> Callable[[Any, Any], bool]:
    """
    Parse the operator input and return a binary operator function that casts the first argument
    to the type of the second argument before comparison.

    The operator input can be one of the following:
      - A string representing an operator ("==", "!=", "<", "<=", ">", ">=").
      - An instance of Op.RelOperator, a custom operator type from the Operands module.
      - A callable that accepts two arguments.

    If the input is a string, it is converted using get_operator(). If it's an instance of Op.RelOperator,
    a lambda is created to use Python's bitwise operator overloading to apply it. Otherwise, if it's a callable,
    it is returned directly.

    In every case, the returned function will cast the first argument (data) to the type of the second
    argument (the value to compare to) before applying the operator.

    :param op_input: The operator input (string, Op.RelOperator, or callable).
    :param cast_type: If True, cast the first argument to the type of the second argument.
    :return: A function that takes two arguments, casts the first to the type of the second, and applies the operator.
    :raises TypeError: If the input is not one of the supported types.
    """
    def wrap_operator(opF: Callable[[Any, Any], bool]) -> Callable[[Any, Any], bool]:
        def try_cast(a, b):
            try:
                return opF(type(b)(a), b)
            except TypeError as e:
                wn.warn(f"TypeError: {e}. Attempting to cast {a} to {type(b)} failed. Returning False.")
                return opF(a, b)
        return try_cast

    if isinstance(op_input, str):
        opF = get_operator(op_input)
        return wrap_operator(opF) if cast_type else opF
    elif isinstance(op_input, Op.RelOperator):
        opF = lambda a, b: a | op_input | b
        return wrap_operator(opF) if cast_type else opF
    elif callable(op_input):
        return wrap_operator(op_input) if cast_type else op_input
    else:
        raise TypeError(f"Operator must be either a string, a RelOperator, or a callable function. Operator input: {op_input}")



class UserData:
    """
    A container for storing and managing user profile data extracted from a comment.

    This class holds information like the user's name, profile link, ID, IP territory,
    and various numeric metrics (followers, likes, etc.). It also keeps track of the
    date and time at which each attribute was collected—because time flies and data ages!

    Attributes:
        uName (str): The user's name.
        uLink (str): The URL to the user's profile.
        uID (str): The user's unique identifier (as a string).
        IPTerritory (str): A string representing the user's IP territory.
        uFollowers (int): The number of followers the user has. -1 indicates uninitialized.
        uFollowing (int): The number of accounts the user follows.
        uLikesReceived (int): The number of likes received by the user.
        uVideoCount (int): The number of videos posted by the user.
        uGender (DGen): The user's gender (using DouyinGender enum; default is unknown).
        uAge (int): The user's age. -1 indicates uninitialized.
        uBio (str): The user's biography.
        uAccountType (AccT): The type of account (using AcctType enum; default is unknown).
        baseVars (Iterable): The list of attribute names at initialization.
        dataCollectionDate (Dict[str, Optional[dt.date]]): Timestamps (dates) for when data was collected.
        dataCollectionTime (Dict[str, Optional[dt.time]]): Timestamps (times) for when data was collected.
    """

    def __init__(self):
        """
        Initialize a new UserData instance with default values.

        All string fields are initialized to empty strings, numerical fields to -1,
        and enums to their 'unknown' value. Also creates dictionaries to track when
        each piece of data was collected. Because we like to know when our data gets old.
        """
        self.uName = ""
        self.uLink = ""
        self.uID = ""
        self.IPTerritory = ""
        self.uFollowers = -1
        self.uFollowing = -1
        self.uLikesReceived = -1
        self.uVideoCount = -1
        self.uGender = DGen.unknown
        self.uAge = -1
        self.uBio = ""
        self.uAccountType = AccT.unknown

        # Store the keys of the instance dictionary (these are our 'base' variables)
        baseVars = ["uName", "uLink", "uID", "IPTerritory", "uFollowers", "uFollowing", "uLikesReceived", "uVideoCount",
                    "uGender", "uAge", "uBio", "uAccountType"]
        self.baseVars = baseVars

        # Initialize collection timestamps for each attribute (none collected yet)
        self.dataCollectionDate: Dict[str, Optional[dt.date]] = {key: None for key in self.baseVars}
        self.dataCollectionTime: Dict[str, Optional[dt.time]] = {key: None for key in self.baseVars}
        self.JSONData = None

    def setJSONData(self, data: Dict[str, Any]):
        self.JSONData = data

    def getJSONData(self) -> Dict[str, Any]:
        return self.JSONData

    def setUName(self, name: str):
        """
        Set the user's name and update the timestamp for when it was set.

        :param name: The new name for the user.
        """
        self.uName = name
        # Record the date and time when the user's name was updated.
        self.dataCollectionDate["uName"] = dt.now().date()
        self.dataCollectionTime["uName"] = dt.now().time()

    def setULink(self, link: str):
        """
        Set the user's profile URL and update the timestamp.

        :param link: The new URL for the user's profile.
        """
        self.uLink = link
        self.dataCollectionDate["uLink"] = dt.now().date()
        self.dataCollectionTime["uLink"] = dt.now().time()

    def setUID(self, ID: str):
        """
        Set the user's unique identifier and update the timestamp.

        :param ID: The new unique identifier.
        """
        self.uID = ID
        self.dataCollectionDate["uID"] = dt.now().date()
        self.dataCollectionTime["uID"] = dt.now().time()

    def setIPTerritory(self, territory: str):
        """
        Set the user's IP territory and update the timestamp.

        :param territory: A string representing the user's IP territory.
        """
        self.IPTerritory = territory
        self.dataCollectionDate["IPTerritory"] = dt.now().date()
        self.dataCollectionTime["IPTerritory"] = dt.now().time()

    def setUFollowers(self, followers: int):
        """
        Set the number of followers the user has and update the timestamp.

        :param followers: The new follower count.
        """
        self.uFollowers = followers
        self.dataCollectionDate["uFollowers"] = dt.now().date()
        self.dataCollectionTime["uFollowers"] = dt.now().time()

    def setUFollowing(self, following: int):
        """
        Set the number of accounts the user is following and update the timestamp.

        :param following: The new following count.
        """
        self.uFollowing = following
        self.dataCollectionDate["uFollowing"] = dt.now().date()
        self.dataCollectionTime["uFollowing"] = dt.now().time()

    def setULikesReceived(self, likes: int):
        """
        Set the number of likes the user has received and update the timestamp.

        :param likes: The new likes received count.
        """
        self.uLikesReceived = likes
        self.dataCollectionDate["uLikesReceived"] = dt.now().date()
        self.dataCollectionTime["uLikesReceived"] = dt.now().time()

    def setUVideoCount(self, count: int):
        """
        Set the user's video count and update the timestamp.

        :param count: The new video count.
        """
        self.uVideoCount = count
        self.dataCollectionDate["uVideoCount"] = dt.now().date()
        self.dataCollectionTime["uVideoCount"] = dt.now().time()

    def setUGender(self, gender: DGen):
        """
        Set the user's gender and update the timestamp.

        :param gender: The new gender (from the DouyinGender enum).
        """
        self.uGender = gender
        self.dataCollectionDate["uGender"] = dt.now().date()
        self.dataCollectionTime["uGender"] = dt.now().time()

    def setUAge(self, age: int):
        """
        Set the user's age and update the timestamp.

        :param age: The new age.
        """
        self.uAge = age
        self.dataCollectionDate["uAge"] = dt.now().date()
        self.dataCollectionTime["uAge"] = dt.now().time()

    def setBio(self, bio: str):
        """
        Set the user's biography and update the timestamp.

        :param bio: The new biography text.
        """
        self.uBio = bio
        self.dataCollectionDate["uBio"] = dt.now().date()
        self.dataCollectionTime["uBio"] = dt.now().time()

    def setUAccountType(self, accountType: AccT):
        """
        Set the user's account type and update the timestamp.

        :param accountType: The new account type (from the AcctType enum).
        """
        self.uAccountType = accountType
        self.dataCollectionDate["uAccountType"] = dt.now().date()
        self.dataCollectionTime["uAccountType"] = dt.now().time()

    def setAll(self, name: str, link: str, ID: str, territory: str, followers: int, following: int, likes: int,
               count: int, gender: DGen, age: int, bio: str, accountType: AccT, dateC: Optional[dt.date] = None,
               timeC: Optional[dt.time] = None):
        """
        Set all user data attributes at once and update collection timestamps.

        This method calls the individual setters for each attribute. Optionally, you can override the
        automatic timestamp with a provided date/time.

        :param name: User's name.
        :param link: User's profile URL.
        :param ID: User's unique identifier.
        :param territory: The user's IP territory.
        :param followers: Number of followers.
        :param following: Number of accounts the user follows.
        :param likes: Number of likes received.
        :param count: Number of videos posted.
        :param gender: User's gender.
        :param age: User's age.
        :param bio: User's biography.
        :param accountType: User's account type.
        :param dateC: (Optional) A specific date to set for all attributes.
        :param timeC: (Optional) A specific time to set for all attributes.
        """
        # Call individual setters for each attribute
        self.setUName(name)
        self.setULink(link)
        self.setUID(ID)
        self.setIPTerritory(territory)
        self.setUFollowers(followers)
        self.setUFollowing(following)
        self.setULikesReceived(likes)
        self.setUVideoCount(count)
        self.setUGender(gender)
        self.setUAge(age)
        self.setBio(bio)
        self.setUAccountType(accountType)
        # Overwrite the collection date if provided, otherwise use current date/time.
        if dateC is not None:
            self.dataCollectionDate = dict(zip(self.baseVars, [dateC] * len(self.baseVars)))
        else:
            self.dataCollectionDate = dict(zip(self.baseVars, [dt.now().date()] * len(self.baseVars)))
        if timeC is not None:
            self.dataCollectionTime = dict(zip(self.baseVars, [timeC] * len(self.baseVars)))
        else:
            self.dataCollectionTime = dict(zip(self.baseVars, [dt.now().time()] * len(self.baseVars)))

    def getUName(self) -> str:
        """
        Return the user's name.

        :return: The user's name as a string.
        """
        return self.uName

    def getULink(self) -> str:
        """
        Return the user's profile URL.

        :return: The user's profile URL.
        """
        return self.uLink

    def getUID(self) -> str:
        """
        Return the user's unique identifier.

        :return: The user ID.
        """
        return self.uID

    def getIPTerritory(self) -> str:
        """
        Return the user's IP territory.

        :return: The IP territory string.
        """
        return self.IPTerritory

    def getUFollowers(self) -> int:
        """
        Return the number of followers.

        :return: Follower count.
        """
        return self.uFollowers

    def getUFollowing(self) -> int:
        """
        Return the number of accounts the user is following.

        :return: Following count.
        """
        return self.uFollowing

    def getULikesReceived(self) -> int:
        """
        Return the number of likes received.

        :return: Likes received.
        """
        return self.uLikesReceived

    def getUVideoCount(self) -> int:
        """
        Return the user's video count.

        :return: Video count.
        """
        return self.uVideoCount

    def getUGender(self) -> DGen:
        """
        Return the user's gender.

        :return: Gender as defined in the DouyinGender enum.
        """
        return self.uGender

    def getUAge(self) -> int:
        """
        Return the user's age.

        :return: Age.
        """
        return self.uAge

    def getBio(self) -> str:
        """
        Return the user's biography.

        :return: Biography string.
        """
        return self.uBio

    def getUAccountType(self) -> AccT:
        """
        Return the user's account type.

        :return: Account type as defined in the AcctType enum.
        """
        return self.uAccountType

    def getAll(self, dictify: bool = False) -> Tuple[str, str, str, str, int, int, int, int, DGen, int, str,
    AccT] | Dict[str, Any]:
        """
        Retrieve all user data attributes.

        :param dictify: If True, return the data as a dictionary; otherwise, as a tuple.
        :return: All attributes either as a tuple or a dictionary.
        """
        allList = (self.uName, self.uLink, self.uID, self.IPTerritory, self.uFollowers, self.uFollowing,
                   self.uLikesReceived, self.uVideoCount, self.uGender, self.uAge, self.uBio, self.uAccountType)
        if dictify:
            return dict(zip(self.baseVars, allList))
        return allList

    def getCollectionDates(self) -> Dict[str, dt]:
        """
        Return the dictionary of collection dates for each attribute.

        :return: A dictionary mapping attribute names to the date they were collected.
        """
        return self.dataCollectionDate

    def getCollectionTimes(self) -> Dict[str, dt]:
        """
        Return the dictionary of collection times for each attribute.

        :return: A dictionary mapping attribute names to the time they were collected.
        """
        return self.dataCollectionTime

    def getCollectionDate(self, var: str) -> dt:
        """
        Get the collection date for a specific attribute.

        :param var: The attribute name.
        :return: The collection date.
        """
        return self.dataCollectionDate[var]

    def getCollectionTime(self, var: str) -> dt:
        """
        Get the collection time for a specific attribute.

        :param var: The attribute name.
        :return: The collection time.
        """
        return self.dataCollectionTime[var]

    def getCollectionDateTime(self, var: str) -> Tuple[dt, dt]:
        """
        Get both the collection date and time for a specific attribute.

        :param var: The attribute name.
        :return: A tuple (date, time).
        """
        return self.dataCollectionDate[var], self.dataCollectionTime[var]

    def getCollectionDateTimeAll(self) -> Dict[str, Tuple[dt, dt]]:
        """
        Get the collection date and time for all attributes.

        :return: A dictionary mapping attribute names to a tuple (date, time).
        """
        return dict(
            zip(self.baseVars, [(self.dataCollectionDate[var], self.dataCollectionTime[var]) for var in self.baseVars]))

    def __copy__(self) -> "UserData":
        """
        Create a shallow copy of the UserData instance.

        :return: A shallow copy of the UserData instance.
        """
        newUserData = UserData()
        newUserData.__dict__.update(self.__dict__)
        return newUserData

    def __deepcopy__(self) -> "UserData":
        """
        Create a deep copy of the UserData instance.

        :return: A deep copy of the UserData instance.
        """
        newUserData = UserData()
        for key, value in self.__dict__.items():
            if isinstance(value, (int, str, DGen, AccT)):
                setattr(newUserData, key, value)
            else:
                setattr(newUserData, key, value.__deepcopy__())
        return newUserData

    def __eq__(self, other: "UserData") -> bool:
        """
        Check if two UserData instances are equal.

        :param other: Another UserData instance.
        :return: True if the instances are equal, False otherwise.
        """
        return all(getattr(self, var) == getattr(other, var) for var in self.baseVars)

    def __ne__(self, other: "UserData") -> bool:
        """
        Check if two UserData instances are not equal.

        :param other: Another UserData instance.
        :return: True if the instances are not equal, False otherwise.
        """
        return not self.__eq__(other)

    def deepEq(self, other: "UserData") -> bool:
        """
        Check if two UserData instances are equal, including data collection timestamps.

        :param other: Another UserData instance.
        :return: True if the instances are equal, False otherwise.
        """
        return all(getattr(self, var) == getattr(other, var) for var in self.baseVars) and \
               all(self.getCollectionDateTime(var) == other.getCollectionDateTime(var) for var in self.baseVars)

    def deepNotEq(self, other: "UserData") -> bool:
        """
        Check if two UserData instances are not equal, including data collection timestamps.

        :param other: Another UserData instance.
        :return: True if the instances are not equal, False otherwise.
        """
        return not self.deepEq(other)


class Data:
    """
    A container for comment data, complete with user metadata and collection timestamps.

    This class stores the core elements of a comment such as its text, the user's link,
    and the number of likes. It also contains a UserData instance holding details about the
    commenter. Additionally, it tracks the date and time when each attribute was "captured"
    (because even comments have a moment in time when they were cool).

    Attributes:
        cText (str): The actual comment text.
        cUserLink (str): The URL to the commenter's profile.
        cLikes (int): The number of likes on the comment.
        userData (UserData): The associated user data for the comment.
        baseVars (Iterable): The initial set of variable names (used for timestamp tracking).
        dataCollectionDate (Dict[str, Optional[dt.date]]): Timestamps (dates) when attributes were set.
        dataCollectionTime (Dict[str, Optional[dt.time]]): Timestamps (times) when attributes were set.
    """

    def __init__(self):
        """
        Initialize a new Data instance with default values.

        The comment text and user link are initialized as empty strings, the like count as 0,
        and a new UserData instance is created. All collection timestamps are set to None,
        because nothing has been recorded yet (and yes, we know it's dramatic).
        """
        self.cText = "" # The comment text (e.g., "I totally agree!")
        self.cUserLink = "" # URL to the commenter's profile
        self.cLikes = 0 # Initial likes count is 0 (we start at the bottom)
        self.userData = UserData() # Create a fresh UserData container

        # Store the names of all instance attributes for later use (e.g., timestamping)
        baseVars = ["cText", "cUserLink", "cLikes", "userData"]
        self.baseVars = baseVars

        # Initialize timestamp dictionaries (none collected yet, because time hasn't flown)
        self.dataCollectionDate: Dict[str, Optional[dtd]] = {key: None for key in self.baseVars}
        self.dataCollectionTime: Dict[str, Optional[dtt]] = {key: None for key in self.baseVars}

    def setCText(self, text: str):
        """
        Set the comment text and update its collection timestamp.

        :param text: The new comment text.
        """
        self.cText = text
        # Record the moment this comment text was set—because every word counts. Same for all below.
        self.dataCollectionDate["cText"] = dt.now().date()
        self.dataCollectionTime["cText"] = dt.now().time()

    def setCUserLink(self, link: str):
        """
        Set the commenter's profile URL and update its collection timestamp.

        :param link: The new user profile URL.
        """
        self.cUserLink = link
        self.dataCollectionDate["cUserLink"] = dt.now().date()
        self.dataCollectionTime["cUserLink"] = dt.now().time()

    def setCLikes(self, likes: int):
        """
        Set the comment's like count and update its collection timestamp.

        :param likes: The new like count.
        """
        self.cLikes = likes
        self.dataCollectionDate["cLikes"] = dt.now().date()
        self.dataCollectionTime["cLikes"] = dt.now().time()

    def setUserData(self, data: UserData):
        """
        Set the user data for the comment and update its collection timestamp.

        :param data: A UserData instance containing the commenter's details.
        """
        self.userData = data
        self.dataCollectionDate["userData"] = dt.now().date()
        self.dataCollectionTime["userData"] = dt.now().time()

    def setAll(self, text: str, userLink: str, likes: int, userData: UserData, dateC: Optional[dt.date] = None,
               timeC: Optional[dt.time] = None):
        """
        Set all comment data attributes at once and update collection timestamps.

        This method calls the individual setters for comment text, user link, like count, and user data.
        Optionally, you can override the automatically generated timestamps with provided values.

        :param text: The comment text.
        :param userLink: The URL to the commenter's profile.
        :param likes: The number of likes on the comment.
        :param userData: A UserData instance for the comment's author.
        :param dateC: (Optional) Specific date to set for all attributes.
        :param timeC: (Optional) Specific time to set for all attributes.
        """
        self.setCText(text)
        self.setCUserLink(userLink)
        self.setCLikes(likes)
        self.setUserData(userData)

        # If a specific date is provided, use it; otherwise, use the current date.
        if dateC is not None:
            self.dataCollectionDate = dict(zip(self.baseVars, [dateC] * len(self.baseVars)))
        else:
            self.dataCollectionDate = dict(zip(self.baseVars, [dt.now().date()] * len(self.baseVars)))
        # Similarly, set the time for all attributes.
        if timeC is not None:
            self.dataCollectionTime = dict(zip(self.baseVars, [timeC] * len(self.baseVars)))
        else:
            self.dataCollectionTime = dict(zip(self.baseVars, [dt.now().time()] * len(self.baseVars)))

    def deepSetAll(self, text: str, userLink: str, likes: int, name: str, ID: str, territory: str, followers: int,
                   following: int, likesReceived: int, videoCount: int, gender: DGen, age: int, bio: str,
                   accountType: AccT, dateC: Optional[dt.date] = None, timeC: Optional[dt.time] = None):
        """
        Set all comment and user data attributes at once, including the nested user details.

        This method first sets the comment-level attributes (text, link, likes) and then creates a new
        UserData instance to set the user's details. It updates collection timestamps for all attributes.

        :param text: The comment text.
        :param userLink: The commenter's profile URL.
        :param likes: The like count for the comment.
        :param name: The commenter's name.
        :param ID: The commenter's unique identifier.
        :param territory: The commenter's IP territory.
        :param followers: Number of followers the commenter has.
        :param following: Number of accounts the commenter follows.
        :param likesReceived: Total likes received by the commenter.
        :param videoCount: Number of videos posted by the commenter.
        :param gender: The commenter's gender.
        :param age: The commenter's age.
        :param bio: The commenter's biography.
        :param accountType: The commenter's account type.
        :param dateC: (Optional) Override collection date.
        :param timeC: (Optional) Override collection time.
        """
        # Set comment-specific data.
        self.setCText(text)
        self.setCUserLink(userLink)
        self.setCLikes(likes)

        # Create a new UserData object and set all its attributes.
        userData = UserData()
        userData.setAll(name, userLink, ID, territory, followers, following, likesReceived, videoCount, gender, age,
                        bio, accountType, dateC, timeC)
        self.setUserData(userData)

        # Overwrite collection timestamps for all attributes, either with provided or current values.
        if dateC is not None:
            self.dataCollectionDate = dict(zip(self.baseVars, [dateC] * len(self.baseVars)))
        else:
            self.dataCollectionDate = dict(zip(self.baseVars, [dt.now().date()] * len(self.baseVars)))
        if timeC is not None:
            self.dataCollectionTime = dict(zip(self.baseVars, [timeC] * len(self.baseVars)))
        else:
            self.dataCollectionTime = dict(zip(self.baseVars, [dt.now().time()] * len(self.baseVars)))

    def getCText(self) -> str:
        """
        Get the comment text.

        :return: The comment text as a string.
        """
        return self.cText

    def getCLikes(self) -> int:
        """
        Get the number of likes on the comment.

        :return: The like count.
        """
        return self.cLikes

    def getAll(self) -> Tuple[str, str, int, UserData]:
        """
        Retrieve all comment-level attributes as a tuple.

        :return: A tuple containing comment text, user link, like count, and the UserData instance.
        """
        return self.cText, self.cUserLink, self.cLikes, self.userData

    def deepGetAll(self, dictify: bool = False) -> Tuple[
                                                       str, str, int, str, str, str, str, int, int, int, int, DGen, int, str, AccT] | \
                                                   Dict[str, Any]:
        """
        Retrieve all comment and nested user data attributes.

        This method first obtains all user data via the UserData.getAll() method and then combines it
        with the comment-level attributes. It can return the result as either a tuple or a dictionary.

        :param dictify: If True, return the data as a dictionary; otherwise, as a tuple.
        :return: All attributes combined as a tuple or dictionary.
        """
        # Get all user data (as a tuple).
        userDataPull: Tuple[str, str, str, str, int, int, int, int, DGen, int, str, AccT] = self.userData.getAll()
        uName, uLink, uID, IPTerritory, uFollowers, uFollowing, uLikesReceived, uVideoCount, uGender, uAge, uBio, uAccountType = userDataPull

        # Combine comment and user data into one tuple.
        tupleAll = (self.cText, self.cUserLink, self.cLikes, uName, uLink, uID, IPTerritory, uFollowers, uFollowing,
                    uLikesReceived, uVideoCount, uGender, uAge, uBio, uAccountType)
        if dictify:
            fullBaseVars = list(self.baseVars)[:-1] + list(self.userData.baseVars)
            # Zip up the base variable names with their values to form a dictionary.
            return dict(zip(fullBaseVars, tupleAll))
        return tupleAll

    def getCollectionDate(self, var: str) -> Optional[dtd]:
        """
        Get the collection date for a specified attribute.

        :param var: The attribute name (e.g., 'cText').
        :return: The date when the attribute was set.
        """
        return self.dataCollectionDate[var]

    def getCollectionTime(self, var: str) -> Optional[dtt]:
        """
        Get the collection time for a specified attribute.

        :param var: The attribute name (e.g., 'cText').
        :return: The time when the attribute was set.
        """
        return self.dataCollectionTime[var]

    def getCollectionDateTime(self, var: str) -> Tuple[Optional[dtd], Optional[dtt]]:
        """
        Get both the collection date and time for a specified attribute.

        :param var: The attribute name.
        :return: A tuple (date, time) when the attribute was recorded.
        """
        return self.dataCollectionDate[var], self.dataCollectionTime[var]

    def getCollectionDateTimeAll(self) -> Dict[str, Tuple[Optional[dtd], Optional[dtt]]]:
        """
        Get the collection date and time for all attributes.

        :return: A dictionary mapping each attribute name to a tuple (date, time) of when it was set.
        """
        return dict(zip(self.baseVars, [(self.dataCollectionDate[var], self.dataCollectionTime[var]) for var in self.baseVars]))

    def __copy__(self) -> "Data":
        """
        Create a shallow copy of the Data instance.

        :return: A shallow copy of the Data instance.
        """
        newData = Data()
        newData.__dict__.update(self.__dict__)
        return newData

    def __deepcopy__(self) -> "Data":
        """
        Create a deep copy of the Data instance.

        :return: A deep copy of the Data instance.
        """
        newData = Data()
        for key, value in self.__dict__.items():
            if isinstance(value, (int, str)):
                setattr(newData, key, value)
            elif key == "userData":
                setattr(newData, key, value.__deepcopy__())
            else:
                setattr(newData, key, value.__copy__())
        return newData

    def __eq__(self, other: "Data") -> bool:
        """
        Check if two Data instances are equal.

        :param other: The other Data instance to compare.
        :return: True if the two instances are equal, False otherwise.
        """
        return self.cText == other.cText and self.cUserLink == other.cUserLink and self.cLikes == other.cLikes and self.userData == other.userData

    def deepEq(self, other: "Data") -> bool:
        """
        Check if two Data instances are equal, including dateTime.

        :param other: The other Data instance to compare.
        :return: True if the two instances are equal, False otherwise.
        """
        return self.cText == other.cText and self.cUserLink == other.cUserLink and self.cLikes == other.cLikes and self.userData.deepEq(other.userData) and self.dataCollectionDate == other.dataCollectionDate and self.dataCollectionTime == other.dataCollectionTime

    def __ne__(self, other: "Data") -> bool:
        """
        Check if two Data instances are not equal.

        :param other: The other Data instance to compare.
        :return: True if the two instances are not equal, False otherwise.
        """
        return not self.__eq__(other)

    def deepNotEq(self, other: "Data") -> bool:
        """
        Check if two Data instances are not equal, including dateTime.

        :param other: The other Data instance to compare.
        :return: True if the two instances are not equal, False otherwise.
        """
        return not self.deepEq(other)


class Node:
    """
    A node in the comment tree.

    Each Node holds a Data instance (i.e., the comment's content and associated metadata),
    a reference to its parent (if any), and a list of children nodes. Each node is given a unique
    UUID (with a hex string version for easy display) and tracks its index with respect to its parent.

    Warning: While this class is as solid as your morning coffee, be sure to use it wisely.
    """

    def __init__(self,
                 value: Data = None,
                 parent: "Node" = None,
                 uniqueID: uuid.UUID = None,
                 indexFromParent: int = None):
        """
        Initialize a new Node.

        If no Data instance is provided, a new empty Data instance is created.
        A unique identifier is assigned (unless one is given) and the node’s index among its siblings
        is set (defaults to 0 if provided, otherwise automatically determined on insertion).

        :param value: The Data instance for this node (comment text, likes, etc.). If None, an empty Data is created.
        :param parent: The parent Node (if any). Defaults to None for root nodes.
        :param uniqueID: An optional UUID. If not provided, a new UUID is generated.
        :param indexFromParent: An optional integer indicating this node's index among its siblings.
        """
        self.parent: "Node" = parent
        self.data = Data() if value is None else value
        self.children: List["Node"] = []  # List to hold child nodes (because one can never have too many children)
        # Assign a unique identifier; if provided, use it, otherwise generate a new one.
        self.unique = uniqueID if uniqueID is not None else uuid.uuid4()
        self.uniqueStr = self.unique.hex # Convenient hex string representation for display and easy dict search
        # indexFromParent: if given, use that; otherwise, we'll set it when adding the node to a parent.
        self.indexFromParent = indexFromParent if indexFromParent is None else 0


    def __len__(self):
        """
        Return the size of the subtree rooted at this node (including self).

        :return: Total number of nodes in this subtree.
        """
        # The size is defined as the total count of this node plus all its descendants. We can use the __iter__ method.
        return sum(1 for _ in self.__iter__())

    def size(self) -> int:
        """
        Alias for __len__.

        :return: The size of the subtree.
        """
        return len(self)

    def shortSize(self) -> int:
        """
        Return the number of children plus one (self).
        :return: The number of children plus one.
        """
        return len(self.children) + 1

    def getUniqueID(self) -> uuid.UUID:
        """
        Return the node's unique identifier.

        :return: The UUID of this node.
        """
        return self.unique

    def setUniqueID(self, uniqueID: uuid.UUID):
        """
        Set a new unique identifier for this node.

        :param uniqueID: The new UUID.
        """
        self.unique = uniqueID

    def getUniqueStr(self) -> str:
        """
        Return the hex string representation of the node's unique identifier.

        :return: Unique identifier as a hex string.
        """
        return self.uniqueStr

    def getIndexFromParent(self) -> int:
        """
        Return the index of this node among its siblings.

        :return: An integer index.
        """
        return self.indexFromParent

    def setIndexFromParent(self, index: int):
        """
        Set the index of this node among its siblings.

        :param index: The new index.
        """
        self.indexFromParent = index

    def add_child(self, child: "Node"):
        """
        Add a child node to this node.

        The child node's parent reference is updated, and its index is set based on its position.

        :param child: The Node to add as a child.
        """
        self.children.append(child)
        child.parent = self
        # Set the child's index from the parent based on its position in the list.
        child.setIndexFromParent(self.children.index(child))

    def remove_child(self, child: "Node"):
        """
        Remove a child node from this node.

        The child's parent reference is set to None.

        :param child: The Node to remove.
        """
        self.children.remove(child)
        child.parent = None
        # The child is now an orphan—sad but sometimes necessary.

    def replace_child(self, oldChild: "Node", newChild: "Node"):
        """
        Replace an existing child node with a new node.

        The new child takes the place of the old child, and parent/child references are updated.

        :param oldChild: The existing child to be replaced.
        :param newChild: The new Node that will replace oldChild.
        """
        self.children[self.children.index(oldChild)] = newChild
        newChild.parent = self
        newChild.setIndexFromParent(self.children.index(newChild))

    def add_children(self, children: List["Node"]):
        """
        Add multiple children to this node.

        :param children: A list of Node objects to add as children.
        """
        for child in children:
            self.add_child(child)

    def remove_children(self, children: Optional[List["Node"]] = None):
        """
        Remove specified children from this node. If no list is provided, remove all children.

        :param children: Optional list of Node objects to remove. If None, all children are removed.
        """
        if children is None:
            children = self.children.copy()
        for child in children:
            self.remove_child(child)
            child.setIndexFromParent(-1)
            # Setting index to -1 to indicate removal from parent's ordering.

    def get_children(self) -> List["Node"]:
        """
        Return the list of child nodes.

        :return: A list of Node objects that are children of this node.
        """
        return self.children

    def get_parent(self) -> "Node":
        """
        Return the parent of this node.

        :return: The parent Node, or None if this is the root.
        """
        return self.parent

    def get_data(self) -> Data:
        """
        Return the Data instance associated with this node.

        :return: The Data instance (comment data).
        """
        return self.data

    def set_data(self, data: Data):
        """
        Set the Data instance for this node.

        :param data: The new Data instance.
        """
        self.data = data

    def set_parent(self, parent: "Node"):
        """
        Set the parent node of this node.

        :param parent: The new parent Node.
        """
        self.parent = parent

    def is_root(self) -> bool:
        """
        Check if this node is the root (has no parent).

        :return: True if root, False otherwise.
        """
        return self.parent is None

    def is_leaf(self) -> bool:
        """
        Check if this node is a leaf (has no children).

        :return: True if leaf, False otherwise.
        """
        return len(self.children) == 0

    def is_internal(self) -> bool:
        """
        Check if this node is internal (i.e., not a leaf or root).

        :return: True if the node is internal, False otherwise.
        """
        return not self.is_leaf() and not self.is_root()

    def get_depth(self: "Node") -> int:
        """
        Calculate the depth of this node in the tree.

        Depth is defined as the number of edges from this node to the root.

        :return: The depth as an integer.
        """
        depth = 0
        node = self
        while node.parent:
            depth += 1
            node = node.parent
        return depth

    def get_path(self: "Node") -> List[int]:
        """
        Retrieve the path from the root to this node as a list of indices.

        Each number in the list represents this node's index in its parent's children list.
        For example, [0, 2, 1] means: first child of the root, then third child of that node, then second child.

        :return: A list of integer indices representing the path.
        """
        path = []
        node = self
        while node:
            if node.is_root():
                break
            # Find the index of the current node in its parent's children list.
            ind = node.get_parent().get_children().index(node)
            path.insert(0, ind)
            node = node.parent
        return path

    def localTree(self) -> NestedSelf:
        """
        Represent the subtree rooted at this node as a nested list.

        The format is [self, [localTree(child1), localTree(child2), ...]].

        :return: A nested list representation of the subtree.
        """
        ch = self.get_children()
        # Recursively call localTree on each child.
        return [self, [c.localTree() for c in ch]]

    def flatten(self: "Node") -> List["Node"]:
        """
        Flatten the subtree rooted at this node into a single list.

        :return: A flat list of all nodes in the subtree (including self).
        """
        # Utilize the recursiveFlatten helper function.
        return recursiveFlatten(self.localTree())

    def __iter__(self):
        """
        Allow iteration over the subtree in a depth-first manner.

        Yields:
            Each node in the subtree, starting with self.
        """
        yield self
        for c in self.get_children():
            yield from c.__iter__()

    def __repr__(self):
        """
        Return an unambiguous string representation of the node.

        :return: A string like "Node(CommentText)".
        """
        return f"Node({self.data.getCText()})"

    def __str__(self):
        """
        Return a user-friendly string representation of the node.

        :return: A string like "Node(CommentText)".
        """
        return f"Node({self.data.getCText()})"

    def find_nodes(self, count: int = None,
                   variables: Union[DTyp, List[DTyp], List[List[DTyp]]] = None,
                   data: Union[Any, List[Any], List[List[Any]]] = None,
                   indexLists: List[int] | List[List[int]] = None,
                   operatorCode: Union[str, Op.RelOperator, Callable[[Any, Any], bool]] = "==") -> list | List["Node"] | \
                                                                                                   List[List["Node"]]:
        """
        Search for nodes in the subtree that match the specified criteria.

        This method supports three types of search criteria:
            1. By variable/data pairs: Provide variables (from DTyp) and expected data values.
            2. By indexLists: Provide a list (or list of lists) of indices to traverse directly.
            3. By a custom operator (e.g., ">", "<", etc.) to compare attribute values.

        If none of the criteria are provided, it simply returns the first 'count' nodes in a flattened list.

        :param count: Maximum number of nodes to return. If None, all matching nodes are returned.
        :param variables: A DTyp instance or list (or list of lists) specifying which attributes to compare.
        :param data: The expected values for the corresponding variables.
        :param indexLists: Direct index path(s) into the tree. (Less recommended unless you know what you're doing.)
        :param operatorCode: A string, Op.RelOperator, or callable that defines how to compare values. Defaults to equality ("==").
        :return: A list (or list of lists) of nodes matching the criteria.
        """

        # Initialize the output list
        outList: list | List[Node] | List[List[Node]] = []

        # Interpret the count parameter
        if count is None:
            count = -1
        if count == 0:
            return []
        if count == -1:
            count = self.size()
        if count > self.size():
            wn.warn("The number of nodes requested exceeds the number of children. Returning all matches.")
            count = self.size()

        # Define a simple recursive function to gather nodes (if no search criteria is provided)
        def recursiveAll(node: "Node", currList: List["Node"]) -> List["Node"]:
            if len(currList) == count:
                return currList
            currList.append(node)
            for child in node.children:
                currList = recursiveAll(child, currList)
            return [currList]

        # Validate search criteria
        if variables is None and data is None and indexLists is None:
            wn.warn(f"You must specify at least one of vars and data or indexList. First {count} nodes.")
            return recursiveAll(self, outList)
        if variables is not None and data is None:
            raise ValueError("You must specify data to match if you specify vars.")
        if data is not None and variables is None:
            raise ValueError("You must specify vars to match if you specify data.")

        # Interpret the vars and data parameters
        # We need to ensure that the 'variables' parameter is a list of lists of DTyp (data types).
        if variables is not None:
            # If variables is a single DTyp (not a list), wrap it in two layers of lists.
            if isinstance(variables, DTyp):
                variables: List[List[DTyp]] = [[variables]]
            # If variables is already a list...
            elif isinstance(variables, list):
                # Check if at least one element is a DTyp (i.e., not already nested)
                if any([isinstance(var, DTyp) for var in variables]):
                    # And also check if any element is a list (i.e., already nested)
                    if any([isinstance(var, list) for var in variables]):
                        # If we have a mix, we need to normalize it to a list of lists.
                        tempList: List[List[DTyp]] = []
                        for varI in range(len(variables)):
                            temp = variables[varI]
                            # If the element itself is a DTyp, wrap it in a list.
                            if isinstance(temp, DTyp):
                                temp2: List[DTyp] = [temp]
                                tempList.append(temp2)
                            # If it is already a list, we check to see if it's a list of DTyp. if not, raise an error.
                            elif isinstance(temp, list):
                                if any([not isinstance(var, DTyp) for var in temp]):
                                    raise TypeError("vars must be a list of lists of DataTypes.")
                                # If it is a list of DTyp, we can append it as is.
                                temp2: List[DTyp] = temp
                                tempList.append(temp2)
                            else:
                                # This shouldn't happen unless someone passes a wrong type.
                                raise TypeError("vars must be a list of lists of DataTypes.")
                        variables: List[List[DTyp]] = tempList
                    # If all elements in the list are DTyp (i.e., not nested), wrap the entire list in another list.
                    elif all([isinstance(var, DTyp) for var in variables]):
                        variables: List[List[DTyp]] = [variables]
                # If variables is already a list of lists, then we’re good to go.
                elif all([isinstance(var, list) for var in variables]):
                    # Check if all elements in the nested lists are DTyp. If not, raise an error.
                    if any([any([not isinstance(var, DTyp) for var in sublist]) for sublist in variables]):
                        raise TypeError("vars must be a list of lists of DataTypes.")
                    else:
                        pass
                else:
                    raise TypeError("vars must be a list of lists of DataTypes.")
            else:
                raise TypeError("vars must be a list of lists of DataTypes.")

        # Now, process the 'data' parameter in a similar way to variables.
        if data is not None:
            # If data is a single value of one of the expected types (present in DTyp2), wrap it.
            if type(data) in DTyp2:
                data: DTyp2
                data: List[List[DTyp2]] = [[data]]
            # If data is a list...
            elif isinstance(data, list):
                # Check if any element's type is in DTyp2.
                if any([type(datum) in DTyp2 for datum in data]):
                    # If some elements are already lists, normalize the structure.
                    if any([isinstance(datum, list) for datum in data]):
                        tempList: List[List[DTyp2]] = []
                        for datumI in range(len(data)):
                            if type(data[datumI]) in DTyp2:
                                temp = [data[datumI]]
                                tempList.append(temp)
                            elif isinstance(data[datumI], list):
                                tempList.append(data[datumI])
                            else:
                                raise TypeError("data must be a list of lists of DataTypes.")
                        data: List[List[DTyp2]] = tempList
                    # If every element is of a type in DTyp2 (and none are lists), wrap the entire list.
                    elif all([type(datum) in DTyp2 for datum in data]):
                        data: List[List[DTyp]] = [data]
                # If data is already a list of lists, we should be good to go.
                elif all([isinstance(datum, list) for datum in data]):
                    # Check if all elements in the nested lists are in DTyp2. If not, raise an error.
                    if any([any([type(datum) not in DTyp2 for datum in sublist]) for sublist in data]):
                        raise TypeError("data must be a list of lists of DataTypeTypes.")
                    else:
                        pass
                else:
                    raise TypeError("data must be a list of lists of DataTypes.")
            else:
                raise TypeError("data must be a list of lists of DataTypeTypes.")
        # --- End interpretation of 'variables' and 'data' parameters ---

        # --- Begin type checking of parameters ---
        # Ensure 'variables' is a list and not empty, because an empty search is like searching for unicorns.
        if variables is not None:
            if not isinstance(variables, list):
                raise TypeError("vars must be a list of DataTypes.")
            if len(variables) == 0:
                wn.warn("vars is empty. Returning all matches.")

        # Ensure 'data' is a list and not empty, because data-less searches are a waste of time.
        if data is not None:
            if not isinstance(data, list):
                raise TypeError("data must be a list of DataTypeTypes.")
            if len(data) == 0:
                wn.warn("data is empty. Returning all matches.")

        # Ensure that the 'variables' and 'data' lists have the same length.
        if variables is not None and data is not None:
            if len(variables) != len(data):
                raise ValueError("vars and data must have the same length.")

        # --- End type checking of parameters ---

        # --- Begin interpretation of indexLists parameter ---
        # indexLists allow for direct navigation down the tree.
        if indexLists is not None:
            # Ensure indexLists is a list; if not, it's like giving directions in a language no one understands.
            if not isinstance(indexLists, list):
                raise TypeError("indexLists must be a list of lists of integers. \n "
                                "Lists of integers will be accepted for a single point, though not recommended.")
            # Warn if indexLists is empty.
            if len(indexLists) == 0:
                wn.warn("indexLists is empty. Returning zero matches.")
                return []
            # Determine if indexLists is a list of lists or a list of integers.
            ListLists = [isinstance(element, list) for element in indexLists]
            ListInts = [isinstance(element, int) for element in indexLists]
            allListsBool = all(ListLists)
            allIntsBool = all(ListInts)
            if not (allListsBool or allIntsBool):
                raise TypeError(
                    "indexLists must be a list of lists of integers or a list of integers. Lists and integers cannot be mixed.")
            if allListsBool:
                inLists: List[List[int]] = indexLists
                # Double-check that every sub-element is indeed an integer.
                if any([any([not isinstance(element, int) for element in sublist]) for sublist in inLists]):
                    raise TypeError("indexLists must be a list of lists of integers.")
                parsedIndexList: List[List[int]] = inLists
            elif allIntsBool:
                inLists: List[int] = indexLists
                parsedIndexList: List[List[int]] = [inLists]
            else:
                raise TypeError("indexLists must be a list of lists of integers or a list of integers.")

            # When indexLists are provided, override count with the number of index lists.
            count = len(indexLists)

            # Traverse the tree based on the provided index paths.
            for sublist in parsedIndexList:
                refNode = self # Start at the current node (self)
                for index in sublist:
                    try:
                        refNode = refNode.get_children()[index]
                    except IndexError:
                        wn.warn(f"Index {index} is out of range. Returning last valid node.")
                        break
                outList.append(refNode)
            # Once direct access is done, return the list of nodes found.
            return outList
        # --- End interpretation of indexLists parameter ---

        # --- Begin final check for variables and data matching ---
        # For each pair in the lists, ensure they have the same length and type.
        if isinstance(variables, list) and isinstance(data, list):
            for i in range(len(variables)):
                if type(variables[i]) is not type(data[i]):
                    raise TypeError("vars and data must have the same type.")
                if len(variables[i]) != len(data[i]):
                    raise ValueError("vars and data must have the same length.")
                # Check each element: ensure the type of data[i][ji] matches the type specified in varTypeDict for variables[i][ji].
                if any([not (type(data[i][ji]) is not varTypeDict[variables[i][ji].value]) for ji in
                        range(len(variables[i]))]):
                    raise TypeError(
                        "Individual data elements must be of the same type as the corresponding var element.")
        # --- End final check for variables and data matching ---

        # Define a recursive helper for variable-based search (logic unchanged)
        def _recursiveVarSearch(node: "Node",
                                rVars: List[List[DTyp]],
                                rData: List[List[DTyp2]],
                                rCount: int,
                                currList: List[List["Node"]],
                                iterated: bool = False,
                                opF: Callable[[Any, Any], bool] = opp.eq) -> List[List["Node"]]:
            if rCount == 0:
                return currList

            # Inner function to check if node data matches expected criteria.
            def _dataSearch(node2: "Node",
                            vars2: List[List[DTyp]],
                            data2: List[List[DTyp2]],
                            innerCurrList: List[List["Node"]],
                            iterated2: bool = False,
                            added: bool = False,
                            opFF: Callable[[Any, Any], bool] = opp.eq) -> Tuple[List[List[DTyp2]], bool, bool]:
                for i in range(len(vars2)):
                    # Add list to outList if first iteration
                    if not iterated2:
                        innerCurrList.append([])

                    # Create a dictionary from the node's data for easy access.
                    dataDict = dict(zip([DTElem.value for DTElem in DTyp], node2.data.deepGetAll()))

                    # check if all values match
                    if all(opFF(dataDict[k.value], expected) for k, expected in zip(vars2[i], data2[i])):
                        if not added:
                            innerCurrList[i].append(node2)
                            added = True
                        else:
                            wn.warn("Duplicate node found. Skipping.")
                iterated2 = True
                return innerCurrList, iterated2, added

            # Check if the node matches any query
            currList, iterated, addRef = _dataSearch(node, rVars, rData, currList, iterated2=iterated, opFF=opF)
            if addRef:
                rCount -= 1

            # Check if the node has children
            if node.is_leaf():
                return currList

            # Recurse on children
            for child in node.children:
                currList = _recursiveVarSearch(child, rVars, rData, rCount, currList, iterated, opF)
                if rCount == 0:
                    return currList
            return currList

        # Call recursive search function
        opFunc = parseOpFunc(operatorCode)
        outList = _recursiveVarSearch(self, variables, data, count, outList, opF=opFunc)
        return outList

    def sort(self, variable: DTyp = None,
             reverse: bool = False,
             key: Union[Op, Callable[[Any], Any]] = None,
             cmp: Union[str, Op.InOperator, opp, Callable[[Any, Any], int]] = None) -> None:
        """
        Recursively sort the children of this node based on a specified variable.

        You can specify the sorting key and comparator. By default, it sorts by 'cLikes' in ascending order.
        Use reverse=True to flip the order. The comparator (cmp) can be a string (like "<"), a custom InOperator,
        or any callable that compares two values.

        :param variable: The DTyp variable to sort by (default: cLikes).
        :param reverse: If True, sort in descending order.
        :param key: A function that extracts a comparison key from a node. Defaults to using the variable from the node's data.
        :param cmp: A comparator defining the order; defaults to "<" (ascending order).
        """
        # Define a helper function to convert the cmp parameter to a callable function.
        def _pyCmpFromCmp(cmp2: Union[str, Op.InOperator, opp, Callable[[Any, Any], int]]) -> Callable[[Any, Any], int]:
            if isinstance(cmp2, str):
                preCMP3 = parseOpFunc(cmp2)
            elif isinstance(cmp2, Op.InOperator):
                def preCMP3(a: Any, b: Any) -> int:
                    return a | cmp2 | b
            elif cmp2.is_callable():
                # Check if the function has the expected signature.
                if cmp2.__code__.co_argcount != 2:
                    raise TypeError("Operator function must accept exactly two parameters.")
                preCMP3 = cmp2
            else:
                raise TypeError("cmp must be a string, InOperator, or a callable function.")
            # Prevent using equality or inequality operators for sorting.
            if preCMP3 == opp.eq:
                raise ValueError("Cannot use equality operator for sorting.")
            elif preCMP3 == opp.ne:
                raise ValueError("Cannot use inequality operator for sorting.")
            # For common operators, provide a standard comparator.
            elif preCMP3 == opp.lt:
                return lambda a, b: -1 if a < b else 0 if a == b else 1
            elif preCMP3 == opp.le:
                return lambda a, b: -1 if a < b else 0 if a == b else 1
            elif preCMP3 == opp.gt:
                return lambda a, b: 1 if a < b else 0 if a == b else -1
            elif preCMP3 == opp.ge:
                return lambda a, b: 1 if a < b else 0 if a == b else -1
            elif isinstance(preCMP3, Callable) or isinstance(preCMP3, Op.InOperator):
                return preCMP3
            else:
                raise TypeError("cmp must be a string, InOperator, or a callable function.")

        if variable is None:
            variable = DTyp.cLikes # Default to sorting by comment likes
        if key is None:
            key = lambda x: x.data.deepGetAll(True)[variable.value]
        elif isinstance(key, Op.PreFixOperator):
            key = lambda x: key | x
        else:
            key = key
        if cmp is None:
            cmp2 = "<"
        else:
            cmp2 = cmp
        cmp: Callable[[Any, Any], int] = _pyCmpFromCmp(cmp2)
        for node in self:
            node: "Node"
            if node.is_leaf():
                continue
            # Sort each node's children using the provided key and comparator.
            node.children = sorted(node.children, key=cmp_to_key(lambda a, b: cmp(key(a), key(b))), reverse=reverse)
        for node in self:
            node: "Node"
            # Update the index of each child node based on its position in the list.
            for i, child in enumerate(node.children):
                child.setIndexFromParent(i)

    def __copy__(self) -> "Node":
        """
        Create a shallow copy of the Node.

        :return: A shallow copy of the Node.
        """
        return Node(self.data, self.parent, self.unique, self.indexFromParent)

    def __deepcopy__(self) -> "Node":
        """
        Create a deep copy of the Node.

        :return: A deep copy of the Node.
        """
        # Create a shallow copy of the node.
        new_node = Node(self.data, self.parent, self.unique, self.indexFromParent)
        # Deep copy the children and add them to the new node.
        new_node.children = [child.__deepcopy__() for child in self.children]
        return new_node

    def __eq__(self, other: "Node") -> bool:
        """
        Check if two nodes are equal.
        Checks if the data,and index from parent are equal. For root and all children recursively.

        :param other: The other Node to compare.
        :return: True if the nodes are equal, False otherwise.
        """
        if not isinstance(other, Node):
            return False
        if self.data != other.data:
            return False
        if self.indexFromParent != other.indexFromParent:
            return False
        if len(self.children) != len(other.children):
            return False
        for i in range(len(self.children)):
            if self.children[i] != other.children[i]:
                return False
        return True

    def __ne__(self, other: "Node") -> bool:
        """
        Check if two nodes are not equal.

        :param other: The other Node to compare.
        :return: True if the nodes are not equal, False otherwise.
        """
        return not self.__eq__(other)

    def deepEqual(self, other: "Node") -> bool:
        """
        Check if two nodes are equal, including all descendants.
        Matches UUIDs and data, and recursively checks children and collection dates.

        :param other: The other Node to compare.
        :return: True if the nodes are equal, False otherwise.
        """
        if not isinstance(other, Node):
            return False
        if self.data != other.data:
            return False
        if self.getUniqueID() != other.getUniqueID():
            return False
        if len(self.children) != len(other.children):
            return False
        for i in range(len(self.children)):
            if not self.children[i].deepEqual(other.children[i]):
                return False
        return True

    def deepNotEqual(self, other: "Node") -> bool:
        """
        Check if two nodes are not equal, including all descendants.

        :param other: The other Node to compare.
        :return: True if the nodes are not equal, False otherwise.
        """
        return not self.deepEqual(other)


class CommentTree:
    """
    Creates a tree of comments under a dummy root node.

    This class is your high-level interface for managing a comment tree. It wraps the low-level Node
    operations and provides methods to add, remove, find, sort, and otherwise manipulate the tree.
    Think of it as your comment jungle gym, with a dummy root at the top keeping everything in line.
    """

    def __init__(self):
        """
        Initialize a new CommentTree instance.

        A dummy root node is created (which holds no real comment data, just a placeholder).
        Two dictionaries (uuidDict and uuidStrDict) are used for fast lookups of nodes by their unique IDs.
        """
        self.root = Node() # Create the dummy root node.
        self.root.setIndexFromParent(0) # Set root's index (arbitrarily 0, since it's the top).
        # Dictionaries for fast lookup by UUID and its hex string representation.
        self.uuidDict: Dict[uuid.UUID, Node] = {self.root.getUniqueID(): self.root}
        self.uuidStrDict: Dict[str, Node] = {self.root.getUniqueStr(): self.root}
        self.uuid = self.root.getUniqueID() # Store the root's UUID for easy access.
        self.uuidStr = self.root.getUniqueStr() # Store the root's hex string for easy access.

    def get_root(self) -> Node:
        """
        Return the root node of the tree.

        :return: The root Node.
        """
        return self.root

    def set_root(self, root: Node):
        """
        Set a new root node for the tree.

        :param root: The new root Node.
        """
        self.root = root
        self.uuidDict = {self.root.getUniqueID(): self.root}
        self.uuidStrDict = {self.root.getUniqueStr(): self.root}
        self.uuid = self.root.getUniqueID()
        self.uuidStr = self.root.getUniqueStr()

    def find_nodes(self, count: int = None,
                   variables: Union[DTyp, List[DTyp], List[List[DTyp]]] = None,
                   data: Union[Any, List[Any], List[List[Any]]] = None,
                   indexLists: List[int] | List[List[int]] = None,
                   operatorCode: Union[str, Op.RelOperator, Callable[[Any, Any], bool]] = "==") -> list | List[Node] | \
                                                                                                   List[List[Node]]:
        """
        Find nodes in the tree that match the specified criteria.

        This method simply delegates the search operation to the root node's find_nodes method.
        You can search using variable/data pairs, by index paths, or using a custom operator.

        :param count: Maximum number of nodes to return. If None, defaults to the size of the tree.
        :param variables: A single DTyp, list of DTyp, or list of lists of DTyp specifying which attributes to match.
        :param data: The corresponding expected values for the given variables.
        :param indexLists: Direct index path(s) into the tree (not recommended unless you really know what you're doing).
        :param operatorCode: A string (like "==", ">", etc.), a custom Op.RelOperator, or a callable for comparing values.
        :return: A list (or list of lists) of Node objects that match the search criteria.
        """
        return self.root.find_nodes(count, variables, data, indexLists, operatorCode)

    def sort(self, variable: DTyp = None,
             reverse: bool = False,
             key: Union[Op, Callable[[Any], Any]] = None,
             cmp: Union[str, Op.InOperator, opp, Callable[[Any, Any], int]] = None) -> None:
        """
        Recursively sort the children of each node in the tree.

        This method sorts the children of the root (and recursively all descendants) based on a specified
        variable in the node's data. You can specify a custom key function and comparator.

        :param variable: The DTyp variable to sort by (default: cLikes).
        :param reverse: If True, sort in descending order; otherwise, ascending.
        :param key: A function to extract a comparison key from a node. Defaults to extracting the value of 'variable'.
        :param cmp: A comparator that can be a string (e.g., "<"), a custom InOperator, or a callable.
        :return: None.
        """
        self.root.sort(variable, reverse, key, cmp)

    def __iter__(self):
        """
        Return an iterator over the nodes in the tree (depth-first order).

        :return: An iterator yielding Node objects.
        """
        return self.root.__iter__()

    def __copy__(self) -> "CommentTree":
        """
        Create a shallow copy of the CommentTree.

        :return: A shallow copy of the CommentTree.
        """
        new_tree = CommentTree()
        new_tree.set_root(self.root.__deepcopy__())
        return new_tree

    def iterSort(self, variable: DTyp = None, reverse: bool = False, key: Union[Op, Callable[[Any], Any]] = None, cmp: Union[str, Op.InOperator, opp, Callable[[Any, Any], int]] = None):
        """
        Return an iterator over the nodes in the tree (depth-first order) after sorting them without modifying the tree.

        :param variable: The DTyp variable to sort by (default: cLikes).
        :param reverse: If True, sort in descending order; otherwise, ascending.
        :param key: A function to extract a comparison key from a node. Defaults to extracting the value of 'variable'.
        :param cmp: A comparator that can be a string (e.g., "<"), a custom InOperator, or a callable.
        :return: An iterator yielding Node objects.
        """
        tempTree = self.__copy__()
        tempTree.sort(variable, reverse, key, cmp)
        return tempTree.__iter__()

    def iterPostSort(self, variable: DTyp = None, reverse: bool = False, key: Union[Op, Callable[[Any], Any]] = None, cmp: Union[str, Op.InOperator, opp, Callable[[Any, Any], int]] = None):
        """
        Return an iterator over the nodes in the tree fully sorting them fully without modifying the tree.
        Note tree structure not maintained. Functionally equivalent to flattening then sorting.

        :param variable: The DTyp variable to sort by (default: cLikes).
        :param reverse: If True, sort in descending order; otherwise, ascending.
        :param key: A function to extract a comparison key from a node. Defaults to extracting the value of 'variable'.
        :param cmp: A comparator that can be a string (e.g., "<"), a custom InOperator, or a callable.
        :return: An iterator yielding Node objects.
        """
        # Define a helper function to convert the cmp parameter to a callable function.
        def _pyCmpFromCmp(cmp2: Union[str, Op.InOperator, opp, Callable[[Any, Any], int]]) -> Callable[[Any, Any], int]:
            if isinstance(cmp2, str):
                preCMP3 = parseOpFunc(cmp2)
            elif isinstance(cmp2, Op.InOperator):
                def preCMP3(a: Any, b: Any) -> int:
                    return a | cmp2 | b
            elif cmp2.is_callable():
                # Check if the function has the expected signature.
                if cmp2.__code__.co_argcount != 2:
                    raise TypeError("Operator function must accept exactly two parameters.")
                preCMP3 = cmp2
            else:
                raise TypeError("cmp must be a string, InOperator, or a callable function.")
            # Prevent using equality or inequality operators for sorting.
            if preCMP3 == opp.eq:
                raise ValueError("Cannot use equality operator for sorting.")
            elif preCMP3 == opp.ne:
                raise ValueError("Cannot use inequality operator for sorting.")
            # For common operators, provide a standard comparator.
            elif preCMP3 == opp.lt:
                return lambda a, b: -1 if a < b else 0 if a == b else 1
            elif preCMP3 == opp.le:
                return lambda a, b: -1 if a < b else 0 if a == b else 1
            elif preCMP3 == opp.gt:
                return lambda a, b: 1 if a < b else 0 if a == b else -1
            elif preCMP3 == opp.ge:
                return lambda a, b: 1 if a < b else 0 if a == b else -1
            elif isinstance(preCMP3, Callable) or isinstance(preCMP3, Op.InOperator):
                return preCMP3
            else:
                raise TypeError("cmp must be a string, InOperator, or a callable function.")

        if variable is None:
            variable = DTyp.cLikes # Default to sorting by comment likes
        if key is None:
            key = lambda x: x.data.deepGetAll(True)[variable.value]
        elif isinstance(key, Op.PreFixOperator):
            key = lambda x: key | x
        else:
            key = key
        if cmp is None:
            cmp2 = "<"
        else:
            cmp2 = cmp
        cmp: Callable[[Any, Any], int] = _pyCmpFromCmp(cmp2)
        return sorted(self.flatten()[1:], key=cmp_to_key(lambda a, b: cmp(key(a), key(b))), reverse=reverse).__iter__()

    def __len__(self):
        """
        Return the total number of nodes in the tree, excluding the dummy root.

        Equivalent to calling size().

        :return: Size of the tree as an integer.
        """
        return len(self.root) - 1

    def size(self) -> int:
        """
        Alias for __len__; returns the size of the tree.

        :return: The number of nodes in the tree.
        """
        return len(self)

    def flatten(self) -> List[Node]:
        """
        Flatten the tree into a single list of nodes.

        :return: A flat list of Node objects in the tree.
        """
        return self.root.flatten()

    def localTree(self) -> NestedNode:
        """
        Return a nested list representation of the tree.

        Each node is represented as [node, [child1, child2, ...]].

        :return: A nested list representing the tree structure.
        """
        return self.root.localTree()

    def get_path(self) -> List[int]:
        """
        Return the path (index sequence) of the root node.

        Since the root is at the top, this is typically empty.

        :return: A list of indices (should be empty for the root).
        """
        return self.root.get_path()

    def get_depth(self) -> int:
        """
        Return the depth of the root node.

        For the root, this should be 0.

        :return: Depth as an integer.
        """
        return self.root.get_depth()

    def find_node_by_index(self, indexList: List[int]) -> Node:
        """
        Traverse the tree based on an index path and return the node at that path.

        For example, an index list [0, 2, 1] means:
            - Start at the root's first child,
            - then take that node's third child,
            - then that node's second child.
        If any index is out of range, a warning is issued and the last valid node is returned.

        :param indexList: A list of integers specifying the path.
        :return: The Node at the specified path.
        """
        refNode = self.root
        for index in indexList:
            try:
                refNode = refNode.get_children()[index]
            except IndexError:
                wn.warn(f"Index {index} is out of range. Returning last valid node.")
                break
        return refNode

    def add_node_parent(self, node: Node, parent: Node):
        """
        Add a node as a child of a specified parent node.

        The parent's children list is updated, and the node is added to lookup dictionaries.

        :param node: The Node to add.
        :param parent: The parent Node under which to add the new node.
        :return: None.
        """
        parent.add_child(node)
        self.uuidDict[node.getUniqueID()] = node
        self.uuidStrDict[node.getUniqueStr()] = node

    def add_node(self, node: Node, indexList: List[int] = None):
        """
        Add a node to the tree at a specific index path.

        The node is inserted as a child of the node at the given index path.

        :param node: The Node to add.
        :param indexList: A list of indices specifying where to insert the node.
        :return: None.
        """
        refNode = self.find_node_by_index(indexList)
        self.add_node_parent(node, refNode)

    def add_top_node(self, node: Node):
        """
        Add a node as a top-level comment (i.e., as a direct child of the root).

        :param node: The Node to add.
        :return: None.
        """
        self.add_node_parent(node, self.root)

    def remove_node(self, node: Node):
        """
        Remove a node from the tree.

        The node is removed from its parent's children list and deleted from lookup dictionaries.

        :param node: The Node to remove.
        :return: None.
        """
        node.get_parent().remove_child(node)
        del self.uuidDict[node.getUniqueID()]
        del self.uuidStrDict[node.getUniqueStr()]

    def remove_node_by_id(self, uniqueID: uuid.UUID):
        """
        Remove a node from the tree using its unique UUID.

        :param uniqueID: The UUID of the node to remove.
        :return: None.
        """
        node = self.uuidDict[uniqueID]
        self.remove_node(node)

    def remove_node_by_str(self, uniqueStr: str):
        """
        Remove a node from the tree using its unique identifier as a string.

        :param uniqueStr: The unique hex string of the node to remove.
        :return: None.
        """
        node = self.uuidStrDict[uniqueStr]
        self.remove_node(node)

    def remove_node_by_index(self, indexList: List[int]):
        """
        Remove a node from the tree based on an index path.

        :param indexList: A list of indices specifying the node to remove.
        :return: None.
        """
        refNode = self.find_node_by_index(indexList)
        self.remove_node(refNode)

    def replace_node(self, oldNode: Node, newNode: Node):
        """
        Replace an existing node in the tree with a new node.

        The new node takes the position of the old node, and lookup dictionaries are updated.

        :param oldNode: The Node to be replaced.
        :param newNode: The Node that will replace oldNode.
        :return: None.
        """
        oldNode.get_parent().replace_child(oldNode, newNode)
        del self.uuidDict[oldNode.getUniqueID()]
        del self.uuidStrDict[oldNode.getUniqueStr()]
        self.uuidDict[newNode.getUniqueID()] = newNode
        self.uuidStrDict[newNode.getUniqueStr()] = newNode

    def replace_node_by_id(self, node: Node, uniqueID: uuid.UUID):
        """
        Replace a node in the tree identified by its UUID.

        :param node: The new Node to insert.
        :param uniqueID: The UUID of the node to be replaced.
        :return: None.
        """
        refNode = self.uuidDict[uniqueID]
        self.replace_node(refNode, node)

    def replace_node_by_str(self, node: Node, uniqueStr: str):
        """
        Replace a node in the tree identified by its unique string.

        :param node: The new Node to insert.
        :param uniqueStr: The unique hex string of the node to be replaced.
        :return: None.
        """
        refNode = self.uuidStrDict[uniqueStr]
        self.replace_node(refNode, node)

    def replace_node_by_index(self, node: Node, indexList: List[int]):
        """
        Replace a node in the tree at the given index path.

        :param node: The new Node to insert.
        :param indexList: A list of indices specifying the node to replace.
        :return: None.
        """
        refNode = self.find_node_by_index(indexList)
        self.replace_node(refNode, node)

    def add_comment(self, cText: str, cUserLink: str, cLikes: int, indexList: List[int] = None,
                    userData: UserData = None):
        """
        Add a comment to the tree.

        This method wraps the given comment parameters into a Data object,
        creates a new Node from that Data, and adds it either as a top-level comment
        (if indexList is None) or as a child at the specified index path.

        :param cText: The comment text.
        :param cUserLink: The URL to the commenter's profile.
        :param cLikes: The number of likes the comment has.
        :param indexList: Optional list of indices to specify where in the tree to insert the comment.
        :param userData: Optional UserData object for the commenter. If None, an empty UserData is used.
        :return: None.
        """
        data = Data()
        data.setAll(cText, cUserLink, cLikes, userData)
        node = Node(data)
        if indexList is None:
            self.add_top_node(node)
        else:
            self.add_node(node, indexList)

    def __repr__(self):
        """
        Return an unambiguous string representation of the CommentTree.

        :return: A string in the format "CommentTree(RootCommentText)".
        """
        return f"CommentTree({self.root.get_data().getCText()})"

    def __str__(self):
        """
        Return a user-friendly string representation of the CommentTree.

        :return: A string in the format "CommentTree(RootCommentText)".
        """
        return f"CommentTree({self.root.get_data().getCText()})"

    def __getitem__(self, key: int | str | uuid.UUID) -> Node:
        """
        Retrieve a node from the tree using a key.

        The key can be:
         - an integer (to index into the flattened tree),
         - a unique string (the hex representation of the node's UUID), or
         - a UUID.

        :param key: The key to index the tree.
        :return: The Node corresponding to the key.
        :raises TypeError: If the key is not an int, str, or UUID.
        """
        if isinstance(key, int):
            return self.flatten()[key]
        elif isinstance(key, str):
            return self.uuidStrDict[key]
        elif isinstance(key, uuid.UUID):
            return self.uuidDict[key]
        else:
            raise TypeError("Key must be an integer, string, or UUID.")

    @staticmethod
    def print_level_order_tree(rt: Node, printVar: DTyp = DTyp.cText, printVar2: Optional[DTyp] = None, debug: bool = False):
        """
        Print the tree in level order (breadth-first traversal).

        Each level is indented to visually represent the depth of the tree.
        Nodes that share the same parent are grouped in curly brackets.
        Optionally, additional data from up to two DTyp variables can be printed alongside each node.

        :param rt: The root Node of the tree to print.
        :param printVar: The primary DTyp variable to display for each node (default: cText).
        :param printVar2: An optional secondary DTyp variable to display.
        :param debug: If True, print additional debug information.
        :return: None.
        """
        if rt is None:
            return

        # Queue holds tuples: (node, parent)
        queue = [(rt, None)]
        level = 0

        while queue:
            indent = " " * (4 * level)
            if level == 0:
                # For the root level, simply print the root without brackets.
                node, _ = queue.pop(0)
                print(indent + f"{node.get_data().getCText()} (root)")
                for child in node.get_children():
                    queue.append((child, node))
                level += 1
                continue

            level_size = len(queue)
            grouped_output = []
            current_group = []
            current_parent = None

            for _ in range(level_size):
                entry = queue.pop(0)
                node: Node = entry[0]
                parent: Node = entry[1]

                # Determine parent's display text (root is displayed as 'root').
                parent_text = "root" if (parent is None or parent is rt) else parent.get_data().getCText()
                extra_ = f" (data = {node.data.deepGetAll(dictify=True)[str(printVar2.value)]})" if printVar2 is not None else ""
                debug_text = f" (parent = {parent_text}; index = {node.indexFromParent}; uuid = {node.getUniqueID()})" if debug else ""
                node_repr = f"{node.data.deepGetAll(dictify=True)[str(printVar.value)]} (child of {parent_text})" + extra_ + debug_text

                # Group nodes that share the same parent.
                if current_parent is None:
                    current_parent = parent
                    current_group.append(node_repr)
                elif parent == current_parent:
                    current_group.append(node_repr)
                else:
                    # Parent changed: wrap current group in curly brackets and start a new group.
                    grouped_output.append("{" + " | ".join(current_group) + "}")
                    current_group = [node_repr]
                    current_parent = parent

                # Enqueue the children with the current node as their parent.
                for child in node.get_children():
                    queue.append((child, node))

            # Append any remaining group.
            if current_group:
                grouped_output.append("{" + " | ".join(current_group) + "}")
            print(indent + " ".join(grouped_output))
            level += 1

    def print_lot(self, printVar: DTyp = DTyp.cText, printVar2: Optional[DTyp] = None):
        """
        Print the entire CommentTree in level order.

        This is just a convenience method to print the tree starting from the root.

        :param printVar: The primary DTyp variable to display (default: cText).
        :param printVar2: An optional secondary DTyp variable to display.
        :return: None.
        """
        self.print_level_order_tree(self.root, printVar, printVar2)

    def __eq__(self, other: "CommentTree") -> bool:
        """
        Check if two CommentTrees are equal.

        Two CommentTrees are considered equal if they are structurally identical.

        :param other: The other CommentTree to compare.
        :return: True if the trees are equal, False otherwise.
        """
        return self.root == other.root

    def __ne__(self, other: "CommentTree") -> bool:
        """
        Check if two CommentTrees are not equal.

        :param other: The other CommentTree to compare.
        :return: True if the trees are not equal, False otherwise.
        """
        return not self.__eq__(other)

    def deepEQ(self, other: "CommentTree") -> bool:
        """
        Check if two CommentTrees are equal, including all descendants.

        :param other: The other CommentTree to compare.
        :return: True if the trees are equal, False otherwise.
        """
        return self.root.deepEqual(other.root)

    def toCSV(self, path: str = None) -> None:
        """
        Convert the CommentTree to a CSV file. Use numpy to save as a CSV file.
        We will store all data in a CSV file.
        First we will store the root node, it has no data except for a list of all its children.
        Then we will store all the children of the root node. We will store the children of each child node and so on.
        Each row will have the following columns:
        - The unique ID of the node.
        - The unique ID of the parent node.
        - The index of the node in the parent's children list.
        - The data of the node (all fields).
        - The latest data collection date time.

        :param path: The path to save the CSV file. If None, defaults to 'commentTreeCSVs\\{self.uuidStr}\\commentTree.csv'.
        """
        # If path is none default to 'commentTreeCSVs\\{uuidStr}\\{current datetime}\\commentTree.csv'
        if path is None:
            path = f"commentTreeCSVs\\{self.uuidStr}\\{(str(dt.now()).replace(" ","_").replace(":", "-").replace(".", "_"))}\\commentTree.csv"
        # Create the directory if it does not exist
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # Create numpy data array to store the data
        data = np.array([["UUID",
                          "Parent UUID",
                          "Index",
                          "cText",
                          "cUserLink",
                          "cLikes",
                          "uName",
                          "uLink",
                          "uID",
                          "IPTerritory",
                          "uFollowers",
                          "uFollowing",
                          "uLikesReceived",
                          "uVideoCount",
                          "uGender",
                          "uAge",
                          "uBio",
                          "uAccountType",
                          "Latest Data Collection Date",
                          "Latest Data Collection time"]])
        # Add the root node to the data array
        data = np.append(data, np.array([[self.root.getUniqueStr(), None, None, None, None, None, None, None, None,
                                          None, None, None, None, None, None, None, None, None, None, None]]), axis=0)
        # Use level order traversal to add all the nodes to the data array
        # Remember iterSort returns a generator that is exactly what we want if we sort the tree by index
        # Remember to skip the first node as it is the root node
        for node in list(self.iterSort(key=lambda x: x.getIndexFromParent()))[1:]:
            # Get the data of the node
            nodeData = node.get_data().deepGetAll(True)
            # Find latest data collection date time
            allDataCollectionDateTimes = node.get_data().getCollectionDateTimeAll()
            # Result is a dictionary of variables to their latest data collection date time
            # Thus we can just get the latest data collection date time by getting the maximum value of the dictionary
            # However, we specifically want the value that is the maximum, not the key
            # Thus we get the key of the maximum value and then get the value of that key
            latestDataCollectionDateTime: [dt, dt] = allDataCollectionDateTimes[max(allDataCollectionDateTimes, key=lambda k: allDataCollectionDateTimes[k])]
            # Add the data to the data array
            data = np.append(data, np.array([[node.getUniqueStr(),
                                              node.get_parent().getUniqueStr(),
                                              node.getIndexFromParent(),
                                              nodeData["cText"],
                                              nodeData["cUserLink"],
                                              nodeData["cLikes"],
                                              nodeData["uName"],
                                              nodeData["uLink"],
                                              nodeData["uID"],
                                              nodeData["IPTerritory"],
                                              nodeData["uFollowers"],
                                              nodeData["uFollowing"],
                                              nodeData["uLikesReceived"],
                                              nodeData["uVideoCount"],
                                              nodeData["uGender"],
                                              nodeData["uAge"],
                                              nodeData["uBio"],
                                              nodeData["uAccountType"],
                                              latestDataCollectionDateTime[0],
                                              latestDataCollectionDateTime[1]]]), axis=0)
        # Save the data array to a CSV file
        np.savetxt(path, data, delimiter=",", fmt="%s")

    @staticmethod
    def fromCSV(path: str) -> "CommentTree":
        """
        Load a CommentTree from a CSV file. Use numpy to load a CSV file.
        We will load all data from a CSV file.
        First we will load the root node, it has no data except for a list of all its children.
        Then we will load all the children of the root node. We will load the children of each child node and so on.
        Each row will have the following columns:
        - The unique ID of the node.
        - The unique ID of the parent node.
        - The index of the node in the parent's children list.
        - The data of the node (all fields).
        - The latest data collection date time.

        :param path: The path to load the CSV file from.
        :return: The CommentTree loaded from the CSV file.
        """
        # Load the data from the CSV file
        data = np.loadtxt(path, delimiter=",", dtype=str)
        # Create a new CommentTree
        newTree = CommentTree()
        # Create a dictionary to store the nodes
        nodes = {}
        # Fetch root node and use it to set the root node of the tree
        rootRow = data[1]
        root = Node(Data())
        root.setUniqueID(uuid.UUID(hex=str(rootRow[0])))
        newTree.set_root(root)
        nodes[root.getUniqueID()] = root
        newTree.root.setIndexFromParent(0)
        # Iterate through the data and add the nodes to the tree
        for row in data[2:]:
            # Create a new node
            node = Node(Data())
            nodeID = uuid.UUID(hex=str(row[0]))
            parentID = uuid.UUID(hex=str(row[1]))
            node.setUniqueID(nodeID)
            index = int(row[2])
            # Set the data of the node
            nodeData = {
                "cText": str(row[3]),
                "cUserLink": str(row[4]),
                "cLikes": int(row[5]),
                "uName": str(row[6]),
                "uID": str(row[8]),
                "IPTerritory": str(row[9]),
                "uFollowers": int(row[10]),
                "uFollowing": int(row[11]),
                "uLikesReceived": int(row[12]),
                "uVideoCount": int(row[13]),
                "uGender": DGen[str(row[14])],
                "uAge": int(row[15]),
                "uBio": str(row[16]),
                "uAccountType": AccT[str(row[17])]
            }
            cdate = dtd.fromisoformat(str(row[18]))
            ctime = dtt.fromisoformat(str(row[19]))
            node.data.deepSetAll(*nodeData.values(), dateC=cdate, timeC=ctime)
            # Add the node to the dictionary
            nodes[nodeID] = node
            # Add the node to the tree
            newTree.add_node_parent(node, nodes[parentID])
            # Set the index of the node
            node.setIndexFromParent(index)

        # Return the new CommentTree
        return newTree

    def toPickle(self, path: str = None) -> None:
        """
        This function will save the CommentTree to a pickle file.
        :return:
        """
        # Save the CommentTree to a pickle file
        if path is None:
            path = f"commentTreePickle\\{(str(dt.now()).replace(" ", "_").replace(":", "-").replace(".", "_"))}\\{self.uuidStr}\\commentTree.pickle"
        # Create the directory if it does not exist
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            # noinspection PyTypeChecker
            pickle.dump(self, f)

    @staticmethod
    def fromPickle(path: str) -> "CommentTree":
        """
        This function will load a CommentTree from a pickle file.
        :param path: The path to load the CommentTree from.
        :return: The CommentTree loaded from the pickle file.
        """
        # Load the CommentTree from the pickle file
        with open(path, "rb") as f:
            return pickle.load(f)

    def getUniqueUsers(self) -> Dict[str, List[str]]:
        """
        Get all unique users in the CommentTree.
        :return: A dict of all unique users in the CommentTree.
        """
        # Create a dictionary to store the unique users
        uniqueUsers = {}
        # Iterate through all the nodes in the CommentTree
        for node in self:
            # Get the user data of the node
            userLink = node.get_data().cUserLink
            nodeUUID = node.getUniqueStr()
            # If the user is not in the dictionary, add it
            if userLink not in uniqueUsers:
                uniqueUsers[userLink] = [nodeUUID]
            # Otherwise, append the node to the list of nodes for that user
            else:
                uniqueUsers[userLink].append(nodeUUID)
        return uniqueUsers












if __name__ == "__main__":
    dGDict = {0: DGen.male, 1: DGen.female, 2: DGen.unknown}
    dADict = {0: AccT.blue, 1: AccT.red, 2: AccT.yellow, 3: AccT.personal, 4: AccT.unknown}

    rd = random.Random()

    # Test the Node class
    node0 = Node(Data())
    node1 = Node(Data())
    node2 = Node(Data())
    node3 = Node(Data())
    node4 = Node(Data())
    node5 = Node(Data())
    node6 = Node(Data())
    node7 = Node(Data())
    node8 = Node(Data())
    node9 = Node(Data())
    node10 = Node(Data())
    node11 = Node(Data())

    nodes = [node0, node1, node2, node3, node4, node5, node6, node7, node8, node9, node10, node11]

    for i in range(3):
        for j in range(3):
            nodes[i].add_child(nodes[3 * (i + 1) + j])

    # Generate Data and UserData objects
    dataObjects = []
    randIntsList = []
    for i in range(12):
        data = Data()
        randInts = [rd.randint(0, 100000) for _ in range(7)]
        randIntsList.append(randInts)
        totalData = [
            f"Comment {i}",
            f"https://www.douyin.com/user/{i}",
            randInts[0],
            f"user {i}'s name",
            f"{randInts[1]}",
            f"Territory {i}",
            randInts[2],
            randInts[3],
            randInts[4],
            randInts[5],
            DGen[dGDict[rd.randint(0, 2)]],
            randInts[6],
            f"Bio {i}",
            AccT[dADict[rd.randint(0, 4)]]
        ]
        data.deepSetAll(*totalData)
        dataObjects.append(data)

    # Assign the data objects to the nodes
    for i in range(12):
        nodes[i].set_data(dataObjects[i])

    # Show the node tree
    print("The node tree:")
    childrenNodesY = []
    for i in range(3):
        print(nodes[i].get_data().getCText())
        childrenNodesY.append(nodes[i].get_children())
    for child in childrenNodesY:
        for cNode in child:
            print(cNode.get_data().deepGetAll(True)["cText"])

    # Print all nodes
    print("All nodes:")
    for cNode in nodes:
        print(cNode.get_data().deepGetAll(True)["cText"])

    # Print children
    for i in range(3):
        print(f"Node {i}'s children:")
        for child in nodes[i].get_children():
            print(child.get_data().getCText())

    # Test the find_nodes method

    # Test 1: Find all nodes
    print("Test 1: Find all nodes")
    for i in range(3):
        print(f"Searching node {i}.")
        foundNodes = nodes[i].find_nodes()
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes.")
        for cNode in recursiveFlatten(foundNodes):
            print(cNode.get_data().deepGetAll(True)["cText"])
    print()

    # Test 2: Find all nodes with a specific comment text
    print("Test 2: Find all nodes with a specific comment text")
    for i in range(3):
        print(f"Searching node {i}.")
        foundNodes = nodes[i].find_nodes(variables=[DTyp.cText], data=["Comment 1"])
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes.")
        for cNode in recursiveFlatten(foundNodes):
            print(cNode)
            print(cNode.get_data().deepGetAll(True)["cText"])
    print()

    # Test 3: Find all nodes with a specific comment text and user link
    print("Test 3: Find all nodes with a specific comment text and user link")
    for i in range(3):
        print(f"Searching node {i}.")
        foundNodes = nodes[i].find_nodes(variables=[DTyp.cText, DTyp.cUserLink],
                                         data=["Comment 1", f"https://www.douyin.com/user/1"])
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes.")
        for cNode in recursiveFlatten(foundNodes):
            print(cNode.get_data())
    print()

    # Test 4: Find all nodes with a specific comment text and user link and user ID
    print("Test 4: Find all nodes with a specific comment text and user link and user ID")
    for i in range(3):
        print(f"Searching node {i}.")
        foundNodes = nodes[i].find_nodes(variables=[DTyp.cText, DTyp.cUserLink, DTyp.uID],
                                         data=["Comment 1", f"https://www.douyin.com/user/1", f"{randIntsList[0][1]}"])
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes.")
        for cNode in recursiveFlatten(foundNodes):
            print(cNode.get_data().deepGetAll(True)["cText"])
    print()

    # Test 5: Find all nodes matching two query sets: one with a specific comment text and user id and the other with a specific comment text and user link
    print(
        "Test 5: Find all nodes matching three query sets: two with a specific comment text and user id and the other with a specific comment text and user link")
    for i in range(3):
        print(f"Searching node {i}.")
        foundNodes = nodes[i].find_nodes(
            variables=[[DTyp.cText, DTyp.uID], [DTyp.cText, DTyp.cUserLink], [DTyp.cText, DTyp.cUserLink]],
            data=[["Comment 1", f"{randIntsList[1][1]}"], ["Comment 2", f"https://www.douyin.com/user/2"],
                  ["Comment 5", f"https://www.douyin.com/user/5"]])
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes.")
        for cNode in recursiveFlatten(foundNodes):
            print(cNode.get_data().deepGetAll(True)["cText"])
    print()

    # Test 6: Find 2 nodes matching three query sets, the middle being invalid: one with a specific comment text and user id,
    # one with a specific comment text and user link, and one with a specific comment text and user link and user ID
    print("Test 6: Find 2 nodes matching three query sets, the middle being invalid:"
          " one with a specific comment text and user id, one with a specific comment text and user link,"
          " and one with a specific comment text and user link and user ID")
    for i in range(3):
        print(f"Searching node {i}.")
        foundNodes = nodes[i].find_nodes(
            variables=[[DTyp.cText, DTyp.uID], [DTyp.cText, DTyp.cUserLink], [DTyp.cText, DTyp.cUserLink, DTyp.uID]],
            data=[["Comment 1", f"{randIntsList[1][1]}"], ["Comment n", f"https://www.douyin.com/user/n"],
                  ["Comment 3", f"https://www.douyin.com/user/3", f"{randIntsList[3][1]}"]],
            count=2)
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes.")
        for cNode in recursiveFlatten(foundNodes):
            print(cNode.get_data().deepGetAll(True)["cText"])
    print()

    # Test 7: Test operator for cLikes using ">" (nodes with cLikes > threshold)
    print("Test 7: Test operator for cLikes using '>' operator (nodes with cLikes > threshold)")
    threshold = 50000
    for i in range(3):
        print(f"Searching node {i} for nodes with cLikes > {threshold}.")
        foundNodes = nodes[i].find_nodes(variables=[DTyp.cLikes], data=[threshold], operatorCode=">")
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes with cLikes > {threshold} in node {i}.")
        for cNode in recursiveFlatten(foundNodes):
            print(f"Node with cLikes: {cNode.get_data().deepGetAll(True)['cLikes']}")
    print()

    # Test 8: Test operator for cLikes using a custom lambda (>= operator)
    print("Test 8: Test operator for cLikes using lambda (>=)")
    for i in range(3):
        print(f"Searching node {i} for nodes with cLikes >= {threshold}.")
        foundNodes = nodes[i].find_nodes(variables=[DTyp.cLikes], data=[threshold], operatorCode=lambda a, b: a >= b)
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes with cLikes >= {threshold} in node {i}.")
        for cNode in recursiveFlatten(foundNodes):
            print(f"Node with cLikes: {cNode.get_data().deepGetAll(True)['cLikes']}")
    print()

    # Test 9: Test operator for cLikes using op.RelOperator for "!=" operator
    print("Test 9: Test operator for cLikes using op.RelOperator with '!='")
    not_equal = Op.RelOperator(lambda a, b: a != b)
    for i in range(3):
        print(f"Searching node {i} for nodes with cLikes != {threshold}.")
        foundNodes = nodes[i].find_nodes(variables=[DTyp.cLikes], data=[threshold], operatorCode=not_equal)
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes with cLikes != {threshold} in node {i}.")
        for cNode in recursiveFlatten(foundNodes):
            print(f"Node with cLikes: {cNode.get_data().deepGetAll(True)['cLikes']}")
    print()

    # Test 10: Test operator for cLikes using op.RelOperator for "<" operator
    print("Test 10: Test operator for cLikes using op.RelOperator with '<'")
    less_than = Op.RelOperator(lambda a, b: a < b)
    for i in range(3):
        print(f"Searching node {i} for nodes with cLikes < {threshold}.")
        foundNodes = nodes[i].find_nodes(variables=[DTyp.cLikes], data=[threshold], operatorCode=less_than)
        nodeCount = len(recursiveFlatten(foundNodes))
        print(f"Found {nodeCount} nodes with cLikes < {threshold} in node {i}.")
        for cNode in recursiveFlatten(foundNodes):
            print(f"Node with cLikes: {cNode.get_data().deepGetAll(True)['cLikes']}")
    print()

    # Test sort method
    print("Test sort method")
    for i in range(3):
        print(f"Sorting node {i}.")
        nodes[i].sort()
        for cNode in nodes[i]:
            print(f"Node {cNode.get_data().getCText()} with cLikes: {cNode.get_data().getCLikes()}")
    print()

    # Test sort method with custom key and cmp
    print("Test sort method with custom key and cmp")
    for i in range(3):
        print(f"Sorting node {i}.")
        nodes[i].sort(variable=DTyp.cLikes, reverse=True, key=lambda x: x.data.getCLikes(), cmp="<")
        for cNode in nodes[i]:
            print(f"Node {cNode.get_data().getCText()} with cLikes: {cNode.get_data().getCLikes()}")
    print()


    # Test sort method with custom key and cmp
    def cCmpFunc(a: Any, b: Any) -> int:
        if (a - 50000) ** 2 < (b - 50000) ** 2:
            return -1
        elif (a - 50000) ** 2 == (b - 50000) ** 2:
            return 0
        else:
            return 1


    customCmp = InOperator(cCmpFunc)
    print("Test sort method with custom key and cmp")
    for i in range(3):
        print(f"Sorting node {i}.")
        nodes[i].sort(variable=DTyp.cLikes, reverse=True, key=lambda x: x.data.getCLikes(), cmp=customCmp)
        for cNode in nodes[i]:
            print(f"Node {cNode.get_data().getCText()} with cLikes: {cNode.get_data().getCLikes()}")

    # Unlink the nodes
    for i in range(12):
        print(f"Unlinking node {i}. \n")
        cNode = nodes[i]
        cNode.remove_children()


    # Reorganize nodes into a tree with the following structure:
    #
    #             Node0
    #            /     \
    #        Node1      Node8
    #        /    \        \
    #    Node2   Node5     Node9
    #    /   \   /   \     /   \
    # Node3 Node4 Node6 Node7 Node10 Node11
    #
    # Assumes nodeList has exactly 12 Node objects (indices 0-11).

    def tree_maker(nodeList: List[Node]) -> Node:
        """
        Reorganize nodes into a tree with the following structure:

                    Node0
                   /     \\
               Node1      Node8
               /    \\        \\
           Node2   Node5     Node9
           /   \\   /   \\     /   \\
        Node3 Node4 Node6 Node7 Node10 Node11

        Assumes nodeList has exactly 12 nodes.
        """
        if len(nodeList) < 12:
            raise ValueError("Need at least 12 nodes to build the tree structure.")

        # Level 1 (root)
        root = nodeList[0]

        # Level 2
        root.add_child(nodeList[1])  # Node1
        root.add_child(nodeList[8])  # Node8

        # Level 3
        nodeList[1].add_child(nodeList[2])  # Node2 (child of Node1)
        nodeList[1].add_child(nodeList[5])  # Node5 (child of Node1)
        nodeList[8].add_child(nodeList[9])  # Node9 (child of Node8)

        # Level 4
        nodeList[2].add_child(nodeList[3])  # Node3 (child of Node2)
        nodeList[2].add_child(nodeList[4])  # Node4 (child of Node2)
        nodeList[5].add_child(nodeList[6])  # Node6 (child of Node5)
        nodeList[5].add_child(nodeList[7])  # Node7 (child of Node5)
        nodeList[9].add_child(nodeList[10])  # Node10 (child of Node9)
        nodeList[9].add_child(nodeList[11])  # Node11 (child of Node9)

        return root


    def print_level_order_tree(root: Node):
        """
        Prints a level order traversal of the tree starting at 'root',
        with each level indented to visually represent the tree structure.
        """
        if root is None:
            return

        current_level = [root]
        level = 0
        while current_level:
            # Indent each level (4 spaces per level)
            indent = " " * (4 * level)
            # Retrieve the text (or any display data) for nodes at this level
            level_texts = [node.get_data().getCText() for node in current_level]
            print(indent + " | ".join(level_texts))

            # Prepare the next level of nodes
            next_level = []
            for node in current_level:
                next_level.extend(node.get_children())
            current_level = next_level
            level += 1


    # Example usage:
    # Assuming your list 'nodes' contains 12 Node objects:
    root = tree_maker(nodes)

    print("Level order traversal of the tree:")
    print_level_order_tree(root)

    # Test 11: IndexLists
    print("Test 11: IndexLists")
    foundNodes = node0.find_nodes(indexLists=[[0, 1, 0], [0, 1, 1], [1, 0, 0]])
    nodeCount = len(recursiveFlatten(foundNodes))
    print(f"Found {nodeCount} nodes.")
    for cNode in recursiveFlatten(foundNodes):
        print(cNode.get_data().deepGetAll(True)["cText"])
    print()

    # Test getDepth, getPath, localTree, flatten, and iter methods
    print("Test getDepth, getPath, localTree, flatten, and iter methods")
    for i in range(12):
        print(f"Node {i}'s depth: {nodes[i].get_depth()}")
        print(f"Node {i}'s path: {nodes[i].get_path()}")
        print(f"Node {i}'s local tree: {nodes[i].localTree()}")
        print(f"Node {i}'s flattened tree: {[node.get_data().getCText() for node in nodes[i].flatten()]}")
        print(f"Node {i}'s iterator: {[node.get_data().getCText() for node in nodes[i]]}")
        print()

    # Create an array of 100 Data objects with random test data.
    dataObjects = []
    randIntsList = []
    for i in range(100):
        data = Data()
        randInts = [rd.randint(0, 100000) for _ in range(7)]
        randIntsList.append(randInts)
        totalData = [
            f"Comment {i}",
            f"https://www.douyin.com/user/{i}",
            randInts[0],
            f"user {i}'s name",
            f"{randInts[1]}",
            f"Territory {i}",
            randInts[2],
            randInts[3],
            randInts[4],
            randInts[5],
            DGen[dGDict[rd.randint(0, 2)]],
            randInts[6],
            f"Bio {i}",
            AccT[dADict[rd.randint(0, 4)]]
        ]
        data.deepSetAll(*totalData)
        dataObjects.append(data)

    # Create a new CommentTree instance.
    ct = CommentTree()

    # --- Add comments using different methods ---
    # 1. For the first 20 comments, add as top-level nodes.
    for i in range(5):
        node = Node(dataObjects[i])
        ct.add_top_node(node)

    # 2. For the next 30 comments, add as children of random top-level nodes.
    for i in range(5, 15):
        node = Node(dataObjects[i])
        top_level = ct.get_root().get_children()
        parent = rd.choice(top_level) if top_level else ct.get_root()
        ct.add_node_parent(node, parent)

    # 3. For the next 30 comments, add using add_node with a random index path.
    for i in range(15, 30):
        node = Node(dataObjects[i])
        all_nodes = ct.flatten()
        # Choose a random node from the existing tree (or use the root if empty)
        parent = rd.choice(all_nodes) if all_nodes else ct.get_root()
        # Use the parent's path as the index path.
        index_path = parent.get_path()
        ct.add_node(node, index_path)

    # 4. For the last 20 comments, add as children of random nodes using add_node_parent.
    for i in range(30, 100):
        node = Node(dataObjects[i])
        all_nodes = ct.flatten()
        parent = rd.choice(all_nodes) if all_nodes else ct.get_root()
        ct.add_node_parent(node, parent)

    # --- Test tree methods ---
    print("\n=== Flattened Tree ===")
    flat = ct.flatten()
    print([n.get_data().getCText() for n in flat])

    print("\n=== Level Order Tree ===")
    ct.print_level_order_tree(ct.get_root())

    print("\n=== Search Test: Looking for 'Comment 50' ===")
    search_results = ct.find_nodes(variables=[DTyp.cText], data=["Comment 50"])
    flat_search = recursiveFlatten(search_results)
    print([n.get_data().getCText() for n in flat_search])

    print("\n=== Sorting Test ===")
    # Pick a random node with children to sort; if none, use the root.
    candidates = [n for n in ct.get_root().get_children() if n.get_children()]
    sort_node = rd.choice(candidates) if candidates else ct.get_root()
    print("Before sorting (by cLikes):")
    for child in sort_node.get_children():
        print(f"{child.get_data().getCText()} - cLikes: {child.get_data().getCLikes()}")
    sort_node.sort(variable=DTyp.cLikes, reverse=False, cmp="<")
    print("After sorting (ascending by cLikes):")
    for child in sort_node.get_children():
        print(f"{child.get_data().getCText()} - cLikes: {child.get_data().getCLikes()}")

    print("\n=== sort Test 2 ===")
    # Sort whole tree from root.
    print("Before sorting (by cLikes):")
    print("Level Order Tree: \n")
    ct.print_level_order_tree(ct.get_root(), printVar=DTyp.cText, printVar2=DTyp.cLikes)
    ct.sort(variable=DTyp.cLikes, reverse=False, key=lambda x: x.data.getCLikes(), cmp="<")
    print("After sorting (ascending by cLikes):")
    print("Level Order Tree: \n")
    ct.print_level_order_tree(ct.get_root(), printVar=DTyp.cText, printVar2=DTyp.cLikes)

    print("\n=== __getitem__ Test ===")
    # Test __getitem__: retrieve node by integer index, unique string, and unique UUID.
    flat_list = ct.flatten()
    if flat_list:
        first_node = ct[0]
        print("First node (by int index):", first_node.get_data().getCText())
        uid = first_node.getUniqueID()
        print("Access by UUID:", ct[uid].get_data().getCText())
        uidStr = first_node.getUniqueStr()
        print("Access by unique string:", ct[uidStr].get_data().getCText())

    print("\n=== Depth and Path Tests ===")
    # Pick a random node and show its depth and path.
    random_node = rd.choice(ct.flatten())
    print(f"Random node '{random_node.get_data().getCText()}' depth: {random_node.get_depth()}")
    print(f"Random node path (index sequence from root): {random_node.get_path()}")

    print("\n=== Local Tree Structure ===")
    # Print the local tree structure of the root.
    print(ct.get_root().localTree())

    # --- End of tests ---
    print("\n=== Testing Complete ===")

    # Test iterSort method
    print("\n=== iterSort Test ===")
    print("Before sorting:")
    for node in ct:
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")
    print("\nAfter sorting:")
    for node in ct.iterSort(variable=DTyp.cLikes, reverse=True, key=lambda x: x.data.getCLikes(), cmp="<"):
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")
    print("\nAfter sorting (reversed):")
    for node in ct.iterSort(variable=DTyp.cLikes, reverse=False, key=lambda x: x.data.getCLikes(), cmp="<"):
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")
    print("\nAfter sorting (ascending):")
    for node in ct.iterSort(variable=DTyp.cLikes, reverse=False, key=lambda x: x.data.getCLikes(), cmp="<"):
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")
    print("\nAfter sorting (descending):")
    for node in ct.iterSort(variable=DTyp.cLikes, reverse=True, key=lambda x: x.data.getCLikes(), cmp="<"):
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")
    print("\nAfter sorting (reversed, descending):")
    for node in ct.iterSort(variable=DTyp.cLikes, reverse=False, key=lambda x: x.data.getCLikes(), cmp="<"):
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")

    print("\n=== Testing Complete ===")

    # Test iterPostSort method
    print("\n=== iterPostSort Test ===")
    print("Before sorting:")
    for node in ct:
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")
    print("\nAfter sorting:")
    for node in ct.iterPostSort(variable=DTyp.cLikes, reverse=True, key=lambda x: x.data.getCLikes(), cmp="<"):
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")
    print("\nAfter sorting (reversed):")
    for node in ct.iterPostSort(variable=DTyp.cLikes, reverse=False, key=lambda x: x.data.getCLikes(), cmp="<"):
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")
    print("\nAfter sorting (ascending):")
    for node in ct.iterPostSort(variable=DTyp.cLikes, reverse=False, key=lambda x: x.data.getCLikes(), cmp="<"):
        print(f"{node.get_data().getCText()} - cLikes: {node.get_data().getCLikes()}")

    print("\n=== Testing Complete ===")

    # Test toCSV method
    print("\n=== toCSV Test ===")
    ct.toCSV()
    print("\n=== Testing Complete ===")

    # Test fromCSV method
    print("\n=== fromCSV Test ===")
    # find most recent CSV file
    # we know they are stored in a sub directory of there uuidStr
    # then in a subdirectory of their creation date time
    # so we can just find the most recent sub directory
    # and then find the most recent CSV file in that sub directory
    csvDir = "commentTreeCSVs"
    subDirs = [os.path.join(csvDir, o) for o in os.listdir(csvDir) if os.path.isdir(os.path.join(csvDir, o))]
    mostRecentSubDir = max(subDirs, key=os.path.getmtime)
    print(f"Most recent sub directory: {mostRecentSubDir}")
    subSubDirs = [os.path.join(mostRecentSubDir, o) for o in os.listdir(mostRecentSubDir) if os.path.isdir(os.path.join(mostRecentSubDir, o))]
    mostRecentSubSubDir = max(subSubDirs, key=os.path.getmtime)
    print(f"Most recent sub sub directory: {mostRecentSubSubDir}")
    csvFiles = [os.path.join(mostRecentSubSubDir, o) for o in os.listdir(mostRecentSubSubDir) if o.endswith(".csv")]
    mostRecentCSV = max(csvFiles, key=os.path.getmtime)
    print(f"Most recent CSV file: {mostRecentCSV}")
    newTree = CommentTree.fromCSV(mostRecentCSV)
    newTree.print_lot(printVar=DTyp.cText, printVar2=DTyp.cLikes)


    # Check if  equal when from CSV
    print("Check if  equal when from CSV")
    try:
        assert ct == newTree
        print("Trees are the same")
    except AssertionError:
        print("Trees are not the same")
        print("old Tree:")
        ct.print_level_order_tree(ct.get_root(), printVar=DTyp.cText, printVar2=DTyp.cLikes, debug=True)
        print("new Tree:")
        newTree.print_level_order_tree(newTree.get_root(), printVar=DTyp.cText, printVar2=DTyp.cLikes, debug=True)



    # Check if they are deep equal
    try:
        assert ct.deepEQ(newTree)
        print("Trees are deep equal")
    except AssertionError:
        print("Trees are not deep equal")
        print("old Tree:")
        ct.print_level_order_tree(ct.get_root(), printVar=DTyp.cText, printVar2=DTyp.cLikes, debug=True)
        print("new Tree:")
        newTree.print_level_order_tree(newTree.get_root(), printVar=DTyp.cText, printVar2=DTyp.cLikes, debug=True)

    print("\n=== Testing Complete ===")

    # Test toPickle method
    print("\n=== toPickle Test ===")
    ct.toPickle()
    print("\n=== Testing Complete ===")

    # Test fromPickle method
    print("\n=== fromPickle Test ===")
    # find most recent pickle file
    # we know they are stored in a sub directory of there uuidStr
    # then in a subdirectory of their creation date time
    # so we can just find the most recent sub directory
    # and then find the most recent pickle file in that sub directory
    pickleDir = "commentTreePickle"
    subDirs = [os.path.join(pickleDir, o) for o in os.listdir(pickleDir) if os.path.isdir(os.path.join(pickleDir, o))]
    mostRecentSubDir = max(subDirs, key=os.path.getmtime)
    print(f"Most recent sub directory: {mostRecentSubDir}")
    subSubDirs = [os.path.join(mostRecentSubDir, o) for o in os.listdir(mostRecentSubDir) if os.path.isdir(os.path.join(mostRecentSubDir, o))]
    mostRecentSubSubDir = max(subSubDirs, key=os.path.getmtime)
    print(f"Most recent sub sub directory: {mostRecentSubSubDir}")
    pickleFiles = [os.path.join(mostRecentSubSubDir, o) for o in os.listdir(mostRecentSubSubDir) if o.endswith(".pickle")]
    mostRecentPickle = max(pickleFiles, key=os.path.getmtime)
    print(f"Most recent pickle file: {mostRecentPickle}")
    newTree = CommentTree.fromPickle(mostRecentPickle)
    print("old Tree:")
    ct.print_lot(printVar=DTyp.cText, printVar2=DTyp.cLikes)
    print("new Tree:")
    newTree.print_lot(printVar=DTyp.cText, printVar2=DTyp.cLikes)
    # check if the tree is the same as the original tree
    try:
        assert ct == newTree
        print("Trees are the same")
    except AssertionError:
        print("Trees are not the same")
    # check if they are deep equal
    try:
        assert ct.deepEQ(newTree)
        print("Trees are deep equal")
    except AssertionError:
        print("Trees are not deep equal")




