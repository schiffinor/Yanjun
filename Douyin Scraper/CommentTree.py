"""
This is a commentTree data structure.
"""

from typing import Tuple, List, Union, Dict, NewType, Callable, Any, Literal, Optional, Self
import warnings as wn
import Operands as op
import time
from datetime import datetime as dt
from Gender import DouyinGender as DGen
from AccountType import AcctType as AccT
from DataTypes import DtTypes as DTyp
from DataTypeTypes import DtType2 as DTyp2


class UserData:
    """

    """

    def __init__(self):
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
        baseVars = self.__dict__.keys()
        self.baseVars = baseVars
        self.dataCollectionDate: Dict[str, Optional[dt.date]] = {key: None for key in self.baseVars}
        self.dataCollectionTime: Dict[str, Optional[dt.time]] = {key: None for key in self.baseVars}

    def setUName(self, name: str):
        self.uName = name
        self.dataCollectionDates["uName"] = dt.now().date()
        self.dataCollectionTimes["uName"] = dt.now().time()

    def setULink(self, link: str):
        self.uLink = link
        self.dataCollectionDates["uLink"] = dt.now().date()
        self.dataCollectionTimes["uLink"] = dt.now().time()

    def setUID(self, ID: str):
        self.uID = ID
        self.dataCollectionDates["uID"] = dt.now().date()
        self.dataCollectionTimes["uID"] = dt.now().time()

    def setIPTerritory(self, territory: str):
        self.IPTerritory = territory
        self.dataCollectionDates["IPTerritory"] = dt.now().date()
        self.dataCollectionTimes["IPTerritory"] = dt.now().time()

    def setUFollowers(self, followers: int):
        self.uFollowers = followers
        self.dataCollectionDates["uFollowers"] = dt.now().date()
        self.dataCollectionTimes["uFollowers"] = dt.now().time()

    def setUFollowing(self, following: int):
        self.uFollowing = following
        self.dataCollectionDates["uFollowing"] = dt.now().date()
        self.dataCollectionTimes["uFollowing"] = dt.now().time()

    def setULikesReceived(self, likes: int):
        self.uLikesReceived = likes
        self.dataCollectionDates["uLikesReceived"] = dt.now().date()
        self.dataCollectionTimes["uLikesReceived"] = dt.now().time()

    def setUVideoCount(self, count: int):
        self.uVideoCount = count
        self.dataCollectionDates["uVideoCount"] = dt.now().date()
        self.dataCollectionTimes["uVideoCount"] = dt.now().time()

    def setUGender(self, gender: DGen):
        self.uGender = gender
        self.dataCollectionDates["uGender"] = dt.now().date()
        self.dataCollectionTimes["uGender"] = dt.now().time()

    def setUAge(self, age: int):
        self.uAge = age
        self.dataCollectionDates["uAge"] = dt.now().date()
        self.dataCollectionTimes["uAge"] = dt.now().time()

    def setBio(self, bio: str):
        self.uBio = bio
        self.dataCollectionDates["uBio"] = dt.now().date()
        self.dataCollectionTimes["uBio"] = dt.now().time()

    def setUAccountType(self, accountType: AccT):
        self.uAccountType = accountType
        self.dataCollectionDates["uAccountType"] = dt.now().date()
        self.dataCollectionTimes["uAccountType"] = dt.now().time()

    def setAll(self, name: str, link: str, ID: str, territory: str, followers: int, following: int, likes: int,
               count:int, gender: DGen, age: int, bio: str, accountType: AccT, dateC: Optional[dt.date] = None, timeC: Optional[dt.time] = None):
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
        if dateC is not None:
            self.dataCollectionDates = dict(zip(self.baseVars, [dateC] * len(self.baseVars)))
        if timeC is not None:
            self.dataCollectionTimes = dict(zip(self.baseVars, [timeC] * len(self.baseVars)))

    def getUName(self) -> str:
        return self.uName

    def getULink(self) -> str:
        return self.uLink

    def getUID(self) -> str:
        return self.uID

    def getIPTerritory(self) -> str:
        return self.IPTerritory

    def getUFollowers(self) -> int:
        return self.uFollowers

    def getUFollowing(self) -> int:
        return self.uFollowing

    def getULikesReceived(self) -> int:
        return self.uLikesReceived

    def getUVideoCount(self) -> int:
        return self.uVideoCount

    def getUGender(self) -> DGen:
        return self.uGender

    def getUAge(self) -> int:
        return self.uAge

    def getBio(self) -> str:
        return self.uBio

    def getUAccountType(self) -> AccT:
        return self.uAccountType

    def getAll(self) -> Tuple[str, str, str, str, int, int, int, int, DGen, int, str, AccT]:
        return (self.uName, self.uLink, self.uID, self.IPTerritory, self.uFollowers, self.uFollowing,
                self.uLikesReceived, self.uVideoCount, self.uGender, self.uAge, self.uBio, self.uAccountType)

    def getCollectionDates(self) -> Dict[str, dt]:
        return self.dataCollectionDates

    def getCollectionTimes(self) -> Dict[str, dt]:
        return self.dataCollectionTimes

    def getCollectionDate(self, var: str) -> dt:
        return self.dataCollectionDates[var]

    def getCollectionTime(self, var: str) -> dt:
        return self.dataCollectionTimes[var]

    def getCollectionDateTime(self, var: str) -> Tuple[dt, dt]:
        return self.dataCollectionDates[var], self.dataCollectionTimes[var]

    def getCollectionDateTimeAll(self) -> Dict[str, Tuple[dt, dt]]:
        return dict(zip(self.baseVars, [(self.dataCollectionDates[var], self.dataCollectionTimes[var]) for var in self.baseVars]))


class Data:
    """

    """

    def __init__(self):
        self.cText = "" # comment text
        self.cUserLink = ""
        self.cLikes = 0
        self.userData = UserData()
        baseVars = self.__dict__.keys()
        self.baseVars = baseVars
        self.dataCollectionDate: Dict[str, Optional[dt.date]] = {key: None for key in self.baseVars}
        self.dataCollectionTime: Dict[str, Optional[dt.time]] = {key: None for key in self.baseVars}

    def setCText(self, text: str):
        self.cText = text
        self.dataCollectionDate["cText"] = dt.now().date()
        self.dataCollectionTime["cText"] = dt.now().time()

    def setCLikes(self, likes: int):
        self.cLikes = likes
        self.dataCollectionDate["cLikes"] = dt.now().date()
        self.dataCollectionTime["cLikes"] = dt.now().time()

    def setAll(self, text: str, likes: int, dateC: Optional[dt.date] = None, timeC: Optional[dt.time] = None):
        self.setCText(text)
        self.setCLikes(likes)
        if dateC is not None:
            self.dataCollectionDate = dict(zip(self.baseVars, [dateC] * len(self.baseVars)))
        if timeC is not None:
            self.dataCollectionTime = dict(zip(self.baseVars, [timeC] * len(self.baseVars)))

    def getCText(self) -> str:
        return self.cText

    def getCLikes(self) -> int:
        return self.cLikes

    def getAll(self) -> Tuple[str, int]:
        return self.cText, self.cLikes

    def getCollectionDate(self, var: str) -> dt:
        return self.dataCollectionDate[var]

    def getCollectionTime(self, var: str) -> dt:
        return self.dataCollectionTime[var]

    def getCollectionDateTime(self, var: str) -> Tuple[dt, dt]:
        return self.dataCollectionDate[var], self.dataCollectionTime[var]


class Node:
    """

    """


    def __init__(self, value: Data = None, parent: Self = None):
        self.parent: Self = parent
        self.data = Data() if value is None else value
        self.children: List[Self] = [] # Child nodes

    def add_child(self, child: Self):
        self.children.append(child)
        child.parent = self

    def remove_child(self, child: Self):
        self.children.remove(child)
        child.parent = None

    def add_children(self, children: List[Self]):
        for child in children:
            self.add_child(child)

    def remove_children(self, children: List[Self]):
        for child in children:
            self.remove_child(child)

    def get_children(self) -> List[Self]:
        return self.children

    def get_parent(self) -> Self:
        return self.parent


    def get_data(self) -> Data:
        return self.data

    def set_data(self, data: Data):
        self.data = data

    def set_parent(self, parent: Self):
        self.parent = parent

    def is_root(self) -> bool:
        return self.parent is None

    def is_leaf(self) -> bool:
        return len(self.children) == 0

    def is_internal(self) -> bool:
        return not self.is_leaf() and not self.is_root()

    def find_nodes(self, count: int = None, vars: Union[DTyp,List[DTyp], List[List[DTyp]]] = None, data: Any = None,
                   indexLists: List[int] | List[List[int]] = None) -> list | List[Self] | List[List[Self]]:
        """
        Find nodes that match the specified criteria.
        :param count:
        :param vars:
        :param data:
        :param indexLists:
        :return:
        """

        # Initialize the output list
        outList: list | List[Node] | List[List[Node]] = []

        # Interpret the count parameter
        if count is None:
            count = -1
        if count == 0:
            return []
        if count == -1:
            count = len(self.children)
        if count > len(self.children):
            wn.warn("The number of nodes requested exceeds the number of children. Returning all matches.")
            count = len(self.children)

        # Check for invalid input
        if vars is None and data is None and indexLists is None:
            wn.warn(f"You must specify at least one of vars and data or indexList. First {count} nodes.")
        if vars is not None and data is None:
            raise ValueError("You must specify data to match if you specify vars.")
        if data is not None and vars is None:
            raise ValueError("You must specify vars to match if you specify data.")

        # Type check the all parameters
        if not isinstance(vars, list):
            raise TypeError("vars must be a list of DataTypes.")
        if len(vars) == 0:
            wn.warn("vars is empty. Returning all matches.")
        else:
            if any([not isinstance(var, DTyp) for var in vars]):
                raise TypeError("vars must be a list of DataTypes.")
        if data is not None:
            if not isinstance(data, list):
                raise TypeError("data must be a list of DataTypeTypes.")
            if len(data) == 0:
                wn.warn("data is empty. Returning all matches.")
            else:
                if any([not isinstance(datum, DTyp2) for datum in data]):
                    raise TypeError("data must be a list of DataTypeTypes.")
        if len(vars) != len(data):
            raise ValueError("vars and data must have the same length.")

        # Interpret the indexLists parameter
        if indexLists is not None:
            if not isinstance(indexLists, list):
                raise TypeError("indexLists must be a list of lists of integers.")
            if len(indexLists) == 0:
                wn.warn("indexLists is empty. Returning zero matches.")
                return []
            ListLists = [isinstance(element, list) for element in indexLists]
            ListInts = [isinstance(element, int) for element in indexLists]
            allListsBool = all(ListLists)
            allIntsBool = all(ListInts)
            if not (allListsBool or allIntsBool):
                raise TypeError("indexLists must be a list of lists of integers or a list of integers. Lists and integers cannot be mixed.")
            if allListsBool:
                inLists: List[List[int]] = indexLists
                if any([any([not isinstance(element, int) for element in sublist]) for sublist in inLists]):
                    raise TypeError("indexLists must be a list of lists of integers.")
            parsedIndexList: List[List[int]] = [indexLists]

            # IndexLists length overrides count
            count = len(indexLists)

            # Fetch the nodes indexed by the indexLists
            for sublist in parsedIndexList:
                refNode = self
                for index in sublist:
                    try:
                        refNode = refNode.children[index]
                    except IndexError:
                        wn.warn(f"Index {index} is out of range. Returning last valid node.")
                        break
                outList.append(refNode)
            return outList

        # Interpret the vars and data parameters
        if isinstance(vars, DTyp):
            vars: List[List[DTyp]] = [[vars]]
        if isinstance(data, DTyp2):
            data: List[List[DTyp2]] = [[data]]

        # Parse the vars and data parameters
        if isinstance(vars, list) and isinstance(data, list):
            for i in range (len(vars)):
                if type(vars[i]) is not type(data[i]):
                    raise TypeError("vars and data must have the same type.")
                if len(vars[i]) != len(data[i]):
                    raise ValueError("vars and data must have the same length.")
                if any([not (data[i][j] is not vars[i][j]) for j in range(len(vars[i]))]):
                    raise TypeError("vars and data must have the same type.")

            for i in range(len(vars)):
                varSet = vars[i]
                dataSet = data[i]




        # Define recursive var search function
        def _recursiveVarSearch(node: Self, vars: Union[DTyp,List[DTyp]], count: int, outList: List[Self]):
            """

            :param node:
            :param vars:
            :param count:
            :param outList:
            :return:
            """

            # Base case
            if count == 0:
                return outList









        if indexLists is not None:
            pass


        def _recursiveVarSearch(node: Self, vars: Union[DTyp,List[DTyp]], count: int, outList: List[Self]):
            pass

        if vars is not None:
            if isinstance(vars, DTyp):
                vars = [vars]
            for var in vars:
                for child in self.children:
                    if child.data.getCText() == var:
                        outList.append(child)
                        count -= 1
                        if count == 0:
                            return outList



class CommentTree:
    """

    """

    def __init__(self):
        self.root = Node(0, None)
        self.size = 0








if __name__ == "__main__":
    print("Hello")