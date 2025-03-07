from enum import Enum
from Gender import DouyinGender as DGen
from AccountType import AcctType as AccT


class DtType2(Enum):
    """
    Represents the gender types that can be stored in the gender category.
    """

    (cText, cUserLink, cLikes, uName, uLink, uID, IPTerritory, uFollowers, uFollowing, uLikesReceived, uVideoCount,
     uGender, uAge, uBio, uAccountType) = [
        str,
        str,
        int,
        str,
        str,
        str,
        str,
        int,
        int,
        int,
        int,
        DGen,
        int,
        str,
        AccT
    ]
