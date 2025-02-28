from enum import StrEnum

class DtTypes(StrEnum):
    """
    Represents the gender types that can be stored in the gender category.
    """

    uName, uLink, uID, IPTerritory, uFollowers, uFollowing, uLikesReceived, uVideoCount, uGender, uAge, uBio, uAccountType = [
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
        "uAccountType"
    ]
