from enum import StrEnum

class DouyinGender(StrEnum):
    """
    Represents the gender types that can be stored in the gender category.
    """
    male, female, unknown = ["male", "female", "unknown"]
