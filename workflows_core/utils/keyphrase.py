from typing import List, Dict
from dataclasses import dataclass


@dataclass
class Keyphrase:
    """
    We use this class to enforce the format of a keyphrase and
    make it easier to handle to CRUD operations on Keyphrase Field.

    When declare a Keyphrase object, the following parameters are needed as input:
    text: string, the keyphrase text
    _id: string, the id of the keyphrase.
    ancestors: list of strings, the ancestors of the keyphrase in the taxnonomy.
    parents: list of strings, the parents of the keyphrase in the taxnonomy.
    level: integer, the level of the keyphrase.
    keyphrase_score: float, >=0, the score of the keyphrase.
    frequency: integer, the frequency of the keyphrase.
    metadata: dict, the metadata that is associated with the keyphrase.
    """
    def __init__(self,
                 text: str,
                 _id: str = None,
                 ancestors: List[str] = None,
                 parents: List[str] = None,
                 level: int = 0,
                 keyphrase_score: float = 0.0,
                 frequency: int = 0,
                 metadata: Dict = None,
                 ):
        self._id = _id
        self._text = text
        self._ancestors = ancestors
        self._parents = parents
        self._level = level
        self._keyphrase_score = keyphrase_score
        self._frequency = frequency
        self._metadata = metadata
        if self._id == '' or self._id is None:
            self._id = self._text

    """
    We use the to_dict function to generate the keyphrase in dict format. 
    The output dict can be then used for CURD operations of KeyphraseField.
    """
    def to_dict(self):
        res = {
            "_id": self._id,
            "text": self._text,
            "ancestors": self._ancestors,
            "parents": self._parents,
            "level": self._level,
            "keyphrase_score": self._keyphrase_score,
            "frequency": self._frequency,
            "metadata": self._metadata,
        }
        return res
