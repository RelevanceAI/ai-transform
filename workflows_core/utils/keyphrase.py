from typing import List, Dict
from dataclasses import dataclass


@dataclass
class Keyphrase:
    """
    We use this class to enforce the format of a keyphrase and
    make it easier to handle to CRUD operations on Keyphrase Field.

    Member variables are:
    _text: string, the keyphrase text
    _id: string, the id of the keyphrase.
    _ancestors: list of strings, the ancestors of the keyphrase in the taxnonomy.
    _parents: list of strings, the parents of the keyphrase in the taxnonomy.
    _level: integer, the level of the keyphrase.
    _keyphrase_score: float, >=0, the score of the keyphrase.
    _frequency: integer, the frequency of the keyphrase.
    _metadata: dict, the metadata that is associated with the keyphrase.
    """
    _text: str
    _id: str = None
    _ancestors: List[str] = None
    _parents: List[str] = None
    _level: int = 0
    _keyphrase_score: float = 0.0
    _frequency: int = 0
    _metadata: Dict = None