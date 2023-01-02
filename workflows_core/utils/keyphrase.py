from typing import List


class Keyphrase:
    def __init__(self,
                 _id: str,
                 text: str,
                 ancestors: List[str] = [],
                 parents: List[str] = [],
                 level: int = 0,
                 keyphrase_score: float = 0.0,
                 frequency: int = 0,
                 metadata=None,
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
