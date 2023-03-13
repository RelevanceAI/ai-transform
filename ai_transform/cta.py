from dataclasses import dataclass
from typing import Optional

@dataclass
class FrontendCTA:
    type: str
    link: str
    label: str = ""
    expiry_date: Optional[str] = None

@dataclass
class DownloadCTA(FrontendCTA):
    type = "download"

@dataclass
class LinkCTA(FrontendCTA):
    type = "link"
