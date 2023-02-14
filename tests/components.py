"""
Test components
"""

from ai_transform.components import *


def test_instantiate_components():
    EXAMPLE = "base title"
    base_input = BaseInput(title=EXAMPLE, optional=False)
    data = base_input.json()
    assert data["title"] == EXAMPLE
    assert data["props"]["optional"] == False, data
