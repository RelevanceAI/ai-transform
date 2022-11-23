import random
import os
from typing import Optional


def set_seed(seed: Optional[int] = None):
    """Set all seeds to make results reproducible (deterministic mode).
       When seed is None, disables deterministic mode.
    :param seed: an integer to your choosing
    """
    if seed is not None:
        try:
            import torch

            torch.manual_seed(seed)
            torch.cuda.manual_seed_all(seed)
            torch.backends.cudnn.deterministic = True
            torch.backends.cudnn.benchmark = False
        except:
            pass
        try:
            import numpy as np

            np.random.seed(seed)
        except:
            pass
        random.seed(seed)
        os.environ["PYTHONHASHSEED"] = str(seed)
