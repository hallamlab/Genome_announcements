import numpy as np
import pandas as pd
from dataclasses import dataclass

@dataclass
class OmicSet:
    omic_type: str
    condition: str
    features: pd.DataFrame
    mat: np.ndarray
