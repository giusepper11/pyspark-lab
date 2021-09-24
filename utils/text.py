import re
from typing import List, Tuple


SUB_PATTERN = re.compile(r"[^\w]+")


def normalize_text(text: str) -> List[str]:
    """Cleans text returning a list of words."""
    splitted = text.split()
    treated = list(set([re.sub(SUB_PATTERN, "", word) for word in splitted]))
    return [word for word in treated if word]


def rdd_normalizer(row: Tuple[str, str]) -> Tuple[str, str]:
    """Cleans RDD tuple from raw files"""
    filepath, text = row
    norm_text = normalize_text(text)
    filename = filepath.rsplit("/", 1)[-1]
    return filename, norm_text
