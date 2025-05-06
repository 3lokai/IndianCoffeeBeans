import logging
import os

# List of negative/utility tags to ignore in extraction
NEGATIVE_TAGS = {
    "btpicks", "coffeeonly", "coffee packets", "rec_aer", "rec_bla", "rec_col",
    "rec_com_bl", "rec_fre", "rec_hot", "rec_pou", "coffee", "organic", "kerala",
    "moderate acidity", "low bitterness", "tasting notes", "india", "pack", "packet",
    "blend", "single origin", "estate", "beans", "arabica", "robusta"
    # Add more as you discover them
}

def is_negative_tag(tag: str) -> bool:
    """
    Check if a tag is a negative/utility tag that should be ignored.
    Args:
        tag: The tag string (should be normalized: lowercase, spaces not hyphens/underscores)
    Returns:
        True if tag should be ignored, False otherwise.
    """
    return tag in NEGATIVE_TAGS


def log_unknown_tag(tag: str, log_file: str = None):
    """
    Log an unknown tag for later review. Ensures no duplicates in the log file.
    Args:
        tag: The tag string (should be normalized)
        log_file: Optional path to log file. Defaults to 'logs/unknown_tags.log' in project root.
    """
    if log_file is None:
        log_file = os.path.join(os.path.dirname(__file__), '..', 'logs', 'unknown_tags.log')
        log_file = os.path.abspath(log_file)
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    # Read existing tags
    existing = set()
    if os.path.exists(log_file):
        with open(log_file, 'r', encoding='utf-8') as f:
            for line in f:
                existing.add(line.strip())
    # Only log if not already present
    if tag not in existing:
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(tag + '\n')
