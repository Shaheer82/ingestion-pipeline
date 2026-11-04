def normalize_field_name(name: str) -> str:
    """Compare names case-insensitively while preserving spaces and punctuation."""
    return name.casefold()
