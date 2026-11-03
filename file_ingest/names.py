def normalize_field_name(name: str) -> str:
    """Normalize field names for matching while treating spaces, '_' and '-' alike."""
    return (
        "".join(name.strip().casefold().split())
        .replace("_", "")
        .replace("-", "")
    )
