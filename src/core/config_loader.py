import json

_REQUIRED_TOP = {"dataset", "source", "sink"}
_SUPPORTED_SOURCE_FORMATS = {"csv", "parquet", "json", "delta"}
_SUPPORTED_SINK_FORMATS = {"csv", "parquet", "json", "delta", "postgres"}


def validate(config: dict) -> None:
    missing = _REQUIRED_TOP - set(config)
    if missing:
        raise ValueError(f"Config missing required top-level keys: {sorted(missing)}")

    source = config["source"]
    if "path" not in source:
        raise ValueError("Config 'source' must have a 'path' field")
    src_fmt = source.get("format", "")
    if src_fmt not in _SUPPORTED_SOURCE_FORMATS:
        raise ValueError(
            f"Unsupported source format '{src_fmt}'. "
            f"Supported: {sorted(_SUPPORTED_SOURCE_FORMATS)}"
        )

    transformations = config.get("transformations", [])
    if not isinstance(transformations, list):
        raise ValueError("Config 'transformations' must be a list")

    sink = config["sink"]
    if sink.get("format") not in _SUPPORTED_SINK_FORMATS:
        raise ValueError(
            f"Unsupported sink format '{sink.get('format')}'. "
            f"Supported: {sorted(_SUPPORTED_SINK_FORMATS)}"
        )
