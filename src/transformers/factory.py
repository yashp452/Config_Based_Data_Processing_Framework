from .add_ingestion_date import AddIngestionDateTransformer
from .base import BaseTransformer
from .cast_types import CastTypesTransformer
from .drop_duplicates import DropDuplicatesTransformer
from .filter_nulls import FilterNullsTransformer
from .standardize_strings import StandardizeStringsTransformer

_REGISTRY: dict[str, type[BaseTransformer]] = {
    "add_ingestion_date": AddIngestionDateTransformer,
    "cast_types": CastTypesTransformer,
    "drop_duplicates": DropDuplicatesTransformer,
    "filter_nulls": FilterNullsTransformer,
    "standardize_strings": StandardizeStringsTransformer,
}


class TransformerFactory:
    @staticmethod
    def get(name: str, params: dict) -> BaseTransformer:
        if name not in _REGISTRY:
            raise ValueError(
                f"Unknown transformer: '{name}'. "
                f"Registered: {sorted(_REGISTRY)}"
            )
        return _REGISTRY[name](params)

    @staticmethod
    def register(name: str, cls: type[BaseTransformer]) -> None:
        """Add a new transformer without touching orchestrator code."""
        _REGISTRY[name] = cls
