from .base import BaseProvider
from .linz import LINZProvider

PROVIDERS = {
    "linz": LINZProvider,
}


def get_provider(provider_id: str, **kwargs) -> BaseProvider:
    if provider_id not in PROVIDERS:
        raise ValueError(f"Unknown provider: {provider_id}")
    return PROVIDERS[provider_id](**kwargs)


def list_providers() -> list[dict]:
    return [cls.get_info().__dict__ for cls in PROVIDERS.values()]
