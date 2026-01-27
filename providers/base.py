from abc import ABC, abstractmethod
from typing import Optional
from pathlib import Path

from qgis.core import QgsGeometry

from ..core.models import Dataset, DatasetCategory, ProviderInfo, DownloadResult


class BaseProvider(ABC):

    @classmethod
    @abstractmethod
    def get_info(cls) -> ProviderInfo:
        pass

    @abstractmethod
    def search(self, geometry: QgsGeometry, **kwargs) -> list[DatasetCategory]:
        pass

    @abstractmethod
    def get_size_estimate(self, dataset: Dataset, geometry: QgsGeometry) -> Optional[int]:
        pass

    @abstractmethod
    def download(
        self,
        dataset: Dataset,
        geometry: QgsGeometry,
        output_dir: Path,
        progress_callback: Optional[callable] = None
    ) -> DownloadResult:
        pass

    def validate_credentials(self) -> bool:
        return True
