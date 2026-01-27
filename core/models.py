from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from pathlib import Path


class DataType(Enum):
    RASTER = "raster"
    VECTOR = "vector"
    POINTCLOUD = "pointcloud"


@dataclass
class Dataset:
    id: str
    name: str
    provider: str
    category: str
    data_type: DataType
    crs: str
    size_bytes: Optional[int] = None
    download_url: Optional[str] = None
    metadata: dict = field(default_factory=dict)

    @property
    def size_display(self) -> str:
        if self.size_bytes is None:
            return "Unknown"
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if self.size_bytes < 1024:
                return f"{self.size_bytes:.1f} {unit}"
            self.size_bytes /= 1024
        return f"{self.size_bytes:.1f} PB"


@dataclass
class DatasetCategory:
    id: str
    name: str
    datasets: list[Dataset] = field(default_factory=list)


@dataclass
class ProviderInfo:
    id: str
    name: str
    requires_auth: bool
    auth_url: Optional[str] = None
    description: str = ""


@dataclass
class DownloadResult:
    dataset: Dataset
    output_path: Path
    success: bool
    error_message: Optional[str] = None
    warning_message: Optional[str] = None
    clipped: bool = False
