from typing import Optional, Callable
from pathlib import Path

from qgis.core import QgsGeometry, QgsTask, QgsApplication

from .models import Dataset, DownloadResult
from .clipper import Clipper
from ..providers.base import BaseProvider


class DownloadTask(QgsTask):

    def __init__(
        self,
        provider: BaseProvider,
        dataset: Dataset,
        geometry: QgsGeometry,
        output_dir: Path,
        clip_geometry: Optional[QgsGeometry] = None,
        nodata_value: Optional[int] = None,
        on_file_start: Optional[Callable[[str, int], None]] = None,
        on_progress: Optional[Callable[[int, int, float], None]] = None,
        on_complete: Optional[Callable[[DownloadResult], None]] = None
    ):
        super().__init__(f"Downloading {dataset.name}", QgsTask.CanCancel)
        self.provider = provider
        self.dataset = dataset
        self.geometry = geometry
        self.output_dir = output_dir
        self.clip_geometry = clip_geometry
        self.nodata_value = nodata_value
        self.on_file_start = on_file_start
        self.on_progress = on_progress
        self.on_complete = on_complete
        self.result: Optional[DownloadResult] = None
        self._total_bytes = dataset.size_bytes or 0
        self._bytes_downloaded = 0

    def run(self):
        if self.on_file_start:
            self.on_file_start(self.dataset.name, self._total_bytes)

        def progress_callback(percent, bytes_downloaded=None, total_bytes=None):
            if total_bytes is not None:
                self._total_bytes = total_bytes
            if bytes_downloaded is not None:
                self._bytes_downloaded = bytes_downloaded
            else:
                self._bytes_downloaded = int(self._total_bytes * percent / 100) if self._total_bytes else 0
            self.setProgress(percent * 0.8)
            if self.on_progress:
                self.on_progress(self._bytes_downloaded, self._total_bytes, percent * 0.8)

        self.result = self.provider.download(
            self.dataset,
            self.geometry,
            self.output_dir,
            progress_callback
        )

        if self.result.success and self.clip_geometry:
            try:
                self.setProgress(85)
                clipped_path = Clipper.clip(
                    self.dataset,
                    self.result.output_path,
                    self.clip_geometry,
                    nodata_value=self.nodata_value
                )
                self.result.output_path = clipped_path
                self.result.clipped = True
                self.setProgress(100)
            except Exception as e:
                self.result.error_message = f"Clip failed: {e}"

        return self.result.success

    def finished(self, success):
        if self.on_complete and self.result:
            self.on_complete(self.result)


class DownloadManager:

    def __init__(
        self,
        provider: BaseProvider,
        output_dir: Path,
        clip_geometry: Optional[QgsGeometry] = None,
        nodata_value: Optional[int] = None
    ):
        self.provider = provider
        self.output_dir = output_dir
        self.clip_geometry = clip_geometry
        self.nodata_value = nodata_value
        self.active_tasks: list[DownloadTask] = []

    def download(
        self,
        dataset: Dataset,
        geometry: QgsGeometry,
        on_file_start: Optional[Callable[[str, int], None]] = None,
        on_progress: Optional[Callable[[int, int, float], None]] = None,
        on_complete: Optional[Callable[[DownloadResult], None]] = None
    ) -> DownloadTask:
        task = DownloadTask(
            self.provider,
            dataset,
            geometry,
            self.output_dir,
            clip_geometry=self.clip_geometry,
            nodata_value=self.nodata_value,
            on_file_start=on_file_start,
            on_progress=on_progress,
            on_complete=on_complete
        )
        self.active_tasks.append(task)
        QgsApplication.taskManager().addTask(task)
        return task

    def download_multiple(
        self,
        datasets: list[Dataset],
        geometry: QgsGeometry,
        on_file_start: Optional[Callable[[str, int], None]] = None,
        on_progress: Optional[Callable[[int, int, float], None]] = None,
        on_dataset_complete: Optional[Callable[[DownloadResult], None]] = None,
        on_all_complete: Optional[Callable[[list[DownloadResult]], None]] = None
    ) -> list[DownloadTask]:
        results = []
        completed_count = [0]

        def handle_complete(result: DownloadResult):
            results.append(result)
            completed_count[0] += 1
            if on_dataset_complete:
                on_dataset_complete(result)
            if completed_count[0] == len(datasets) and on_all_complete:
                on_all_complete(results)

        tasks = []
        for dataset in datasets:
            task = self.download(
                dataset, geometry,
                on_file_start=on_file_start,
                on_progress=on_progress,
                on_complete=handle_complete
            )
            tasks.append(task)

        return tasks

    def cancel_all(self):
        for task in self.active_tasks:
            task.cancel()
        self.active_tasks.clear()
