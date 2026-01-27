import time
from qgis.PyQt.QtWidgets import QWidget, QVBoxLayout, QHBoxLayout, QLabel, QProgressBar, QPushButton
from qgis.PyQt.QtCore import Qt, pyqtSignal, QTimer


class DownloadProgressWidget(QWidget):
    cancel_requested = pyqtSignal()

    def __init__(self, parent=None):
        super().__init__(parent)
        self._start_time = None
        self._bytes_downloaded = 0
        self._total_bytes = 0
        self._last_update_time = 0
        self._last_bytes = 0
        self._speed = 0
        self._current_file = ""
        self._file_index = 0
        self._total_files = 0
        self._pending_update = None
        self._setup_ui()

        self._update_timer = QTimer(self)
        self._update_timer.timeout.connect(self._process_pending_update)
        self._update_timer.start(100)

    def _setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)
        layout.setSpacing(6)

        header_layout = QHBoxLayout()
        self.file_label = QLabel()
        self.file_label.setStyleSheet("font-weight: bold;")
        self.file_label.setWordWrap(True)
        header_layout.addWidget(self.file_label, 1)

        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.setFixedWidth(70)
        self.cancel_btn.clicked.connect(self.cancel_requested.emit)
        header_layout.addWidget(self.cancel_btn)
        layout.addLayout(header_layout)

        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimum(0)
        self.progress_bar.setMaximum(100)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setMinimumHeight(20)
        layout.addWidget(self.progress_bar)

        stats_layout = QHBoxLayout()
        stats_layout.setSpacing(15)

        self.size_label = QLabel("0 B / 0 B")
        self.size_label.setMinimumWidth(140)
        stats_layout.addWidget(self.size_label)

        self.speed_label = QLabel("0 B/s")
        self.speed_label.setMinimumWidth(80)
        stats_layout.addWidget(self.speed_label)

        self.eta_label = QLabel("--:--")
        self.eta_label.setMinimumWidth(100)
        stats_layout.addWidget(self.eta_label)

        self.overall_label = QLabel("File 0 of 0")
        self.overall_label.setAlignment(Qt.AlignRight)
        stats_layout.addWidget(self.overall_label, 1)

        layout.addLayout(stats_layout)

    def start_download(self, total_files: int):
        self._total_files = total_files
        self._file_index = 0
        self._reset_file_stats()
        self._update_overall()
        self.cancel_btn.setEnabled(True)

    def start_file(self, filename: str, total_bytes: int = 0):
        self._file_index += 1
        self._current_file = filename
        self._total_bytes = total_bytes
        self._reset_file_stats()
        self._pending_update = {
            'file': filename,
            'bytes': 0,
            'total': total_bytes,
            'speed': 0,
            'start_time': self._start_time,
            'file_index': self._file_index,
            'total_files': self._total_files
        }

    def _reset_file_stats(self):
        self._start_time = time.time()
        self._bytes_downloaded = 0
        self._last_update_time = self._start_time
        self._last_bytes = 0
        self._speed = 0

    def update_progress(self, bytes_downloaded: int = None, percent: float = None, total_bytes: int = None):
        current_time = time.time()

        if total_bytes is not None and total_bytes > 0:
            self._total_bytes = total_bytes

        if bytes_downloaded is not None:
            self._bytes_downloaded = bytes_downloaded
        elif percent is not None and self._total_bytes > 0:
            self._bytes_downloaded = int(self._total_bytes * percent / 100)

        time_diff = current_time - self._last_update_time
        if time_diff >= 0.5:
            bytes_diff = self._bytes_downloaded - self._last_bytes
            self._speed = bytes_diff / time_diff if time_diff > 0 else 0
            self._last_update_time = current_time
            self._last_bytes = self._bytes_downloaded

        self._schedule_update()

    def _schedule_update(self):
        self._pending_update = {
            'file': self._current_file,
            'bytes': self._bytes_downloaded,
            'total': self._total_bytes,
            'speed': self._speed,
            'start_time': self._start_time,
            'file_index': self._file_index,
            'total_files': self._total_files
        }

    def _process_pending_update(self):
        if self._pending_update is None:
            return

        data = self._pending_update
        self._pending_update = None

        self.file_label.setText(f"Downloading: {data['file']}")

        if data['total'] > 0:
            percent = min(100, int(data['bytes'] / data['total'] * 100))
            self.progress_bar.setValue(percent)
            self.size_label.setText(f"{self._format_size(data['bytes'])} / {self._format_size(data['total'])}")
        else:
            self.progress_bar.setValue(0)
            self.size_label.setText(f"{self._format_size(data['bytes'])} / Unknown")

        self.speed_label.setText(f"{self._format_size(data['speed'])}/s")

        if data['speed'] > 0 and data['total'] > 0:
            remaining_bytes = data['total'] - data['bytes']
            eta_seconds = remaining_bytes / data['speed']
            self.eta_label.setText(f"ETA: {self._format_time(eta_seconds)}")
        else:
            elapsed = time.time() - data['start_time'] if data['start_time'] else 0
            self.eta_label.setText(f"Elapsed: {self._format_time(elapsed)}")

        self.overall_label.setText(f"File {data['file_index']} of {data['total_files']}")

    def _update_overall(self):
        self.overall_label.setText(f"File {self._file_index} of {self._total_files}")

    def file_complete(self, success: bool):
        if success:
            self.progress_bar.setValue(100)

    def all_complete(self, success_count: int, fail_count: int):
        self._pending_update = None
        self.file_label.setText(f"Complete: {success_count} succeeded, {fail_count} failed")
        self.progress_bar.setValue(100)
        self.speed_label.setText("")
        self.eta_label.setText("")
        self.cancel_btn.setEnabled(False)

    @staticmethod
    def _format_size(size_bytes: float) -> str:
        if size_bytes < 0:
            size_bytes = 0
        for unit in ["B", "KB", "MB", "GB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"

    @staticmethod
    def _format_time(seconds: float) -> str:
        if seconds < 0:
            return "--:--"
        if seconds < 60:
            return f"{int(seconds)}s"
        if seconds < 3600:
            mins = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{mins}m {secs}s"
        hours = int(seconds // 3600)
        mins = int((seconds % 3600) // 60)
        return f"{hours}h {mins}m"
