from pathlib import Path
from typing import Optional
from enum import Enum
import math
import traceback

from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QComboBox, QPushButton,
    QLineEdit, QLabel, QFileDialog, QMessageBox,
    QTextEdit, QMenu, QCheckBox, QSpinBox, QScrollArea, QWidget
)
from qgis.PyQt.QtCore import Qt
from qgis.core import (
    QgsGeometry, QgsSettings, QgsApplication, QgsProject,
    QgsRasterLayer, QgsVectorLayer, QgsWkbTypes,
    QgsCoordinateReferenceSystem, QgsCoordinateTransform,
    QgsDistanceArea, QgsFeature, QgsField, QgsFillSymbol
)
from qgis.PyQt.QtCore import QVariant

from .widgets.area_tools import AreaType, RectangleTool, SquareTool, PolygonTool
from .widgets.dataset_tree import DatasetTreeWidget
from .widgets.progress_widget import DownloadProgressWidget
from .widgets.collapsible_group import CollapsibleGroupBox
from .api_key_dialog import APIKeyDialog
from ..providers import get_provider, list_providers
from ..providers.base import BaseProvider
from ..core.downloader import DownloadManager
from ..core.models import DataType
from ..core.api_keys import APIKeyManager


class AreaUnit(Enum):
    HECTARES = "ha"
    ACRES = "ac"
    SQ_KILOMETERS = "km²"
    SQ_METERS = "m²"


class MainDialog(QDialog):

    AREA_TOOLS = {
        AreaType.RECTANGLE: RectangleTool,
        AreaType.SQUARE: SquareTool,
        AreaType.POLYGON: PolygonTool,
    }

    UNIT_CONVERSIONS = {
        AreaUnit.HECTARES: 0.0001,
        AreaUnit.ACRES: 0.000247105,
        AreaUnit.SQ_KILOMETERS: 0.000001,
        AreaUnit.SQ_METERS: 1.0,
    }

    def __init__(self, iface, parent=None):
        super().__init__(parent)
        self.iface = iface
        self.settings = QgsSettings()
        self.api_key_manager = APIKeyManager()
        self.area_tool = None
        self.current_geometry: Optional[QgsGeometry] = None
        self.provider: Optional[BaseProvider] = None
        self.download_manager: Optional[DownloadManager] = None
        self.current_area_m2: float = 0.0
        self.area_layer: Optional[QgsVectorLayer] = None

        self._setup_ui()
        self._connect_signals()
        self._load_settings()

    def _setup_ui(self):
        self.setWindowTitle("LINZ Data Downloader")
        self.setMinimumSize(800, 600)

        main_layout = QVBoxLayout(self)

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setFrameShape(QScrollArea.NoFrame)

        scroll_content = QWidget()
        layout = QVBoxLayout(scroll_content)
        layout.setSpacing(5)

        api_group = CollapsibleGroupBox("API Keys")
        api_layout = QHBoxLayout()

        configured_count = len(self.api_key_manager.get_configured_domains())
        status_text = f"{configured_count} portal(s) configured" if configured_count > 0 else "No API keys configured"
        self.api_status_label = QLabel(status_text)
        api_layout.addWidget(self.api_status_label)
        api_layout.addStretch()

        self.configure_keys_btn = QPushButton("Configure API Keys...")
        self.configure_keys_btn.clicked.connect(self._configure_api_keys)
        api_layout.addWidget(self.configure_keys_btn)

        api_group.content_layout().addLayout(api_layout)
        layout.addWidget(api_group)

        area_group = CollapsibleGroupBox("Area Selection")
        area_layout = QHBoxLayout()

        self.area_btn = QPushButton("Area")
        self.area_menu = QMenu(self)
        draw_menu = self.area_menu.addMenu("Draw")
        draw_menu.addAction("Rectangle", lambda: self._start_area_drawing(AreaType.RECTANGLE))
        draw_menu.addAction("Square", lambda: self._start_area_drawing(AreaType.SQUARE))
        draw_menu.addAction("Polygon", lambda: self._start_area_drawing(AreaType.POLYGON))
        self.layer_menu = self.area_menu.addMenu("From Layer")
        self.area_btn.setMenu(self.area_menu)

        self.clear_area_btn = QPushButton("Clear")
        self.area_status = QLabel("No area selected")

        self.unit_combo = QComboBox()
        self.unit_combo.addItem("Hectares", AreaUnit.HECTARES)
        self.unit_combo.addItem("Acres", AreaUnit.ACRES)
        self.unit_combo.addItem("km²", AreaUnit.SQ_KILOMETERS)
        self.unit_combo.addItem("m²", AreaUnit.SQ_METERS)
        self.unit_combo.setFixedWidth(90)

        area_layout.addWidget(self.area_btn)
        area_layout.addWidget(self.unit_combo)
        area_layout.addWidget(self.clear_area_btn)
        area_layout.addWidget(self.area_status)
        area_layout.addStretch()
        area_group.content_layout().addLayout(area_layout)
        layout.addWidget(area_group)

        datasets_group = CollapsibleGroupBox("Available Datasets")
        search_layout = QHBoxLayout()
        self.search_btn = QPushButton("Search LINZ Data")
        self.search_btn.setEnabled(False)
        search_layout.addWidget(self.search_btn)
        self.show_all_checkbox = QCheckBox("Show all datasets (including those without coverage in area)")
        self.show_all_checkbox.setChecked(False)
        search_layout.addWidget(self.show_all_checkbox)
        search_layout.addStretch()
        datasets_group.content_layout().addLayout(search_layout)

        filter_layout = QHBoxLayout()
        filter_layout.addWidget(QLabel("Filter:"))
        self.filter_input = QLineEdit()
        self.filter_input.setPlaceholderText("Search datasets by name, domain, or description...")
        self.filter_input.setClearButtonEnabled(True)
        filter_layout.addWidget(self.filter_input)
        datasets_group.content_layout().addLayout(filter_layout)

        self.dataset_tree = DatasetTreeWidget()
        self.dataset_tree.setMinimumHeight(250)
        datasets_group.content_layout().addWidget(self.dataset_tree)
        self.size_label = QLabel("Selected: 0 items")
        datasets_group.content_layout().addWidget(self.size_label)
        layout.addWidget(datasets_group)

        log_group = CollapsibleGroupBox("Download Log")
        log_header = QHBoxLayout()
        log_header.addStretch()
        self.clear_log_btn = QPushButton("Clear Log")
        self.clear_log_btn.setFixedWidth(80)
        log_header.addWidget(self.clear_log_btn)
        log_group.content_layout().addLayout(log_header)
        self.log_text = QTextEdit()
        self.log_text.setReadOnly(True)
        self.log_text.setMaximumHeight(100)
        log_group.content_layout().addWidget(self.log_text)
        layout.addWidget(log_group)

        output_group = CollapsibleGroupBox("Output")
        folder_layout = QHBoxLayout()
        self.output_path_input = QLineEdit()
        self.output_path_input.setPlaceholderText("Select output folder")
        self.browse_btn = QPushButton("Browse...")
        folder_layout.addWidget(QLabel("Folder:"))
        folder_layout.addWidget(self.output_path_input)
        folder_layout.addWidget(self.browse_btn)
        output_group.content_layout().addLayout(folder_layout)
        options_layout = QHBoxLayout()
        self.import_checkbox = QCheckBox("Import to QGIS project")
        self.import_checkbox.setChecked(True)
        options_layout.addWidget(self.import_checkbox)
        options_layout.addSpacing(20)
        self.nodata_checkbox = QCheckBox("Set NoData value:")
        self.nodata_checkbox.setChecked(False)
        options_layout.addWidget(self.nodata_checkbox)
        self.nodata_spinbox = QSpinBox()
        self.nodata_spinbox.setRange(-99999, 99999)
        self.nodata_spinbox.setValue(-9999)
        self.nodata_spinbox.setEnabled(False)
        self.nodata_spinbox.setFixedWidth(80)
        options_layout.addWidget(self.nodata_spinbox)
        options_layout.addStretch()
        output_group.content_layout().addLayout(options_layout)
        layout.addWidget(output_group)

        layout.addStretch()
        scroll_area.setWidget(scroll_content)
        main_layout.addWidget(scroll_area)

        self.progress_widget = DownloadProgressWidget()
        self.progress_widget.setVisible(False)
        main_layout.addWidget(self.progress_widget)

        button_layout = QHBoxLayout()
        self.download_btn = QPushButton("Download Selected")
        self.download_btn.setEnabled(False)
        button_layout.addStretch()
        button_layout.addWidget(self.download_btn)
        main_layout.addLayout(button_layout)

    def _connect_signals(self):
        self.clear_area_btn.clicked.connect(self._clear_area)
        self.search_btn.clicked.connect(self._search_datasets)
        self.browse_btn.clicked.connect(self._browse_output)
        self.download_btn.clicked.connect(self._start_download)
        self.progress_widget.cancel_requested.connect(self._cancel_download)
        self.dataset_tree.selection_changed.connect(self._update_selection_info)
        self.nodata_checkbox.toggled.connect(self.nodata_spinbox.setEnabled)
        self.clear_log_btn.clicked.connect(self.log_text.clear)
        self.unit_combo.currentIndexChanged.connect(self._update_area_display)
        self.layer_menu.aboutToShow.connect(self._populate_layer_menu)
        self.filter_input.textChanged.connect(self.dataset_tree.filter_datasets)

    def _load_settings(self):
        self.output_path_input.setText(
            self.settings.value("DataDownloader/output_path", "")
        )
        self.import_checkbox.setChecked(
            self.settings.value("DataDownloader/import_to_project", True, type=bool)
        )
        self.nodata_checkbox.setChecked(
            self.settings.value("DataDownloader/set_nodata", False, type=bool)
        )
        self.nodata_spinbox.setValue(
            self.settings.value("DataDownloader/nodata_value", -9999, type=int)
        )

    def _save_settings(self):
        self.settings.setValue("DataDownloader/output_path", self.output_path_input.text())
        self.settings.setValue("DataDownloader/import_to_project", self.import_checkbox.isChecked())
        self.settings.setValue("DataDownloader/set_nodata", self.nodata_checkbox.isChecked())
        self.settings.setValue("DataDownloader/nodata_value", self.nodata_spinbox.value())

    def _configure_api_keys(self):
        dialog = APIKeyDialog(self)
        if dialog.exec_():
            configured_count = len(self.api_key_manager.get_configured_domains())
            status_text = f"{configured_count} portal(s) configured" if configured_count > 0 else "No API keys configured"
            self.api_status_label.setText(status_text)

            has_keys = configured_count > 0
            if has_keys and self.current_geometry:
                self.search_btn.setEnabled(True)

    def _populate_layer_menu(self):
        self.layer_menu.clear()
        layers = QgsProject.instance().mapLayers().values()
        valid_layers = []

        for layer in layers:
            if isinstance(layer, QgsVectorLayer):
                geom_type = layer.geometryType()
                if geom_type in (QgsWkbTypes.PolygonGeometry, QgsWkbTypes.LineGeometry):
                    valid_layers.append(layer)
            elif isinstance(layer, QgsRasterLayer):
                valid_layers.append(layer)

        if not valid_layers:
            action = self.layer_menu.addAction("No suitable layers")
            action.setEnabled(False)
        else:
            for layer in valid_layers:
                self.layer_menu.addAction(layer.name(), lambda l=layer: self._use_layer_extent(l))

    def _use_layer_extent(self, layer):
        extent = layer.extent()
        if extent.isNull() or extent.isEmpty():
            QMessageBox.warning(self, "Error", "Layer has no valid extent")
            return

        geometry = None

        if isinstance(layer, QgsVectorLayer) and layer.geometryType() == QgsWkbTypes.PolygonGeometry:
            features = list(layer.getFeatures())
            if features:
                geometries = [f.geometry() for f in features if f.hasGeometry()]
                if geometries:
                    if len(geometries) == 1:
                        geometry = geometries[0]
                    else:
                        geometry = QgsGeometry.unaryUnion(geometries)

        if not geometry:
            geometry = QgsGeometry.fromRect(extent)

        if layer.crs().isValid():
            project_crs = QgsProject.instance().crs()
            if layer.crs() != project_crs:
                transform = QgsCoordinateTransform(layer.crs(), project_crs, QgsProject.instance())
                geometry.transform(transform)

        self._set_geometry(geometry)

    def _start_area_drawing(self, area_type: AreaType):
        tool_class = self.AREA_TOOLS[area_type]
        self.area_tool = tool_class(self.iface.mapCanvas())
        self.area_tool.area_complete.connect(self._on_area_complete)
        self.iface.mapCanvas().setMapTool(self.area_tool)

        instructions = {
            AreaType.RECTANGLE: "Click and drag to draw rectangle",
            AreaType.SQUARE: "Click and drag to draw square",
            AreaType.POLYGON: "Click to add points, double-click to finish, Esc to cancel",
        }
        self.area_status.setText(instructions[area_type])

    def _on_area_complete(self, geometry: QgsGeometry):
        self.iface.mapCanvas().unsetMapTool(self.area_tool)
        self._set_geometry(geometry)

    def _create_area_layer(self, geometry: QgsGeometry):
        if self.area_layer and self.area_layer.id() in QgsProject.instance().mapLayers():
            QgsProject.instance().removeMapLayer(self.area_layer.id())

        crs = QgsProject.instance().crs()
        self.area_layer = QgsVectorLayer(f"Polygon?crs={crs.authid()}", "Download Area", "memory")

        provider = self.area_layer.dataProvider()
        provider.addAttributes([QgsField("id", QVariant.Int)])
        self.area_layer.updateFields()

        feature = QgsFeature()
        feature.setGeometry(geometry)
        feature.setAttributes([1])
        provider.addFeature(feature)

        symbol = QgsFillSymbol.createSimple({
            "color": "transparent",
            "outline_color": "#ff0000",
            "outline_width": "2",
            "outline_width_unit": "MM"
        })
        self.area_layer.renderer().setSymbol(symbol)

        QgsProject.instance().addMapLayer(self.area_layer)

    def _set_geometry(self, geometry: QgsGeometry):
        self.current_geometry = geometry
        self.current_area_m2 = self._calculate_area_m2(geometry)
        self._update_area_display()
        self._create_area_layer(geometry)
        self.search_btn.setEnabled(True)

    def _calculate_area_m2(self, geometry: QgsGeometry) -> float:
        da = QgsDistanceArea()
        da.setSourceCrs(QgsProject.instance().crs(), QgsProject.instance().transformContext())
        da.setEllipsoid(QgsProject.instance().ellipsoid())
        area = da.measureArea(geometry)
        if math.isnan(area) or area <= 0:
            bbox = geometry.boundingBox()
            area = bbox.width() * bbox.height()
        return area

    def _update_area_display(self):
        if math.isnan(self.current_area_m2) or self.current_area_m2 <= 0:
            self.area_status.setText("Area: calculating...")
            return

        unit = self.unit_combo.currentData()
        conversion = self.UNIT_CONVERSIONS[unit]
        area_value = self.current_area_m2 * conversion

        if area_value >= 1000:
            self.area_status.setText(f"Area: {area_value:,.0f} {unit.value}")
        elif area_value >= 1:
            self.area_status.setText(f"Area: {area_value:.2f} {unit.value}")
        else:
            self.area_status.setText(f"Area: {area_value:.4f} {unit.value}")

    def _clear_area(self):
        self.current_geometry = None
        self.current_area_m2 = 0.0
        self.area_status.setText("No area selected")
        self.search_btn.setEnabled(False)
        self.dataset_tree.clear()

        if self.area_layer and self.area_layer.id() in QgsProject.instance().mapLayers():
            QgsProject.instance().removeMapLayer(self.area_layer.id())
        self.area_layer = None

    def _get_provider(self) -> BaseProvider:
        return get_provider("linz", api_key_manager=self.api_key_manager)

    def _search_datasets(self):
        if not self.current_geometry:
            return

        self.provider = self._get_provider()

        if not self.provider.validate_credentials():
            QMessageBox.warning(
                self,
                "Authentication Error",
                "No API keys configured. Please click 'Configure API Keys...' to add API keys for Koordinates portals."
            )
            return

        self.search_btn.setEnabled(False)
        self.log("Searching LINZ datasets...")

        QgsApplication.processEvents()

        try:
            show_all = self.show_all_checkbox.isChecked()
            categories = self.provider.search(self.current_geometry, show_all=show_all)
            self.dataset_tree.load_categories(categories)
            total = sum(len(c.datasets) for c in categories)
            self.log(f"Found {total} datasets in {len(categories)} categories")
        except Exception as e:
            self.log(f"Search error: {e}")
            self.log(traceback.format_exc())
            QMessageBox.critical(self, "Search Error", str(e))
        finally:
            self.search_btn.setEnabled(True)

    def _browse_output(self):
        folder = QFileDialog.getExistingDirectory(
            self, "Select Output Folder", self.output_path_input.text()
        )
        if folder:
            self.output_path_input.setText(folder)
            self._save_settings()

    def _update_selection_info(self, selected: list):
        self.size_label.setText(f"Selected: {len(selected)} items")
        has_selection = len(selected) > 0
        has_output = bool(self.output_path_input.text())
        self.download_btn.setEnabled(has_selection and has_output)

    def _start_download(self):
        output_path = self.output_path_input.text()
        if not output_path:
            QMessageBox.warning(self, "Error", "Please select an output folder")
            return

        datasets = self.dataset_tree.get_selected_datasets()
        if not datasets:
            return

        self._save_settings()

        output_dir = Path(output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        nodata_value = None
        if self.nodata_checkbox.isChecked():
            nodata_value = self.nodata_spinbox.value()

        self.download_manager = DownloadManager(
            self.provider,
            output_dir,
            clip_geometry=self.current_geometry,
            nodata_value=nodata_value
        )

        self.progress_widget.setVisible(True)
        self.progress_widget.start_download(len(datasets))
        self.download_btn.setEnabled(False)

        self.download_manager.download_multiple(
            datasets,
            self.current_geometry,
            on_file_start=self._on_file_start,
            on_progress=self._on_progress,
            on_dataset_complete=self._on_dataset_complete,
            on_all_complete=self._on_all_complete
        )

    def _on_file_start(self, filename: str, total_bytes: int):
        self.progress_widget.start_file(filename, total_bytes)

    def _on_progress(self, bytes_downloaded: int, total_bytes: int, percent: float):
        self.progress_widget.update_progress(
            bytes_downloaded=bytes_downloaded,
            total_bytes=total_bytes,
            percent=percent
        )

    def _on_dataset_complete(self, result):
        self.progress_widget.file_complete(result.success)
        if result.success:
            self.log(f"Downloaded: {result.dataset.name}")
            if result.warning_message:
                self.log(f"Warning: {result.warning_message}")
            if self.import_checkbox.isChecked():
                self._import_to_project(result)
        else:
            self.log(f"Failed: {result.dataset.name} - {result.error_message}")

    def _import_to_project(self, result):
        try:
            file_path = str(result.output_path)
            layer_name = result.dataset.name

            if result.dataset.data_type == DataType.VECTOR:
                layer = QgsVectorLayer(file_path, layer_name, "ogr")
            else:
                layer = QgsRasterLayer(file_path, layer_name)

            if layer.isValid():
                QgsProject.instance().addMapLayer(layer)
                self.log(f"Imported: {layer_name}")
            else:
                self.log(f"Import failed (invalid layer): {layer_name}")
        except Exception as e:
            self.log(f"Import error: {e}")

    def _on_all_complete(self, results):
        success = sum(1 for r in results if r.success)
        failed = len(results) - success
        self.log(f"Complete: {success} succeeded, {failed} failed")

        self.progress_widget.all_complete(success, failed)
        self.download_btn.setEnabled(True)

    def _cancel_download(self):
        if self.download_manager:
            self.download_manager.cancel_all()
            self.log("Download cancelled")
        self.progress_widget.setVisible(False)
        self.download_btn.setEnabled(True)

    def log(self, message: str):
        self.log_text.append(message)

    def closeEvent(self, event):
        self._save_settings()
        if self.area_tool:
            self.iface.mapCanvas().unsetMapTool(self.area_tool)
        super().closeEvent(event)
