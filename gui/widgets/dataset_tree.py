from qgis.PyQt.QtWidgets import QTreeWidget, QTreeWidgetItem, QHeaderView, QAbstractItemView
from qgis.PyQt.QtCore import Qt, pyqtSignal
from qgis.PyQt.QtGui import QColor, QBrush

from ...core.models import Dataset, DatasetCategory


class DatasetTreeWidget(QTreeWidget):

    selection_changed = pyqtSignal(list)

    PRIORITY_CATEGORIES = [
        "elevation",
        "digital_elevation_models",
        "dem",
        "lidar",
        "point_clouds",
        "digital_surface_models",
    ]

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setHeaderLabels(["Name", "Type", "Res", "Domain"])
        self.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.itemSelectionChanged.connect(self._on_selection_changed)

        header = self.header()
        header.setSectionResizeMode(0, QHeaderView.Stretch)
        header.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeToContents)

        self.setAlternatingRowColors(True)
        self.setRootIsDecorated(True)
        self.setItemsExpandable(True)

    def _get_category_sort_key(self, category: DatasetCategory) -> tuple:
        cat_id_lower = category.id.lower()
        cat_name_lower = category.name.lower()

        for i, priority in enumerate(self.PRIORITY_CATEGORIES):
            if priority in cat_id_lower or priority in cat_name_lower:
                return (0, i, category.name)

        if "elevation" in cat_name_lower or "dem" in cat_name_lower:
            return (0, 0, category.name)

        return (1, 0, category.name)

    def load_categories(self, categories: list[DatasetCategory]):
        self.clear()

        sorted_categories = sorted(categories, key=self._get_category_sort_key)

        for category in sorted_categories:
            if not category.datasets:
                continue

            count_text = f"({len(category.datasets)} files)"

            category_item = QTreeWidgetItem([
                f"{category.name} {count_text}",
                "",
                "",
                ""
            ])

            font = category_item.font(0)
            font.setBold(True)
            category_item.setFont(0, font)

            category_item.setFlags(
                Qt.ItemIsEnabled | Qt.ItemIsSelectable
            )

            sorted_datasets = sorted(category.datasets, key=lambda d: d.name)
            for dataset in sorted_datasets:
                resolution = dataset.metadata.get("resolution", "") if dataset.metadata else ""
                is_portal_only = dataset.metadata.get("portal_only", False) if dataset.metadata else False
                domain = dataset.metadata.get("domain", "") if dataset.metadata else ""

                dataset_item = QTreeWidgetItem([
                    dataset.name,
                    dataset.data_type.value,
                    resolution,
                    domain
                ])
                dataset_item.setFlags(
                    Qt.ItemIsEnabled | Qt.ItemIsSelectable
                )
                dataset_item.setData(0, Qt.UserRole, dataset)

                if is_portal_only:
                    gray_brush = QBrush(QColor(128, 128, 128))
                    for col in range(4):
                        dataset_item.setForeground(col, gray_brush)
                    font = dataset_item.font(0)
                    font.setItalic(True)
                    dataset_item.setFont(0, font)

                category_item.addChild(dataset_item)

            self.addTopLevelItem(category_item)
            category_item.setExpanded(True)

    def get_selected_datasets(self) -> list[Dataset]:
        selected = []
        for item in self.selectedItems():
            dataset = item.data(0, Qt.UserRole)
            if dataset:
                selected.append(dataset)
            else:
                for i in range(item.childCount()):
                    child = item.child(i)
                    child_dataset = child.data(0, Qt.UserRole)
                    if child_dataset and child_dataset not in selected:
                        selected.append(child_dataset)
        return selected

    def get_total_size(self) -> int:
        return sum(d.size_bytes or 0 for d in self.get_selected_datasets())

    def _on_selection_changed(self):
        self.selection_changed.emit(self.get_selected_datasets())

    def collapse_all_categories(self):
        for i in range(self.topLevelItemCount()):
            self.topLevelItem(i).setExpanded(False)

    def expand_all_categories(self):
        for i in range(self.topLevelItemCount()):
            self.topLevelItem(i).setExpanded(True)

    def remove_dataset_by_layer_id(self, domain: str, layer_id: str):
        for i in range(self.topLevelItemCount()):
            category_item = self.topLevelItem(i)
            for j in range(category_item.childCount() - 1, -1, -1):
                child = category_item.child(j)
                dataset = child.data(0, Qt.UserRole)
                if dataset:
                    dataset_domain = dataset.metadata.get("domain", "")
                    dataset_layer_id = dataset.metadata.get("layer_id", "")
                    if dataset_domain == domain and dataset_layer_id == layer_id:
                        category_item.removeChild(child)

            if category_item.childCount() == 0:
                self.takeTopLevelItem(i)

    def filter_datasets(self, filter_text: str):
        if not filter_text:
            for i in range(self.topLevelItemCount()):
                category_item = self.topLevelItem(i)
                category_item.setHidden(False)
                for j in range(category_item.childCount()):
                    category_item.child(j).setHidden(False)
            return

        filter_lower = filter_text.lower()

        for i in range(self.topLevelItemCount()):
            category_item = self.topLevelItem(i)
            has_visible_children = False

            for j in range(category_item.childCount()):
                dataset_item = category_item.child(j)
                dataset = dataset_item.data(0, Qt.UserRole)

                name = dataset_item.text(0).lower()
                domain = dataset_item.text(3).lower()
                abstract = ""
                if dataset and dataset.metadata:
                    abstract = dataset.metadata.get("abstract", "").lower()

                matches = (
                    filter_lower in name or
                    filter_lower in domain or
                    filter_lower in abstract
                )

                dataset_item.setHidden(not matches)
                if matches:
                    has_visible_children = True

            category_item.setHidden(not has_visible_children)
            if has_visible_children:
                category_item.setExpanded(True)

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} PB"
