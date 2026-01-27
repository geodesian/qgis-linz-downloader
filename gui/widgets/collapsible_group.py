from qgis.PyQt.QtWidgets import QWidget, QVBoxLayout, QFrame, QPushButton, QSizePolicy
from qgis.PyQt.QtCore import Qt, pyqtSignal


class CollapsibleGroupBox(QWidget):
    collapsed_changed = pyqtSignal(bool)

    def __init__(self, title: str, parent=None):
        super().__init__(parent)
        self._collapsed = False
        self._title = title

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        self._header = QPushButton(f"▼ {title}")
        self._header.setStyleSheet("""
            QPushButton {
                text-align: left;
                padding: 6px 10px;
                font-weight: bold;
                background-color: #e0e0e0;
                border: 1px solid #c0c0c0;
                border-radius: 3px;
            }
            QPushButton:hover {
                background-color: #d0d0d0;
            }
        """)
        self._header.clicked.connect(self.toggle)
        layout.addWidget(self._header)

        self._content = QFrame()
        self._content.setFrameShape(QFrame.StyledPanel)
        self._content.setStyleSheet("QFrame { border: 1px solid #c0c0c0; border-top: none; }")
        self._content_layout = QVBoxLayout(self._content)
        self._content_layout.setContentsMargins(8, 8, 8, 8)
        layout.addWidget(self._content)

        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Maximum)

    def content_layout(self) -> QVBoxLayout:
        return self._content_layout

    def toggle(self):
        self.set_collapsed(not self._collapsed)

    def set_collapsed(self, collapsed: bool):
        self._collapsed = collapsed
        self._content.setVisible(not collapsed)
        self._header.setText(f"{'▶' if collapsed else '▼'} {self._title}")
        self.collapsed_changed.emit(collapsed)

    def is_collapsed(self) -> bool:
        return self._collapsed
