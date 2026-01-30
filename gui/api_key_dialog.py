from qgis.PyQt.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit,
    QPushButton, QFormLayout, QGroupBox, QMessageBox
)
from qgis.PyQt.QtCore import Qt

from ..core.api_keys import APIKeyManager


class APIKeyDialog(QDialog):

    def __init__(self, parent=None):
        super().__init__(parent)
        self.api_key_manager = APIKeyManager()
        self.key_inputs = {}
        self._setup_ui()
        self._load_keys()

    def _setup_ui(self):
        self.setWindowTitle("Configure API Keys")
        self.setMinimumWidth(600)

        layout = QVBoxLayout(self)

        info_label = QLabel(
            "Enter API keys for each Koordinates data portal you wish to access.\n"
            "You can obtain API keys by registering at each portal's website."
        )
        info_label.setWordWrap(True)
        layout.addWidget(info_label)

        form_layout = QFormLayout()
        form_layout.setFieldGrowthPolicy(QFormLayout.ExpandingFieldsGrow)

        for domain, description in self.api_key_manager.KOORDINATES_DOMAINS.items():
            group_box = QGroupBox(description)
            group_layout = QVBoxLayout()

            domain_label = QLabel(f"Domain: {domain}")
            domain_label.setStyleSheet("color: gray; font-size: 10px;")
            group_layout.addWidget(domain_label)

            key_layout = QHBoxLayout()
            key_input = QLineEdit()
            key_input.setPlaceholderText("Enter API key...")
            key_input.setEchoMode(QLineEdit.Password)
            key_layout.addWidget(key_input)

            get_key_btn = QPushButton("Get Key")
            get_key_btn.setFixedWidth(80)
            get_key_btn.clicked.connect(lambda checked, d=domain: self._open_portal(d))
            key_layout.addWidget(get_key_btn)

            group_layout.addLayout(key_layout)
            group_box.setLayout(group_layout)

            form_layout.addRow(group_box)
            self.key_inputs[domain] = key_input

        layout.addLayout(form_layout)

        button_layout = QHBoxLayout()
        button_layout.addStretch()

        save_btn = QPushButton("Save")
        save_btn.clicked.connect(self._save_keys)
        button_layout.addWidget(save_btn)

        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)

        layout.addLayout(button_layout)

    def _load_keys(self):
        for domain, key_input in self.key_inputs.items():
            api_key = self.api_key_manager.get_api_key(domain)
            if api_key:
                key_input.setText(api_key)

    def _save_keys(self):
        saved_count = 0
        for domain, key_input in self.key_inputs.items():
            api_key = key_input.text().strip()
            if api_key:
                self.api_key_manager.set_api_key(domain, api_key)
                saved_count += 1

        QMessageBox.information(
            self,
            "API Keys Saved",
            f"Saved {saved_count} API key(s) successfully."
        )
        self.accept()

    def _open_portal(self, domain: str):
        import webbrowser
        urls = {
            "data.linz.govt.nz": "https://data.linz.govt.nz/my/api/",
            "data.mfe.govt.nz": "https://data.mfe.govt.nz/my/api/",
            "datafinder.stats.govt.nz": "https://datafinder.stats.govt.nz/my/api/",
            "geodata.nzdf.mil.nz": "https://geodata.nzdf.mil.nz/my/api/",
            "lris.scinfo.org.nz": "https://lris.scinfo.org.nz/my/api/",
        }
        url = urls.get(domain)
        if url:
            webbrowser.open(url)
