from pathlib import Path

from qgis.PyQt.QtWidgets import QAction
from qgis.PyQt.QtGui import QIcon
from qgis.core import QgsApplication

from .gui.main_dialog import MainDialog


class DataDownloaderPlugin:

    def __init__(self, iface):
        self.iface = iface
        self.plugin_dir = Path(__file__).parent
        self.action = None
        self.dialog = None
        self.toolbar = None

    def initGui(self):
        icon_path = self.plugin_dir / "resources" / "icon.png"
        icon = QIcon(str(icon_path)) if icon_path.exists() else QIcon()

        self.action = QAction(icon, "LINZ Data Downloader", self.iface.mainWindow())
        self.action.triggered.connect(self.run)

        self.toolbar = self._get_or_create_toolbar("Geodesian Tools")
        self.toolbar.addAction(self.action)

        self.iface.addPluginToMenu("LINZ Data Downloader", self.action)

    def _get_or_create_toolbar(self, toolbar_name):
        main_window = self.iface.mainWindow()
        for toolbar in main_window.findChildren(type(main_window.addToolBar(""))):
            if toolbar.windowTitle() == toolbar_name:
                return toolbar
        return main_window.addToolBar(toolbar_name)

    def unload(self):
        self.iface.removePluginMenu("LINZ Data Downloader", self.action)

        if self.toolbar:
            self.toolbar.removeAction(self.action)

        if self.dialog:
            self.dialog.close()
            self.dialog = None

    def run(self):
        if self.dialog is None:
            self.dialog = MainDialog(self.iface)

        self.dialog.show()
        self.dialog.raise_()
        self.dialog.activateWindow()
