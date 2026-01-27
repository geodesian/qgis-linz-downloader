def classFactory(iface):
    from .plugin import DataDownloaderPlugin
    return DataDownloaderPlugin(iface)
