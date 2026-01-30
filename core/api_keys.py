from qgis.core import QgsSettings


class APIKeyManager:

    KOORDINATES_DOMAINS = {
        "data.linz.govt.nz": "Land Information New Zealand",
        "data.mfe.govt.nz": "Ministry for the Environment",
        "datafinder.stats.govt.nz": "Statistics New Zealand",
        "geodata.nzdf.mil.nz": "NZ Defence Force",
        "lris.scinfo.org.nz": "Landcare Research",
    }

    def __init__(self):
        self.settings = QgsSettings()

    def get_api_key(self, domain: str) -> str:
        return self.settings.value(f"DataDownloader/api_keys/{domain}", "")

    def set_api_key(self, domain: str, api_key: str):
        self.settings.setValue(f"DataDownloader/api_keys/{domain}", api_key)

    def get_all_api_keys(self) -> dict:
        return {
            domain: self.get_api_key(domain)
            for domain in self.KOORDINATES_DOMAINS.keys()
        }

    def has_api_key(self, domain: str) -> bool:
        return bool(self.get_api_key(domain))

    def get_configured_domains(self) -> list:
        return [
            domain for domain in self.KOORDINATES_DOMAINS.keys()
            if self.has_api_key(domain)
        ]
