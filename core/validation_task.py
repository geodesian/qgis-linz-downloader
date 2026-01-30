from typing import Optional, Callable
from qgis.core import QgsTask

from ..providers.base import BaseProvider


class ValidationTask(QgsTask):

    def __init__(
        self,
        provider: BaseProvider,
        layer_ids: list[str],
        extent_geojson: dict,
        domain: str,
        on_layer_validated: Optional[Callable[[str, bool], None]] = None,
        on_complete: Optional[Callable[[set], None]] = None
    ):
        super().__init__(f"Validating coverage for {domain}", QgsTask.CanCancel)
        self.provider = provider
        self.layer_ids = layer_ids
        self.extent_geojson = extent_geojson
        self.domain = domain
        self.on_layer_validated = on_layer_validated
        self.on_complete = on_complete
        self.valid_ids = set()
        self._processed = 0

    def run(self):
        total = len(self.layer_ids)

        for i, layer_id in enumerate(self.layer_ids):
            if self.isCanceled():
                return False

            result = self.provider._check_single_layer_coverage(
                layer_id,
                self.extent_geojson,
                self.domain
            )

            if result:
                self.valid_ids.add(result)
                if self.on_layer_validated:
                    self.on_layer_validated(result, True)

            self._processed = i + 1
            progress = int((self._processed / total) * 100)
            self.setProgress(progress)

        return True

    def finished(self, success):
        if self.on_complete:
            self.on_complete(self.valid_ids)
