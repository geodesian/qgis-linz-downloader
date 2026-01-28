from typing import Optional
from pathlib import Path
import requests
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed

from qgis.core import QgsGeometry, QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject, QgsRectangle

from .base import BaseProvider
from ..core.models import Dataset, DatasetCategory, ProviderInfo, DownloadResult, DataType


class LINZProvider(BaseProvider):

    BASE_URL = "https://data.linz.govt.nz/services/api/v1.x"
    SERVICES_URL = "https://data.linz.govt.nz/services"

    LAYER_CATEGORIES = {
        "elevation": ["elevation", "dem", "dsm", "lidar", "dtm", "height", "contour"],
        "cadastral": ["parcels", "titles", "survey", "boundary", "cadastr", "property"],
        "imagery": ["aerial", "satellite", "imagery", "orthophoto", "rgb"],
        "topographic": ["topo", "hydro", "transport", "building", "road", "rail"],
    }

    RASTER_KEYWORDS = [
        "dem", "dsm", "dtm", "lidar", "elevation", "raster", "imagery",
        "aerial", "satellite", "orthophoto", "rgb", "hillshade", "height"
    ]

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key

    @classmethod
    def get_info(cls) -> ProviderInfo:
        return ProviderInfo(
            id="linz",
            name="LINZ (New Zealand)",
            requires_auth=True,
            auth_url="https://data.linz.govt.nz/my/api/",
            description="Land Information New Zealand Data Service"
        )

    def _get_wfs_url(self, layer_id: str) -> str:
        return f"{self.SERVICES_URL};key={self.api_key}/wfs"

    def _get_wcs_url(self, layer_id: str) -> str:
        return f"{self.SERVICES_URL};key={self.api_key}/wcs"

    def _get_export_url(self) -> str:
        return "https://data.linz.govt.nz/services/api/v1.x/exports/"

    def _geometry_to_geojson(self, geometry: QgsGeometry, target_epsg: str = "EPSG:4326") -> dict:
        target_crs = QgsCoordinateReferenceSystem(target_epsg)
        project_crs = QgsProject.instance().crs()

        transformed = QgsGeometry(geometry)
        if project_crs.isValid() and project_crs.authid() != target_crs.authid():
            transform = QgsCoordinateTransform(project_crs, target_crs, QgsProject.instance())
            transformed.transform(transform)

        coords = []
        if transformed.isMultipart():
            polygons = transformed.asMultiPolygon()
            if polygons:
                for ring in polygons[0]:
                    coords.append([[point.x(), point.y()] for point in ring])
        else:
            polygon = transformed.asPolygon()
            if polygon:
                for ring in polygon:
                    coords.append([[point.x(), point.y()] for point in ring])

        if not coords:
            bbox = transformed.boundingBox()
            coords = [[[bbox.xMinimum(), bbox.yMinimum()], [bbox.xMaximum(), bbox.yMinimum()],
                      [bbox.xMaximum(), bbox.yMaximum()], [bbox.xMinimum(), bbox.yMaximum()],
                      [bbox.xMinimum(), bbox.yMinimum()]]]

        return {"type": "Polygon", "coordinates": coords}

    def _geometry_to_bbox(self, geometry: QgsGeometry, target_epsg: str = "EPSG:4326") -> tuple:
        target_crs = QgsCoordinateReferenceSystem(target_epsg)
        project_crs = QgsProject.instance().crs()

        transformed = QgsGeometry(geometry)

        if project_crs.isValid() and project_crs.authid() != target_crs.authid():
            transform = QgsCoordinateTransform(
                project_crs,
                target_crs,
                QgsProject.instance()
            )
            transformed.transform(transform)

        bbox = transformed.boundingBox()

        if bbox.isNull() or bbox.isEmpty():
            original_bbox = geometry.boundingBox()
            if project_crs.isValid() and project_crs.authid() != target_crs.authid():
                transform = QgsCoordinateTransform(
                    project_crs,
                    target_crs,
                    QgsProject.instance()
                )
                bbox = transform.transformBoundingBox(original_bbox)
            else:
                bbox = original_bbox

        return (bbox.xMinimum(), bbox.yMinimum(), bbox.xMaximum(), bbox.yMaximum())

    def _categorize_layer(self, layer_title: str) -> str:
        title_lower = layer_title.lower()
        for category, keywords in self.LAYER_CATEGORIES.items():
            if any(kw in title_lower for kw in keywords):
                return category
        return "other"

    def _detect_data_type(self, layer: dict) -> DataType:
        layer_type = layer.get("type", "").lower()
        layer_title = layer.get("title", layer.get("name", "")).lower()

        if "raster" in layer_type or "coverage" in layer_type:
            return DataType.RASTER

        if any(kw in layer_title for kw in self.RASTER_KEYWORDS):
            return DataType.RASTER

        if "vector" in layer_type or "wfs" in str(layer.get("services", [])).lower():
            return DataType.VECTOR

        services = layer.get("services", [])
        if isinstance(services, list):
            service_types = [s.get("type", "").lower() if isinstance(s, dict) else str(s).lower() for s in services]
            if any("wcs" in s for s in service_types):
                return DataType.RASTER

        return DataType.VECTOR

    def validate_credentials(self) -> bool:
        if not self.api_key or len(self.api_key) < 10:
            return False
        if "/" in self.api_key or "\\" in self.api_key:
            return False
        try:
            response = requests.get(
                f"{self.BASE_URL}/layers/",
                params={"key": self.api_key, "page_size": 1},
                timeout=10
            )
            return response.status_code == 200
        except requests.RequestException:
            return False

    def search(self, geometry: QgsGeometry, show_all: bool = False) -> list[DatasetCategory]:
        if not self.api_key:
            raise ValueError("LINZ API key required. Get one at: https://data.linz.govt.nz/my/api/")

        bbox = self._geometry_to_bbox(geometry)
        categories = {}

        try:
            response = requests.get(
                f"{self.BASE_URL}/layers/",
                params={
                    "key": self.api_key,
                    "in_bbox": f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}",
                    "page_size": 100
                },
                timeout=30
            )

            if response.status_code == 401:
                raise ValueError("Invalid LINZ API key. Please check your key at: https://data.linz.govt.nz/my/api/")

            if response.status_code == 403:
                raise ValueError("LINZ API access forbidden. Check your API key permissions.")

            response.raise_for_status()
            data = response.json()

            layers = data if isinstance(data, list) else data.get("results", [])

            if not layers:
                return []

            raster_layer_ids = []
            layer_map = {}

            for layer in layers:
                layer_id = str(layer.get("id", ""))
                layer_map[layer_id] = layer
                data_type = self._detect_data_type(layer)
                if data_type == DataType.RASTER:
                    raster_layer_ids.append(layer_id)

            valid_raster_ids = self._validate_raster_coverage(raster_layer_ids, bbox) if not show_all else set()

            for layer_id, layer in layer_map.items():
                data_type = self._detect_data_type(layer)

                is_valid_raster = layer_id in valid_raster_ids
                is_portal_only = data_type == DataType.RASTER and not is_valid_raster and show_all

                if data_type == DataType.RASTER and layer_id not in valid_raster_ids and not show_all:
                    continue

                layer_title = layer.get("title", layer.get("name", "Unknown"))
                category_id = self._categorize_layer(layer_title)

                if is_portal_only:
                    category_id = f"{category_id}_portal"

                if category_id not in categories:
                    category_name = category_id.replace("_portal", "").title()
                    if is_portal_only:
                        category_name = f"{category_name} (No Coverage in Area)"
                    categories[category_id] = DatasetCategory(
                        id=category_id,
                        name=category_name
                    )

                services_info = layer.get("services", [])
                service_types = []
                if isinstance(services_info, list):
                    for svc in services_info:
                        if isinstance(svc, dict):
                            service_types.append(svc.get("type", ""))
                        else:
                            service_types.append(str(svc))

                name_prefix = "[No Coverage] " if is_portal_only else ""

                dataset = Dataset(
                    id=layer_id,
                    name=f"{name_prefix}{layer_title}",
                    provider="linz",
                    category=category_id,
                    data_type=data_type,
                    crs="EPSG:2193",
                    size_bytes=None,
                    download_url=layer.get("url"),
                    metadata={
                        "layer_id": layer_id,
                        "type": layer.get("type", ""),
                        "services": services_info,
                        "service_types": service_types,
                        "portal_only": is_portal_only,
                        "direct_download": not is_portal_only
                    }
                )
                categories[category_id].datasets.append(dataset)

        except requests.RequestException as e:
            raise ValueError(f"LINZ API request failed: {e}")

        return list(categories.values())

    def _check_single_layer_coverage(self, layer_id: str, extent_geojson: dict) -> Optional[str]:
        try:
            export_data = {
                "items": [{"item": f"https://data.linz.govt.nz/services/api/v1.x/layers/{layer_id}/"}],
                "crs": "EPSG:4326",
                "formats": {"grid": "image/tiff;subtype=geotiff", "raster": "image/tiff;subtype=geotiff"},
                "extent": extent_geojson
            }

            response = requests.post(
                self._get_export_url(),
                headers={"Content-Type": "application/json", "Authorization": f"key {self.api_key}"},
                json=export_data,
                timeout=10
            )

            if response.status_code == 201:
                export_info = response.json()
                export_id = export_info.get("id")
                if export_id:
                    try:
                        requests.delete(
                            f"{self._get_export_url()}{export_id}/",
                            headers={"Authorization": f"key {self.api_key}"},
                            timeout=5
                        )
                    except:
                        pass
                return layer_id
            elif response.status_code == 200:
                data = response.json()
                if data.get("is_valid", False):
                    return layer_id
        except:
            pass
        return None

    def _validate_raster_coverage(self, layer_ids: list, bbox: tuple) -> set:
        if not layer_ids:
            return set()

        minx, miny, maxx, maxy = bbox
        extent_geojson = {
            "type": "Polygon",
            "coordinates": [[[minx, miny], [maxx, miny], [maxx, maxy], [minx, maxy], [minx, miny]]]
        }

        valid_ids = set()

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_layer = {
                executor.submit(self._check_single_layer_coverage, layer_id, extent_geojson): layer_id
                for layer_id in layer_ids
            }

            for future in as_completed(future_to_layer):
                result = future.result()
                if result:
                    valid_ids.add(result)

        return valid_ids

    def get_size_estimate(self, dataset: Dataset, geometry: QgsGeometry) -> Optional[int]:
        return None

    def download(
        self,
        dataset: Dataset,
        geometry: QgsGeometry,
        output_dir: Path,
        progress_callback: Optional[callable] = None
    ) -> DownloadResult:
        if dataset.metadata.get("portal_only"):
            return DownloadResult(
                dataset=dataset,
                output_path=output_dir,
                success=False,
                error_message="No data coverage in selected area. Try a different location or dataset."
            )

        if not self.api_key:
            return DownloadResult(
                dataset=dataset,
                output_path=output_dir,
                success=False,
                error_message="API key required"
            )

        bbox = self._geometry_to_bbox(geometry)
        layer_id = dataset.metadata.get("layer_id", dataset.id)
        safe_name = dataset.name.replace(" ", "_").replace("/", "-").replace("(", "").replace(")", "")

        try:
            if dataset.data_type == DataType.RASTER:
                return self._download_raster(layer_id, bbox, safe_name, output_dir, dataset, progress_callback, geometry)
            else:
                return self._download_vector(layer_id, bbox, safe_name, output_dir, dataset, progress_callback)
        except Exception as e:
            return DownloadResult(
                dataset=dataset,
                output_path=output_dir,
                success=False,
                error_message=f"Exception: {str(e)}"
            )

    def _download_vector(
        self,
        layer_id: str,
        bbox: tuple,
        safe_name: str,
        output_dir: Path,
        dataset: Dataset,
        progress_callback: Optional[callable] = None
    ) -> DownloadResult:
        url = self._get_wfs_url(layer_id)
        minx, miny, maxx, maxy = bbox

        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": f"layer-{layer_id}",
            "outputFormat": "application/json",
            "srsName": "EPSG:4326",
            "count": 10000,
            "BBOX": f"{minx},{miny},{maxx},{maxy},EPSG:4326"
        }

        output_path = output_dir / f"{safe_name}.geojson"

        response = requests.get(url, params=params, stream=True, timeout=None)

        if response.status_code == 404:
            raise ValueError(f"Layer {layer_id} not found or WFS not available for this layer")

        response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        if "xml" in content_type.lower() and "exception" in response.text.lower():
            raise ValueError(f"WFS service error: {response.text[:500]}")

        total_size = int(response.headers.get("content-length", 0))
        downloaded = 0
        last_percent = 0

        with open(output_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    if progress_callback and total_size:
                        percent = (downloaded / total_size * 100)
                        if percent - last_percent >= 1 or downloaded == total_size:
                            if progress_callback(percent, downloaded, total_size) == False:
                                raise Exception("Download cancelled")
                            last_percent = percent

        return DownloadResult(
            dataset=dataset,
            output_path=output_path,
            success=True
        )

    def _download_raster(
        self,
        layer_id: str,
        bbox: tuple,
        safe_name: str,
        output_dir: Path,
        dataset: Dataset,
        progress_callback: Optional[callable] = None,
        geometry: Optional[QgsGeometry] = None
    ) -> DownloadResult:
        wcs_result = self._try_wcs_download(layer_id, bbox, safe_name, output_dir, dataset, progress_callback)
        if wcs_result.success:
            return wcs_result

        export_result = self._try_export_download(layer_id, bbox, safe_name, output_dir, dataset, progress_callback, geometry)
        if export_result.success:
            return export_result

        final_error = export_result.error_message
        if "outside" in final_error.lower() or "coverage" in final_error.lower():
            final_error = export_result.error_message
        else:
            final_error = f"WCS: {wcs_result.error_message} | Export: {export_result.error_message}"

        return DownloadResult(
            dataset=dataset,
            output_path=output_dir,
            success=False,
            error_message=final_error
        )

    def _try_wcs_download(
        self,
        layer_id: str,
        bbox: tuple,
        safe_name: str,
        output_dir: Path,
        dataset: Dataset,
        progress_callback: Optional[callable] = None
    ) -> DownloadResult:
        minx, miny, maxx, maxy = bbox

        url = self._get_wcs_url(layer_id)
        params = {
            "service": "WCS",
            "version": "1.0.0",
            "request": "GetCoverage",
            "coverage": f"layer-{layer_id}",
            "format": "GeoTIFF",
            "bbox": f"{minx},{miny},{maxx},{maxy}",
            "crs": "EPSG:4326",
            "response_crs": "EPSG:4326",
            "width": 2048,
            "height": 2048
        }

        output_path = output_dir / f"{safe_name}.tif"

        try:
            response = requests.get(url, params=params, stream=True, timeout=None)

            if response.status_code == 404:
                return DownloadResult(
                    dataset=dataset,
                    output_path=output_dir,
                    success=False,
                    error_message="WCS 404 - layer not available via WCS"
                )

            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if "xml" in content_type.lower():
                error_text = response.text[:1000]
                if "exception" in error_text.lower() or "error" in error_text.lower():
                    return DownloadResult(
                        dataset=dataset,
                        output_path=output_dir,
                        success=False,
                        error_message=f"WCS XML error: {error_text[:200]}"
                    )

            total_size = int(response.headers.get("content-length", 0))
            downloaded = 0
            last_percent = 0

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback and total_size:
                            percent = (downloaded / total_size * 100)
                            if percent - last_percent >= 1 or downloaded == total_size:
                                if progress_callback(percent, downloaded, total_size) == False:
                                    raise Exception("Download cancelled")
                                last_percent = percent

            return DownloadResult(
                dataset=dataset,
                output_path=output_path,
                success=True
            )
        except Exception as e:
            return DownloadResult(
                dataset=dataset,
                output_path=output_dir,
                success=False,
                error_message=f"WCS exception: {type(e).__name__}: {str(e)}"
            )

    def _try_export_download(
        self,
        layer_id: str,
        bbox: tuple,
        safe_name: str,
        output_dir: Path,
        dataset: Dataset,
        progress_callback: Optional[callable] = None,
        geometry: Optional[QgsGeometry] = None
    ) -> DownloadResult:
        import time

        export_url = self._get_export_url()

        if geometry:
            extent_geojson = self._geometry_to_geojson(geometry, "EPSG:4326")
        else:
            bbox_geom = QgsGeometry.fromRect(QgsRectangle(bbox[0], bbox[1], bbox[2], bbox[3]))
            extent_geojson = self._geometry_to_geojson(bbox_geom, "EPSG:4326")

        export_data = {
            "items": [{"item": f"https://data.linz.govt.nz/services/api/v1.x/layers/{layer_id}/"}],
            "crs": "EPSG:4326",
            "formats": {
                "grid": "image/tiff;subtype=geotiff",
                "raster": "image/tiff;subtype=geotiff"
            },
            "extent": extent_geojson
        }

        create_response = None
        last_error = None

        try:
            create_response = requests.post(
                export_url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"key {self.api_key}"
                },
                json=export_data,
                timeout=None
            )
        except requests.RequestException as e:
            last_error = str(e)
            create_response = None

        try:
            if create_response is None:
                return DownloadResult(
                    dataset=dataset,
                    output_path=output_dir,
                    success=False,
                    error_message=f"Export request failed: {last_error or 'Network error'}"
                )

            if create_response.status_code in [401, 403]:
                error_detail = create_response.text[:300]
                return DownloadResult(
                    dataset=dataset,
                    output_path=output_dir,
                    success=False,
                    error_message=f"Export auth ({create_response.status_code}): {error_detail}"
                )

            if create_response.status_code >= 400:
                error_detail = create_response.text
                user_message = f"Export error ({create_response.status_code})"
                try:
                    error_json = create_response.json()
                    items = error_json.get("items", [])
                    if items and isinstance(items, list):
                        for item in items:
                            reasons = item.get("invalid_reasons", [])
                            if "outside-extent" in reasons:
                                user_message = "Selected area is outside this dataset's coverage"
                            elif reasons:
                                user_message = f"Export validation failed: {', '.join(reasons)}"
                except:
                    pass
                return DownloadResult(
                    dataset=dataset,
                    output_path=output_dir,
                    success=False,
                    error_message=user_message
                )

            create_response.raise_for_status()
            export_info = create_response.json()

            export_id = export_info.get("id")
            if not export_id:
                return DownloadResult(
                    dataset=dataset,
                    output_path=output_dir,
                    success=False,
                    error_message="Failed to create export job - no ID returned"
                )

            status_url = f"{export_url}{export_id}/"

            attempt = 0
            while True:
                attempt += 1
                if progress_callback:
                    if progress_callback(min(50, attempt * 0.5), 0, 0) == False:
                        return DownloadResult(
                            dataset=dataset,
                            output_path=output_dir,
                            success=False,
                            error_message="Download cancelled"
                        )

                status_response = requests.get(
                    status_url,
                    headers={"Authorization": f"key {self.api_key}"},
                    timeout=None
                )
                status_response.raise_for_status()
                status_info = status_response.json()

                state = status_info.get("state", "")

                if state == "complete":
                    download_url = status_info.get("download_url")
                    if download_url:
                        break
                elif state in ["error", "cancelled", "failed"]:
                    error_msg = status_info.get("error", state)
                    return DownloadResult(
                        dataset=dataset,
                        output_path=output_dir,
                        success=False,
                        error_message=f"Export failed: {error_msg}"
                    )

                time.sleep(5)

            if progress_callback:
                progress_callback(60, 0, 0)

            file_response = requests.get(
                download_url,
                headers={"Authorization": f"key {self.api_key}"},
                stream=True,
                timeout=None
            )
            file_response.raise_for_status()

            output_path = output_dir / f"{safe_name}.zip"
            total_size = int(file_response.headers.get("content-length", 0))
            downloaded = 0
            last_percent = 0

            with open(output_path, "wb") as f:
                for chunk in file_response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback and total_size:
                            download_percent = (downloaded / total_size * 100)
                            percent = 60 + (download_percent * 0.4)
                            if percent - last_percent >= 0.4 or downloaded == total_size:
                                if progress_callback(percent, downloaded, total_size) == False:
                                    raise Exception("Download cancelled")
                                last_percent = percent

            extracted_path = None
            raster_extensions = ('.tif', '.tiff', '.asc', '.img', '.dem', '.hgt', '.bil', '.flt', '.nc', '.grd')
            try:
                with zipfile.ZipFile(output_path, 'r') as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(raster_extensions):
                            zf.extract(name, output_dir)
                            extracted_path = output_dir / name
                            break
                output_path.unlink()
            except:
                pass

            if extracted_path and extracted_path.exists():
                try:
                    from qgis.core import QgsRasterLayer
                    from qgis import processing

                    layer = QgsRasterLayer(str(extracted_path), "temp")

                    if not layer.isValid():
                        current_crs = None
                    else:
                        current_crs = layer.crs().authid()

                    del layer

                    if current_crs == "EPSG:4326":
                        native_crs = dataset.crs or "EPSG:2193"
                        reprojected_path = extracted_path.with_name(f"{extracted_path.stem}_2193{extracted_path.suffix}")

                        processing.run("gdal:warpreproject", {
                            'INPUT': str(extracted_path),
                            'SOURCE_CRS': 'EPSG:4326',
                            'TARGET_CRS': native_crs,
                            'RESAMPLING': 0,
                            'NODATA': None,
                            'TARGET_RESOLUTION': None,
                            'OPTIONS': '',
                            'DATA_TYPE': 0,
                            'TARGET_EXTENT': None,
                            'TARGET_EXTENT_CRS': None,
                            'MULTITHREADING': True,
                            'EXTRA': '',
                            'OUTPUT': str(reprojected_path)
                        })

                        if reprojected_path.exists():
                            for attempt in range(3):
                                try:
                                    extracted_path.unlink()
                                    reprojected_path.rename(extracted_path)
                                    break
                                except (PermissionError, OSError):
                                    if attempt < 2:
                                        time.sleep(0.5)
                                    else:
                                        raise
                except:
                    pass

            final_path = extracted_path if extracted_path and extracted_path.exists() else output_path
            return DownloadResult(
                dataset=dataset,
                output_path=final_path,
                success=True
            )

        except requests.HTTPError as e:
            return DownloadResult(
                dataset=dataset,
                output_path=output_dir,
                success=False,
                error_message=f"Export HTTP error: {str(e)}"
            )
        except Exception as e:
            return DownloadResult(
                dataset=dataset,
                output_path=output_dir,
                success=False,
                error_message=f"Export exception: {type(e).__name__}: {str(e)}"
            )
