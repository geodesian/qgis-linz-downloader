from typing import Optional
from pathlib import Path
import requests
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

from qgis.core import QgsGeometry, QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsProject, QgsRectangle, QgsVectorLayer, QgsVectorFileWriter, QgsWkbTypes

try:
    from owslib.wfs import WebFeatureService
    HAS_OWSLIB = True
except ImportError:
    HAS_OWSLIB = False

from .base import BaseProvider
from ..core.models import Dataset, DatasetCategory, ProviderInfo, DownloadResult, DataType
from ..core.api_keys import APIKeyManager


class LINZProvider(BaseProvider):

    KOORDINATES_DOMAINS = {
        "data.linz.govt.nz": "Land Information New Zealand",
        "data.mfe.govt.nz": "Ministry for the Environment",
        "datafinder.stats.govt.nz": "Statistics New Zealand",
        "geodata.nzdf.mil.nz": "NZ Defence Force",
        "lris.scinfo.org.nz": "Landcare Research",
    }

    DOMAIN = "data.linz.govt.nz"
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

    def __init__(self, api_key_manager: Optional[APIKeyManager] = None):
        if api_key_manager is None:
            api_key_manager = APIKeyManager()
        self.api_key_manager = api_key_manager

    @classmethod
    def get_info(cls) -> ProviderInfo:
        return ProviderInfo(
            id="linz",
            name="LINZ (New Zealand)",
            requires_auth=True,
            auth_url="https://data.linz.govt.nz/my/api/",
            description="Land Information New Zealand Data Service"
        )

    def _get_wfs_url(self, layer_id: str, domain: str = None) -> str:
        if domain is None:
            domain = self.DOMAIN
        api_key = self.api_key_manager.get_api_key(domain)
        return f"https://{domain}/services;key={api_key}/wfs"

    def _get_wcs_url(self, layer_id: str, domain: str = None) -> str:
        if domain is None:
            domain = self.DOMAIN
        api_key = self.api_key_manager.get_api_key(domain)
        return f"https://{domain}/services;key={api_key}/wcs"

    def _get_export_url(self, domain: str = None) -> str:
        if domain is None:
            domain = self.DOMAIN
        return f"https://{domain}/services/api/v1.x/exports/"

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

    def _get_all_wfs_metadata(self, domain: str) -> dict:
        if not HAS_OWSLIB:
            return {}

        api_key = self.api_key_manager.get_api_key(domain)
        if not api_key:
            return {}

        try:
            wfs_url = f"https://{domain}/services;key={api_key}/wfs?service=WFS&version=2.0.0&request=GetCapabilities"
            response = requests.get(wfs_url, timeout=15)
            response.raise_for_status()

            wfs = WebFeatureService(url=None, xml=response.content, version='2.0.0')

            id_regex = re.compile(
                r"([a-zA-Z]+\.[a-zA-Z]+\.[a-zA-Z]+\.[a-zA-Z]+\:)?(?P<type>[a-zA-Z]+)-(?P<id>[0-9]+)"
            )

            metadata = {}
            for dataset_id, dataset_obj in wfs.contents.items():
                match = id_regex.search(dataset_id)
                if match:
                    obj_id = match.group("id")
                    crs_options = [f"EPSG:{item.code}" for item in dataset_obj.crsOptions if hasattr(item, 'code')]

                    valid_crs = []
                    for crs in crs_options:
                        if re.match(r'^EPSG:\d+$', crs):
                            valid_crs.append(crs)

                    if valid_crs:
                        valid_crs.sort(key=lambda x: int(x.split(':')[1]))

                    metadata[obj_id] = {
                        'crs_options': valid_crs,
                        'title': dataset_obj.title,
                        'abstract': dataset_obj.abstract
                    }

            return metadata
        except Exception:
            return {}

    def validate_credentials(self) -> bool:
        configured_domains = self.api_key_manager.get_configured_domains()
        return len(configured_domains) > 0

    def _search_single_domain(
        self,
        domain: str,
        bbox: tuple,
        show_all: bool,
        wfs_metadata: dict,
        categories: dict
    ) -> None:
        api_key = self.api_key_manager.get_api_key(domain)
        if not api_key:
            return

        try:
            params = {
                "key": api_key,
                "page_size": 100
            }

            if bbox and not show_all:
                params["in_bbox"] = f"{bbox[0]},{bbox[1]},{bbox[2]},{bbox[3]}"

            response = requests.get(
                f"https://{domain}/services/api/v1.x/layers/",
                params=params,
                timeout=30
            )

            if response.status_code in [401, 403, 404]:
                return

            response.raise_for_status()
            data = response.json()

            layers = data if isinstance(data, list) else data.get("results", [])

            if not layers:
                return

            all_layer_ids = []
            layer_map = {}

            for layer in layers:
                layer_id = str(layer.get("id", ""))
                layer_map[layer_id] = layer
                all_layer_ids.append(layer_id)

            if bbox:
                valid_ids = self._validate_coverage(all_layer_ids, bbox, domain)
            else:
                valid_ids = set(all_layer_ids)

            for layer_id, layer in layer_map.items():
                data_type = self._detect_data_type(layer)

                has_coverage = layer_id in valid_ids
                is_portal_only = not has_coverage and show_all

                if not has_coverage and not show_all:
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

                layer_wfs_meta = wfs_metadata.get(layer_id, {})
                crs_options = layer_wfs_meta.get('crs_options', [])
                native_crs = crs_options[0] if crs_options else "EPSG:2193"

                dataset = Dataset(
                    id=f"{domain}:{layer_id}",
                    name=f"{name_prefix}{layer_title}",
                    provider="linz",
                    category=category_id,
                    data_type=data_type,
                    crs=native_crs,
                    size_bytes=None,
                    download_url=layer.get("url"),
                    metadata={
                        "domain": domain,
                        "layer_id": layer_id,
                        "type": layer.get("type", ""),
                        "services": services_info,
                        "service_types": service_types,
                        "portal_only": is_portal_only,
                        "direct_download": not is_portal_only,
                        "crs_options": crs_options,
                        "abstract": layer_wfs_meta.get('abstract', '')
                    }
                )
                categories[category_id].datasets.append(dataset)

        except requests.RequestException:
            pass

    def search(self, geometry: QgsGeometry = None, show_all: bool = False) -> list[DatasetCategory]:
        configured_domains = self.api_key_manager.get_configured_domains()
        if not configured_domains:
            raise ValueError("No API keys configured. Please configure API keys in Settings.")

        bbox = None
        if geometry:
            bbox = self._geometry_to_bbox(geometry)

        categories = {}

        for domain in configured_domains:
            wfs_metadata = self._get_all_wfs_metadata(domain)
            self._search_single_domain(domain, bbox, show_all, wfs_metadata, categories)

        return list(categories.values())

    def _check_single_layer_coverage(self, layer_id: str, extent_geojson: dict, domain: str) -> Optional[str]:
        api_key = self.api_key_manager.get_api_key(domain)
        if not api_key:
            return None

        try:
            export_data = {
                "items": [{"item": f"https://{domain}/services/api/v1.x/layers/{layer_id}/"}],
                "crs": "EPSG:4326",
                "formats": {
                    "grid": "image/tiff;subtype=geotiff",
                    "raster": "image/tiff;subtype=geotiff",
                    "vector": "application/x-ogc-gpkg"
                },
                "extent": extent_geojson
            }

            response = requests.post(
                f"https://{domain}/services/api/v1.x/exports/",
                headers={"Content-Type": "application/json", "Authorization": f"key {api_key}"},
                json=export_data,
                timeout=10
            )

            if response.status_code == 201:
                export_info = response.json()
                export_id = export_info.get("id")
                if export_id:
                    try:
                        requests.delete(
                            f"https://{domain}/services/api/v1.x/exports/{export_id}/",
                            headers={"Authorization": f"key {api_key}"},
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

    def _validate_coverage(self, layer_ids: list, bbox: tuple, domain: str) -> set:
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
                executor.submit(self._check_single_layer_coverage, layer_id, extent_geojson, domain): layer_id
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

        domain = dataset.metadata.get("domain", self.DOMAIN)
        api_key = self.api_key_manager.get_api_key(domain)
        if not api_key:
            return DownloadResult(
                dataset=dataset,
                output_path=output_dir,
                success=False,
                error_message=f"API key required for {domain}"
            )

        bbox = self._geometry_to_bbox(geometry)
        layer_id = dataset.metadata.get("layer_id", dataset.id)
        safe_name = dataset.name.replace(" ", "_").replace("/", "-").replace("(", "").replace(")", "")

        try:
            if dataset.data_type == DataType.RASTER:
                return self._download_raster(layer_id, bbox, safe_name, output_dir, dataset, progress_callback, geometry)
            else:
                return self._download_vector(layer_id, bbox, safe_name, output_dir, dataset, progress_callback, geometry)
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
        progress_callback: Optional[callable] = None,
        geometry: Optional[QgsGeometry] = None
    ) -> DownloadResult:
        export_result = self._try_vector_export_download(layer_id, bbox, safe_name, output_dir, dataset, progress_callback, geometry)
        if export_result.success:
            return export_result

        return DownloadResult(
            dataset=dataset,
            output_path=output_dir,
            success=False,
            error_message=f"Export API failed: {export_result.error_message}"
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

        domain = dataset.metadata.get("domain", self.DOMAIN)
        layer_type = dataset.metadata.get("type", "layer")
        url = self._get_wcs_url(layer_id, domain)
        params = {
            "service": "WCS",
            "version": "1.0.0",
            "request": "GetCoverage",
            "coverage": f"{layer_type}-{layer_id}",
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

    def _try_vector_export_download(
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
        import zipfile

        domain = dataset.metadata.get("domain", self.DOMAIN)
        api_key = self.api_key_manager.get_api_key(domain)
        export_url = self._get_export_url(domain)

        if geometry:
            extent_geojson = self._geometry_to_geojson(geometry, "EPSG:4326")
        else:
            bbox_geom = QgsGeometry.fromRect(QgsRectangle(bbox[0], bbox[1], bbox[2], bbox[3]))
            extent_geojson = self._geometry_to_geojson(bbox_geom, "EPSG:4326")

        export_data = {
            "items": [{"item": f"https://{domain}/services/api/v1.x/layers/{layer_id}/"}],
            "crs": "EPSG:4326",
            "formats": {
                "vector": "application/x-ogc-gpkg"
            },
            "extent": extent_geojson
        }

        try:
            create_response = requests.post(
                export_url,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": f"key {api_key}"
                },
                json=export_data,
                timeout=None
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
                    error_message="Failed to create export job"
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
                    headers={"Authorization": f"key {api_key}"},
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
                headers={"Authorization": f"key {api_key}"},
                stream=True,
                timeout=None
            )
            file_response.raise_for_status()

            output_path = output_dir / f"{safe_name}.zip"
            total_size = int(file_response.headers.get("content-length", 0))
            downloaded = 0

            with open(output_path, "wb") as f:
                for chunk in file_response.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        if progress_callback and total_size:
                            download_percent = (downloaded / total_size * 100)
                            percent = 60 + (download_percent * 0.4)
                            if progress_callback(percent, downloaded, total_size) == False:
                                return DownloadResult(
                                    dataset=dataset,
                                    output_path=output_dir,
                                    success=False,
                                    error_message="Download cancelled"
                                )

            vector_extensions = ('.gpkg', '.geojson', '.json', '.shp', '.gml', '.kml')
            extracted_path = None

            try:
                with zipfile.ZipFile(output_path, 'r') as zf:
                    for name in zf.namelist():
                        if name.lower().endswith(vector_extensions):
                            zf.extract(name, output_dir)
                            extracted_path = output_dir / name
                            break
                output_path.unlink()
            except Exception as e:
                return DownloadResult(
                    dataset=dataset,
                    output_path=output_dir,
                    success=False,
                    error_message=f"Failed to extract vector file: {e}"
                )

            if not extracted_path or not extracted_path.exists():
                return DownloadResult(
                    dataset=dataset,
                    output_path=output_dir,
                    success=False,
                    error_message="No vector file found in export"
                )

            if progress_callback:
                progress_callback(100, 0, 0)

            return DownloadResult(
                dataset=dataset,
                output_path=extracted_path,
                success=True,
                already_clipped=True
            )

        except Exception as e:
            return DownloadResult(
                dataset=dataset,
                output_path=output_dir,
                success=False,
                error_message=str(e)
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

        domain = dataset.metadata.get("domain", self.DOMAIN)
        api_key = self.api_key_manager.get_api_key(domain)
        export_url = self._get_export_url(domain)

        if geometry:
            extent_geojson = self._geometry_to_geojson(geometry, "EPSG:4326")
        else:
            bbox_geom = QgsGeometry.fromRect(QgsRectangle(bbox[0], bbox[1], bbox[2], bbox[3]))
            extent_geojson = self._geometry_to_geojson(bbox_geom, "EPSG:4326")

        export_data = {
            "items": [{"item": f"https://{domain}/services/api/v1.x/layers/{layer_id}/"}],
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
                    "Authorization": f"key {api_key}"
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
                    headers={"Authorization": f"key {api_key}"},
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
                headers={"Authorization": f"key {api_key}"},
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
                success=True,
                already_clipped=True
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
