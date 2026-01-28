from pathlib import Path
from typing import Optional

from qgis.core import (
    QgsGeometry,
    QgsVectorLayer,
    QgsRasterLayer,
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsProject,
    QgsFeature,
)
from qgis import processing

from .models import Dataset, DataType


class Clipper:

    @staticmethod
    def clip(
        dataset: Dataset,
        input_path: Path,
        geometry: QgsGeometry,
        output_path: Optional[Path] = None,
        nodata_value: Optional[int] = None
    ) -> Path:
        if output_path is None:
            suffix = input_path.suffix
            output_path = input_path.with_name(f"{input_path.stem}_clipped{suffix}")

        if dataset.data_type == DataType.RASTER:
            return Clipper._clip_raster(input_path, geometry, output_path, nodata_value)
        elif dataset.data_type == DataType.POINTCLOUD:
            return Clipper._clip_pointcloud(input_path, geometry, output_path)
        return Clipper._clip_vector(input_path, geometry, output_path)

    @staticmethod
    def _clip_raster(
        input_path: Path,
        geometry: QgsGeometry,
        output_path: Path,
        nodata_value: Optional[int] = None
    ) -> Path:
        layer = QgsRasterLayer(str(input_path), "temp")
        if not layer.isValid():
            raise ValueError(f"Invalid raster: {input_path}")

        provider = layer.dataProvider()

        has_nodata = provider.sourceHasNoDataValue(1)
        if has_nodata:
            src_nodata = provider.sourceNoDataValue(1)

        target_crs = layer.crs()
        source_crs = QgsProject.instance().crs()

        clip_geom = QgsGeometry(geometry)
        if source_crs != target_crs:
            transform = QgsCoordinateTransform(source_crs, target_crs, QgsProject.instance())
            clip_geom.transform(transform)

        raster_extent = layer.extent()
        clip_bbox = clip_geom.boundingBox()

        if not raster_extent.intersects(clip_bbox):
            raise ValueError(f"Clip geometry does not intersect raster extent")

        if nodata_value is None:
            if has_nodata:
                nodata_value = int(src_nodata)
            else:
                nodata_value = -9999

        params = {
            "INPUT": str(input_path),
            "MASK": Clipper._geometry_to_layer(clip_geom, target_crs),
            "OUTPUT": str(output_path),
            "CROP_TO_CUTLINE": True,
            "KEEP_RESOLUTION": True,
            "NODATA": nodata_value,
            "SOURCE_CRS": target_crs,
            "TARGET_CRS": target_crs,
            "MULTITHREADING": True
        }

        processing.run("gdal:cliprasterbymasklayer", params)

        return output_path

    @staticmethod
    def _clip_vector(input_path: Path, geometry: QgsGeometry, output_path: Path) -> Path:
        layer = QgsVectorLayer(str(input_path), "temp", "ogr")
        if not layer.isValid():
            raise ValueError(f"Invalid vector: {input_path}")

        target_crs = layer.crs()
        source_crs = QgsProject.instance().crs()

        clip_geom = QgsGeometry(geometry)
        if source_crs != target_crs:
            transform = QgsCoordinateTransform(source_crs, target_crs, QgsProject.instance())
            clip_geom.transform(transform)

        processing.run("native:clip", {
            "INPUT": str(input_path),
            "OVERLAY": Clipper._geometry_to_layer(clip_geom, target_crs),
            "OUTPUT": str(output_path)
        })

        return output_path

    @staticmethod
    def _clip_pointcloud(input_path: Path, geometry: QgsGeometry, output_path: Path) -> Path:
        return input_path

    @staticmethod
    def _geometry_to_layer(geometry: QgsGeometry, crs: QgsCoordinateReferenceSystem) -> QgsVectorLayer:
        layer = QgsVectorLayer(f"Polygon?crs={crs.authid()}", "clip_mask", "memory")
        provider = layer.dataProvider()
        feature = QgsFeature()
        feature.setGeometry(geometry)
        provider.addFeature(feature)
        layer.updateExtents()
        return layer
