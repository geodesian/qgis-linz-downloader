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
        from qgis.core import QgsMessageLog, Qgis

        layer = QgsRasterLayer(str(input_path), "temp")
        if not layer.isValid():
            raise ValueError(f"Invalid raster: {input_path}")

        provider = layer.dataProvider()

        has_nodata = provider.sourceHasNoDataValue(1)
        if has_nodata:
            src_nodata = provider.sourceNoDataValue(1)
            QgsMessageLog.logMessage(f"Source raster nodata: {src_nodata}", "Clipper", Qgis.Info)
        else:
            QgsMessageLog.logMessage("Source raster has no nodata value", "Clipper", Qgis.Info)

        target_crs = layer.crs()
        source_crs = QgsProject.instance().crs()

        QgsMessageLog.logMessage(f"Raster CRS: {target_crs.authid()}", "Clipper", Qgis.Info)
        QgsMessageLog.logMessage(f"Project CRS: {source_crs.authid()}", "Clipper", Qgis.Info)

        clip_geom = QgsGeometry(geometry)
        if source_crs != target_crs:
            transform = QgsCoordinateTransform(source_crs, target_crs, QgsProject.instance())
            clip_geom.transform(transform)
            QgsMessageLog.logMessage("Transformed clip geometry to raster CRS", "Clipper", Qgis.Info)

        raster_extent = layer.extent()
        clip_bbox = clip_geom.boundingBox()

        QgsMessageLog.logMessage(f"Raster extent: {raster_extent.toString()}", "Clipper", Qgis.Info)
        QgsMessageLog.logMessage(f"Clip bbox: {clip_bbox.toString()}", "Clipper", Qgis.Info)

        if not raster_extent.intersects(clip_bbox):
            raise ValueError(f"Clip geometry does not intersect raster extent")

        if nodata_value is None:
            if has_nodata:
                nodata_value = int(src_nodata)
            else:
                nodata_value = -9999

        QgsMessageLog.logMessage(f"Using nodata value: {nodata_value}", "Clipper", Qgis.Info)

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

        QgsMessageLog.logMessage("Starting GDAL clip operation", "Clipper", Qgis.Info)
        result = processing.run("gdal:cliprasterbymasklayer", params)
        QgsMessageLog.logMessage(f"GDAL clip completed: {result}", "Clipper", Qgis.Info)

        clipped_layer = QgsRasterLayer(str(output_path), "clipped_check")
        if clipped_layer.isValid():
            stats = clipped_layer.dataProvider().bandStatistics(1)
            QgsMessageLog.logMessage(f"Clipped stats - min: {stats.minimumValue}, max: {stats.maximumValue}, range: {stats.range}", "Clipper", Qgis.Info)
            if hasattr(stats, 'validPixelCount'):
                QgsMessageLog.logMessage(f"Valid pixels: {stats.validPixelCount}", "Clipper", Qgis.Info)

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
