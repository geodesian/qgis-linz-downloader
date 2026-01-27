from enum import Enum

from qgis.PyQt.QtCore import pyqtSignal
from qgis.PyQt.QtGui import QColor
from qgis.gui import QgsMapTool, QgsRubberBand
from qgis.core import QgsWkbTypes, QgsPointXY, QgsGeometry


class AreaType(Enum):
    RECTANGLE = "rectangle"
    SQUARE = "square"
    POLYGON = "polygon"


class BaseAreaTool(QgsMapTool):

    area_complete = pyqtSignal(QgsGeometry)

    def __init__(self, canvas):
        super().__init__(canvas)
        self.canvas = canvas
        self.rubber_band = None
        self._setup_rubber_band()

    def _setup_rubber_band(self):
        self.rubber_band = QgsRubberBand(self.canvas, QgsWkbTypes.PolygonGeometry)
        self.rubber_band.setColor(QColor(255, 0, 0, 100))
        self.rubber_band.setWidth(2)
        self.rubber_band.setFillColor(QColor(255, 0, 0, 50))

    def reset(self):
        if self.rubber_band:
            self.rubber_band.reset(QgsWkbTypes.PolygonGeometry)

    def deactivate(self):
        self.reset()
        super().deactivate()


class RectangleTool(BaseAreaTool):

    def __init__(self, canvas):
        super().__init__(canvas)
        self.start_point = None

    def canvasPressEvent(self, event):
        self.start_point = self.toMapCoordinates(event.pos())

    def canvasMoveEvent(self, event):
        if not self.start_point:
            return
        end_point = self.toMapCoordinates(event.pos())
        self._update_rubber_band(end_point)

    def canvasReleaseEvent(self, event):
        if not self.start_point:
            return
        end_point = self.toMapCoordinates(event.pos())
        points = self._get_rectangle_points(self.start_point, end_point)
        geometry = QgsGeometry.fromPolygonXY([points])
        self.area_complete.emit(geometry)
        self.reset()
        self.start_point = None

    def _get_rectangle_points(self, start, end):
        return [
            QgsPointXY(start.x(), start.y()),
            QgsPointXY(end.x(), start.y()),
            QgsPointXY(end.x(), end.y()),
            QgsPointXY(start.x(), end.y()),
            QgsPointXY(start.x(), start.y())
        ]

    def _update_rubber_band(self, end_point):
        self.rubber_band.reset(QgsWkbTypes.PolygonGeometry)
        points = self._get_rectangle_points(self.start_point, end_point)
        for point in points:
            self.rubber_band.addPoint(point)

    def reset(self):
        super().reset()
        self.start_point = None


class SquareTool(BaseAreaTool):

    def __init__(self, canvas):
        super().__init__(canvas)
        self.start_point = None

    def canvasPressEvent(self, event):
        self.start_point = self.toMapCoordinates(event.pos())

    def canvasMoveEvent(self, event):
        if not self.start_point:
            return
        end_point = self.toMapCoordinates(event.pos())
        self._update_rubber_band(end_point)

    def canvasReleaseEvent(self, event):
        if not self.start_point:
            return
        end_point = self.toMapCoordinates(event.pos())
        points = self._get_square_points(self.start_point, end_point)
        geometry = QgsGeometry.fromPolygonXY([points])
        self.area_complete.emit(geometry)
        self.reset()
        self.start_point = None

    def _get_square_points(self, start, end):
        dx = end.x() - start.x()
        dy = end.y() - start.y()
        side = max(abs(dx), abs(dy))
        sign_x = 1 if dx >= 0 else -1
        sign_y = 1 if dy >= 0 else -1
        return [
            QgsPointXY(start.x(), start.y()),
            QgsPointXY(start.x() + side * sign_x, start.y()),
            QgsPointXY(start.x() + side * sign_x, start.y() + side * sign_y),
            QgsPointXY(start.x(), start.y() + side * sign_y),
            QgsPointXY(start.x(), start.y())
        ]

    def _update_rubber_band(self, end_point):
        self.rubber_band.reset(QgsWkbTypes.PolygonGeometry)
        points = self._get_square_points(self.start_point, end_point)
        for point in points:
            self.rubber_band.addPoint(point)

    def reset(self):
        super().reset()
        self.start_point = None


class PolygonTool(BaseAreaTool):

    def __init__(self, canvas):
        super().__init__(canvas)
        self.points = []

    def canvasPressEvent(self, event):
        point = self.toMapCoordinates(event.pos())
        self.points.append(QgsPointXY(point))
        self._update_rubber_band()

    def canvasMoveEvent(self, event):
        if not self.points:
            return
        point = self.toMapCoordinates(event.pos())
        self._update_rubber_band(point)

    def canvasDoubleClickEvent(self, event):
        if len(self.points) < 3:
            return
        geometry = QgsGeometry.fromPolygonXY([self.points])
        self.area_complete.emit(geometry)
        self.reset()

    def keyPressEvent(self, event):
        if event.key() == 16777216:
            self.reset()

    def _update_rubber_band(self, temp_point=None):
        self.rubber_band.reset(QgsWkbTypes.PolygonGeometry)
        points = self.points + ([temp_point] if temp_point else [])
        if len(points) >= 2:
            for point in points:
                self.rubber_band.addPoint(point)
            self.rubber_band.addPoint(points[0])

    def reset(self):
        super().reset()
        self.points = []
