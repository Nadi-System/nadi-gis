# -*- coding: utf-8 -*-

"""
/***************************************************************************
 Nadi
                                 A QGIS plugin
 Nadi (River) connections tool
 Generated by Plugin Builder: http://g-sherman.github.io/Qgis-Plugin-Builder/
                              -------------------
        begin                : 2023-12-21
        copyright            : (C) 2023 by Gaurav Atreya
        email                : allmanpride@gmail.com
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

__author__ = 'Gaurav Atreya'
__date__ = '2023-12-21'
__copyright__ = '(C) 2023 by Gaurav Atreya'

# This will get replaced with a git SHA1 when you do a git archive

__revision__ = '$Format:%H$'

import os
from qgis.PyQt.QtGui import QIcon
from qgis.PyQt.QtCore import QCoreApplication
from qgis.core import (
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterFeatureSource,
    QgsProcessingLayerPostProcessorInterface,
    QgsProcessingParameterVectorDestination,
    QgsProcessingParameterField,
    QgsProcessingUtils,
    QgsRunProcess,
    QgsSymbol,
    QgsLineSymbol,
    QgsVectorLayer,
    QgsInterpolatedLineColor,
    QgsInterpolatedLineWidth,
    QgsGraduatedSymbolRenderer,
)
from qgis.core import Qgis
from PyQt5.QtGui import QColor
import pathlib
from .nadi_exe import qgis_nadi_proc


class NadiOrder(QgsProcessingAlgorithm):
    """
    Order the streams based on the connections
    """

    ORDERED_STREAMS = 'ORDERED_STREAMS'
    STREAMS = 'STREAMS'
    REVERSE = 'REVERSE'

    def initAlgorithm(self, config):
        """
        Here we define the inputs and output of the algorithm, along
        with some other properties.
        """
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.STREAMS,
                self.tr('Input Streams'),
                [QgsProcessing.TypeVectorLine]
            )
        )
        self.addParameter(
            QgsProcessingParameterBoolean(
                self.REVERSE,
                self.tr('Reverse Stream Network Direction'),
                False
            )
        )

        self.addParameter(
            QgsProcessingParameterVectorDestination(
                self.ORDERED_STREAMS,
                self.tr('Output Streams')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        streams = self.parameterAsCompatibleSourceLayerPathAndLayerName(
            parameters, self.STREAMS, context, ["gpkg"]
        )
        reverse = self.parameterAsBool(
            parameters, self.REVERSE, context
        )
        ordered = self.parameterAsOutputLayer(
            parameters, self.ORDERED_STREAMS, context
        )
        cmd = ["order"]
        if reverse:
            cmd += ["--reverse"]
        if streams[1] == "":
            streams_file = f"{pathlib.Path(streams[0]).as_posix()}"
        else:
            streams_file = f"{pathlib.Path(streams[0]).as_posix()}::{streams[1]}"
        cmd += [
            streams_file,
            f"{pathlib.Path(ordered).as_posix()}",
        ]

        proc = qgis_nadi_proc(feedback, cmd)
        res = proc.run(feedback)

        if feedback.isCanceled():
            feedback.pushInfo("Cancelled")
        elif res != 0:
            feedback.reportError("Error")
        else:
            feedback.pushInfo("Completed")

        context.layerToLoadOnCompletionDetails(ordered).setPostProcessor(LayerPostProcessor.create())
        return {self.ORDERED_STREAMS: ordered}

    def name(self):
        return 'streams_order'

    def displayName(self):
        return self.tr("Streams Order")

    def group(self):
        return self.tr(self.groupId())

    def groupId(self):
        return 'Vector'

    def icon(self):
        return QIcon(os.path.join(os.path.dirname(__file__), "order.png"))

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return NadiOrder()


class LayerPostProcessor(QgsProcessingLayerPostProcessorInterface):
    instance = None

    def postProcessLayer(self, layer, context, feedback):
        if not isinstance(layer, QgsVectorLayer):
            return
        symbol = QgsSymbol.defaultSymbol(layer.geometryType())
        symbol.setColor(QColor("blue"))
        max_ord = layer.aggregate(Qgis.Aggregate.Max, "order")
        if max_ord[1]:
            rend = QgsGraduatedSymbolRenderer()
            rend.setClassAttribute("order")
            rend.setSourceSymbol(symbol)
            breaks = [
                max_ord[0] * b // 100
                for b in [0, 1, 2, 4, 12, 50]
            ]
            for i in range(1, 6):
                rend.addClassLowerUpper(breaks[i-1], breaks[i])
            rend.addClassLowerUpper(breaks[i], max_ord[0])
            rend.setGraduatedMethod(Qgis.GraduatedMethod.Size)
            rend.setSymbolSizes(0.1, 1.0)
            layer.setRenderer(rend)
        else:
            renderer = layer.renderer().clone()
            renderer.setSymbol(symbol)
            layer.setRenderer(renderer)

    @staticmethod
    def create() -> 'LayerPostProcessor':
        LayerPostProcessor.instance = LayerPostProcessor()
        return LayerPostProcessor.instance
