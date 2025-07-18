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
    QgsLineSymbol,
    QgsProcessing,
    QgsProcessingAlgorithm,
    QgsProcessingLayerPostProcessorInterface,
    QgsProcessingParameterBoolean,
    QgsProcessingParameterFeatureSource,
    QgsProcessingParameterField,
    QgsProcessingParameterFileDestination,
    QgsProcessingParameterVectorDestination,
    QgsVectorLayer,
)
import pathlib
from .nadi_exe import qgis_nadi_proc


class NadiNetwork(QgsProcessingAlgorithm):
    """
    Find the Network (connections) between the points using a streams network
    """

    CONNECTIONS = 'CONNECTIONS'
    STREAMS = 'STREAMS'
    REVERSE = 'REVERSE'
    POINTS = 'POINTS'
    SIMPLIFY = 'SIMPLIFY'
    STREAMS_ID = 'STREAMS_ID'
    POINTS_ID = 'POINTS_ID'
    OUTPUT = 'OUTPUT'
    SNAP_LINES = 'SNAP_LINES'

    def initAlgorithm(self, config):
        self.addParameter(
            QgsProcessingParameterFeatureSource(
                self.STREAMS,
                self.tr('Streams Network'),
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
            QgsProcessingParameterFeatureSource(
                self.POINTS,
                self.tr('Node Points'),
                [QgsProcessing.TypeVectorPoint]
            )
        )

        self.addParameter(
            QgsProcessingParameterField(
                self.POINTS_ID,
                self.tr("Primary Key Field for Node Points"),
                None,
                self.POINTS,
                optional=True
            )
        )

        self.addParameter(
            QgsProcessingParameterBoolean(
                self.SIMPLIFY,
                self.tr('Simplify Connections'),
                False
            )
        )

        self.addParameter(
            QgsProcessingParameterVectorDestination(
                self.CONNECTIONS,
                self.tr('Output Network')
            )
        )

        self.addParameter(
            QgsProcessingParameterVectorDestination(
                self.SNAP_LINES,
                self.tr('Snap Lines')
            )
        )

        self.addParameter(
            QgsProcessingParameterFileDestination(
                self.OUTPUT,
                self.tr('Output Network Text File')
            )
        )

    def processAlgorithm(self, parameters, context, feedback):
        streams = self.parameterAsCompatibleSourceLayerPathAndLayerName(
            parameters, self.STREAMS, context, ["gpkg"]
        )
        reverse = self.parameterAsBool(
            parameters, self.REVERSE, context
        )
        points = self.parameterAsCompatibleSourceLayerPathAndLayerName(
            parameters, self.POINTS, context, ["gpkg"]
        )
        points_id = self.parameterAsString(parameters, self.POINTS_ID, context)
        connection = self.parameterAsOutputLayer(
            parameters, self.CONNECTIONS, context
        )
        snap_lines = self.parameterAsOutputLayer(
            parameters, self.SNAP_LINES, context
        )
        output = self.parameterAsFileOutput(
            parameters, self.OUTPUT, context
        )
        simplify = self.parameterAsBool(
            parameters, self.SIMPLIFY, context
        )

        # main command, ignore spatial reference and verbose for progress
        cmd = ["network", "--ignore-spatial-ref", "--verbose"]
        # add the input layers information
        if reverse:
            cmd += ["--reverse"]
        if simplify:
            cmd += ["--endpoints"]
        if points_id:
            cmd += ["--points-field", points_id]
        try:
            if parameters[self.OUTPUT] != QgsProcessing.TEMPORARY_OUTPUT:
                cmd += ["--output", output]
        except KeyError:
            pass
        try:
            if parameters[self.SNAP_LINES] != QgsProcessing.TEMPORARY_OUTPUT:
                cmd += ["--snap-line", str(pathlib.Path(snap_lines).as_posix())]
        except KeyError:
            pass

        if streams[1] == "":
            streams_file = f"{pathlib.Path(streams[0]).as_posix()}"
        else:
            streams_file = f"{pathlib.Path(streams[0]).as_posix()}::{streams[1]}"
        if points[1] == "":
            points_file = f"{pathlib.Path(points[0]).as_posix()}"
        else:
            points_file = f"{pathlib.Path(points[0]).as_posix()}::{points[1]}"
        cmd += [
            points_file,
            streams_file,
            "--network", str(pathlib.Path(connection).as_posix()),
        ]

        proc = qgis_nadi_proc(feedback, cmd)
        res = proc.run(feedback)

        if feedback.isCanceled():
            feedback.pushInfo("Cancelled")
        elif res != 0:
            feedback.reportError("Error")
        else:
            feedback.pushInfo("Completed")

        context.layerToLoadOnCompletionDetails(connection).setPostProcessor(LayerPostProcessor.create(simplify))
        return {self.CONNECTIONS: connection}

    def name(self):
        return 'find_connections'

    def displayName(self):
        return self.tr("Find Connections")
    
    def icon(self):
        return QIcon(os.path.join(os.path.dirname(__file__), "network.png"))

    def group(self):
        return self.tr(self.groupId())

    def groupId(self):
        return 'Vector'

    def tr(self, string):
        return QCoreApplication.translate('Processing', string)

    def createInstance(self):
        return NadiNetwork()


class LayerPostProcessor(QgsProcessingLayerPostProcessorInterface):

    instance = None

    def __init__(self, simple):
        self.simple = simple
        super().__init__()
    
    def postProcessLayer(self, layer, context, feedback):
        if not isinstance(layer, QgsVectorLayer):
            return
        renderer = layer.renderer().clone()
        if self.simple:
            symbol = QgsLineSymbol.createSimple({'line_color': 'red', 'line_width': '1.0'})
        else:
            symbol = QgsLineSymbol.createSimple({'line_color': 'blue', 'line_width': '0.5'})

        renderer.setSymbol(symbol)
        layer.setRenderer(renderer)

    @staticmethod
    def create(simple) -> 'LayerPostProcessor':
        LayerPostProcessor.instance = LayerPostProcessor(simple)
        return LayerPostProcessor.instance
