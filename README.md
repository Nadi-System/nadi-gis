# NADI (Network Analysis and Data Integration) GIS Tool

This is part of the NADI System. NADI GIS works with Geographical Information System (GIS) file types to do the network detection, or do the import/export network attributes for the network analysis.

Refer to the [NADI Book](https://nadi-system.github.io/) for more detail on the NADI System.

[NADI GIS Example Use](https://nadi-system.github.io/gis/example.html) chapter in the Nadi Book provides a demo for using NADI GIS.

# Installation

## Windows
Precompiled binaries along with `gdal` shared libraryes (`.dll`s) are available in the releases page.

If you want to build NADI GIS from source on Windows, refer to the [Nadi Book Installation chapter](https://nadi-system.github.io/installation.html#nadi-gis)

## Linux and MacOS
`nadi` binary can be installed using the rust ecosystem. You need `git`, `cargo` and `gdal` as pre-requisites. `gdal` can be installed in Linux through your package manager. 

In Linux `gdal` is available as `gdal` package (Arch), `libgdal-dev` package (Ubuntu) or something similar, search your package manager for exact name. In MacOS using homebrew you can install it with `brew install gdal`.

To compile the program, run `cargo build --release`, and then you'll have the `nadi` binary in the `target/release` folder. Copy that to your `PATH`.

# QGIS Plugin
You can download the `nadi-qgis.zip` from releases and use that on the QGIS Plugin tab using "Install from Zip" option. Or you can copy the `qgis/nadi` directory to the QGIS Plugins directory in your OS.

QGIS plugin will try to use the `nadi-gis` binary available in your `PATH` before using the binaries distributed along with the `zip`. For windows it is easier to use the provided binary, while for Linux and MacOS please use self compiled version.
