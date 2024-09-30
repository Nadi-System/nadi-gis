# Maintainer: Gaurav Atreya <allmanpride@gmail.com>
pkgname=nadi-gis
pkgver=0.0.1
pkgrel=1
pkgdesc="GIS tool for Network Analysis and Data Integration (NADI) System"
arch=('x86_64')
license=('GPL3')
depends=('gcc-libs' 'gdal')
makedepends=('rust' 'cargo')

build() {
	cargo build --release
}

package() {
    cd "$srcdir"
    mkdir -p "$pkgdir/usr/bin"
    cp "../target/release/${pkgname}" "$pkgdir/usr/bin/${pkgname}"
}
