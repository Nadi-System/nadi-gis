import pandas as pd
import geopandas as gpd
from shapely import Point

## USGS JSON to GPKG
df = pd.read_json("nation/test.json")
points = [Point(x,y) for x,y in zip(df.Longitude, df.Latitude)]
gdf = gpd.GeoDataFrame(df, geometry=points)
gdf.SiteNumber = gdf.SiteNumber.map(lambda x: f'{x:08}')
gdf.to_file("nation/usgs.gpkg")

## USGS
# df = gpd.read_file("nation/usgs.gpkg")
# all(df.groupby("SiteNumber").geometry.agg(list).map(lambda x: all([x[0] == y for y in x])))
# # after making sure all duplicate site numbers have same geometry:
# usgs = df.groupby("SiteNumber").first().reset_index()
# usgs.SiteNumber = usgs.SiteNumber.map(lambda s: f'{s:08}')
# usgs.to_file("nation/usgs-uniq.gpkg")

gages = gpd.read_file("nation/GageLoc.shp.zip")
gages.loc[:, "SiteNumber"] = gages.SOURCE_FEA
usgs = gages.groupby("SiteNumber").first().reset_index()
usgs.to_file("nation/usgs-uniq.gpkg")

## NID
df = gpd.read_file("nation/nid.gpkg")
df.yearCompleted = pd.to_numeric(df.yearCompleted)

# nid dams do not have same geometry even with same NID ID
all(df.groupby("nidId").geometry.agg(list).map(lambda x: all([x[0] == y for y in x])))

# groupby nid dams to make them unique
yr = df.groupby("nidId").yearCompleted.agg(min)
new_df = df.groupby("nidId").first()
new_df.yearCompleted = yr

new_df.to_file("nation/nid-uniq.gpkg")

## Large Dams

## Combined Dam + Gages

usgs = gpd.read_file("nation/usgs-uniq.gpkg", columns=["SiteNumber"])
nid = gpd.read_file("nation/nid.gpkg", columns=["nidId"])

usgs.columns = ["uniqueId", "geometry"]
nid.columns = ["uniqueId", "geometry"]

# usgs.uniqueId = usgs.uniqueId.map(lambda x: f'{x:08}')

comb = pd.concat([usgs, nid])
comb.to_file("nation/dam+gages.gpkg")

## usgs gages

df = gpd.read_file("nation/usgs-uniq.gpkg")
gages = gpd.read_file("nation/GageLoc.shp.zip")
ohio = gpd.read_file("data/03399800_basin.json")
basin = ohio.iloc[0, 0]

clipped = pd.concat([comb.clip(basin), usgs.loc[usgs.uniqueId=="03399800"]])
clipped.to_file("data/ohio.gpkg", layer="dam+gages")

clip_df = df.clip(basin)
clip_gages = gages.clip(basin)

gf = set(clip_gages.SOURCE_FEA)
tf = set(clip_df.SiteNumber.map(lambda x: f"{x:08}"))
len(gf), len(tf)
len(tf.difference(gf))
len(gf.difference(tf))


clip_df.to_file("/tmp/df.gpkg")
clip_gages.to_file("/tmp/gages.gpkg")
