import geopandas as gpd
import psrcelmerpy

# Read in backdated cities and 2020 tracts as GeoDataFrames
cities = gpd.read_file('J:/Projects/Assessor/assessor_permit/gis/cities/cities_2023.shp')
cities.drop('cnty_name', axis=1, inplace=True)
cities = cities.to_crs(2285)

eg_conn = psrcelmerpy.ElmerGeoConn()
tract20 = eg_conn.read_geolayer('tract2020')
tract20 = tract20[['geoid20', 'name20', 'geometry']]
tract20.rename({'geoid20': 'tractid', 'name20': 'tract20'}, axis=1, inplace=True)
tract20 = tract20.to_crs(2285)

del eg_conn

# --------------------------------------------------------------------------------------------------
# Kitsap
current_prcl_path = 'J:/Projects/Assessor/extracts/2024/July_24/Kitsap/Tax_Parcel_lot_polygons_-1608276356925831498/Tax_Parcel_lot_polygons.shp'
base_prcl_path = 'J:/Projects/Assessor/assessor_permit/kitsap/data/base_year/GIS/parcels_2009.shp'

current_parcels = gpd.read_file(current_prcl_path)
current_parcels = current_parcels[['RP_ACCT_ID', 'geometry']]
current_parcels['RP_ACCT_ID'] = current_parcels['RP_ACCT_ID'].astype('object')
current_parcels = current_parcels.to_crs(2285)

base_parcels = gpd.read_file(base_prcl_path)
base_parcels = base_parcels[['RP_ACCT_ID', 'geometry']]
base_parcels.rename({'RP_ACCT_ID': 'base_rid'}, axis=1, inplace=True)
base_parcels['base_rid'] = base_parcels['base_rid'].astype('object')
base_parcels = base_parcels.to_crs(2285)

# Create points within current parcel polygons, x & y coord columns
current_prcl_pts = current_parcels.copy()
current_prcl_pts['geometry'] = current_prcl_pts.representative_point()
current_prcl_pts['x_coord'] = current_prcl_pts.geometry.x
current_prcl_pts['y_coord'] = current_prcl_pts.geometry.y

# Spatial join current parcel points to base parcel polygons
current_prcl_pts = current_prcl_pts.sjoin(base_parcels, how='left', predicate='intersects')
current_prcl_pts.drop('index_right', axis=1, inplace=True)

# Spatial join to cities and tract layers
current_prcl_pts = current_prcl_pts.sjoin(cities, how='left', predicate='intersects')
current_prcl_pts.drop('index_right', axis=1, inplace=True)

current_prcl_pts = current_prcl_pts.sjoin(tract20, how='left', predicate='intersects')
current_prcl_pts.drop('index_right', axis=1, inplace=True)

# Spatial join attributes of point gdf back to current parcel polygon gdf
kitsap_parcels = current_parcels.sjoin(current_prcl_pts, how='left', predicate='contains')
kitsap_parcels.drop(['index_right', 'RP_ACCT_ID_right'], axis=1, inplace=True)
kitsap_parcels.rename({'RP_ACCT_ID_left': 'RP_ACCT_ID'}, axis=1, inplace=True)

del current_prcl_path, base_prcl_path, current_parcels, base_parcels, current_prcl_pts

# --------------------------------------------------------------------------------------------------
# Pierce
current_prcl_path = 'J:/Projects/Assessor/extracts/2024/July_24/Pierce/Tax_Parcels_3771675036103499724/Tax_Parcels.shp'
base_prcl_path = 'J:/Projects/Assessor/assessor_permit/pierce/data/base_year/GIS/pierce_parcels_2012.shp'

current_parcels = gpd.read_file(current_prcl_path, where="TaxParcelT='Base Parcel'")
current_parcels = current_parcels[['TaxParcelN', 'geometry']]
current_parcels = current_parcels.to_crs(2285)

base_parcels = gpd.read_file(base_prcl_path)
base_parcels = base_parcels[['TaxParcelN', 'geometry']]
base_parcels.rename({'TaxParcelN': 'base_prcl'}, axis=1, inplace=True)
base_parcels = base_parcels.to_crs(2285)

# Create points within current parcel polygons, x & y coord columns
current_prcl_pts = current_parcels.copy()
current_prcl_pts['geometry'] = current_prcl_pts.representative_point()
current_prcl_pts['x_coord'] = current_prcl_pts.geometry.x
current_prcl_pts['y_coord'] = current_prcl_pts.geometry.y

# Spatial join current parcel points to base parcel polygons
current_prcl_pts = current_prcl_pts.sjoin(base_parcels, how='left', predicate='intersects')
current_prcl_pts.drop('index_right', axis=1, inplace=True)

# Spatial join to cities and tract layers
current_prcl_pts = current_prcl_pts.sjoin(cities, how='left', predicate='intersects')
current_prcl_pts.drop('index_right', axis=1, inplace=True)

current_prcl_pts = current_prcl_pts.sjoin(tract20, how='left', predicate='intersects')
current_prcl_pts.drop('index_right', axis=1, inplace=True)

# Spatial join attributes of point gdf back to current parcel polygon gdf
pierce_parcels = current_parcels.sjoin(current_prcl_pts, how='left', predicate='contains')
pierce_parcels.drop(['index_right', 'TaxParcelN_right'], axis=1, inplace=True)
pierce_parcels.rename({'TaxParcelN_left': 'TaxParcelN'}, axis=1, inplace=True)

del current_prcl_path, base_prcl_path, current_parcels, base_parcels, current_prcl_pts

# --------------------------------------------------------------------------------------------------
# Write shapefiles to project folders
kitsap_parcels.to_file('J:/Projects/Assessor/assessor_permit/kitsap/data/2024/GIS/parcels_2024_2010_region23_tract20.shp')
pierce_parcels.to_file('J:/Projects/Assessor/assessor_permit/pierce/data/2024/GIS/parcels_2024_2012_region23_tract20.shp')
