import pandas as pd
import geopandas as gpd
import sqlalchemy
from shapely import wkt
from pathlib import Path
import sys

def get_juris(config):
    """Read in cities shapefile - version from project year."""
    pth = f"{config['project_path']}{config['juris_path']}"
    
    juris = gpd.read_file(pth)
    juris = juris.to_crs(config['crs'])
    juris = juris[['feat_type', 'juris', 'geometry']]
    
    return juris

def get_tracts(config):
    """Connect to ElmerGeo and read in 2020 tract layer."""
    eg_conn = "DRIVER={ODBC Driver 17 for SQL Server}; SERVER=SQLserver; DATABASE=Elmer; trusted_connection=yes"
    tract_query = """SELECT geoid20 AS tractid, name20 AS tract20, Shape.STAsText() AS [geometry]
                     FROM ElmerGeo.dbo.tract2020_evw;
                  """
    
    engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(eg_conn))
    tracts = pd.read_sql(tract_query, engine)
    tracts['geometry'] = tracts['geometry'].apply(wkt.loads)
    tracts = gpd.GeoDataFrame(tracts, geometry='geometry', crs=config['crs'])
    
    return tracts

def process_kitsap_parcels(config, juris, tracts):
    """Combine Kitsap County current and base year parcels with city and tract data."""
    base_path = f"{config['project_path']}{config['kitsap_base_path']}"
    current_path = f"{config['extract_path']}{config['kitsap_current_path']}"
    field_list = ['RP_ACCT_ID', 'base_rid', 'feat_type', 'juris', 'tractid', 'tract20', 'x_coord', 'y_coord', 'geometry']
    
    # read in base parcel layer
    base_gdf = gpd.read_file(base_path)
    base_gdf = base_gdf.to_crs(config['crs'])
    base_gdf.rename({'RP_ACCT_ID':'base_rid'}, axis=1, inplace=True)
    base_gdf = base_gdf[['base_rid', 'geometry']]
    
    # read in current parcel layer
    current_gdf = gpd.read_file(current_path, layer='Tax_Parcel_lot_polygons')
    current_gdf = current_gdf.to_crs(config['crs'])
    
    # create points from polygons
    current_pts = current_gdf[['RP_ACCT_ID', 'geometry']].copy()
    current_pts.geometry = current_pts.representative_point()
    current_pts['x_coord'] = current_pts.geometry.x
    current_pts['y_coord'] = current_pts.geometry.y
    
    # spatial joins of desired features
    current_base_join = current_pts.sjoin(base_gdf, how='left')
    current_base_join.drop(columns='index_right', inplace=True)
    
    current_base_join = current_base_join.sjoin(juris, how='left')
    current_base_join.drop(columns='index_right', inplace=True)
    
    current_base_join = current_base_join.sjoin(tracts, how='left')
    current_base_join.drop(columns='index_right', inplace=True)
    
    # join current points back to polygons
    current_base_df = pd.DataFrame(current_base_join.drop(columns='geometry'))
    
    current_base_gdf = pd.merge(current_gdf, current_base_df,
                                left_on='RP_ACCT_ID', right_on='RP_ACCT_ID',
                                how='left')
    
    current_base_gdf = current_base_gdf[field_list]
    
    return(current_base_gdf)

def process_pierce_parcels(config, juris, tracts):
    """Combine Pierce County current and base year parcels with city and tract data."""
    base_path = f"{config['project_path']}{config['pierce_base_path']}"
    current_path = f"{config['extract_path']}{config['pierce_current_path']}"
    field_list = ['TaxParcelN', 'base_prcl', 'feat_type', 'juris', 'tractid', 'tract20', 'x_coord', 'y_coord', 'geometry']
    
    # read in base parcel layer
    base_gdf = gpd.read_file(base_path)
    base_gdf = base_gdf.to_crs(config['crs'])
    base_gdf.rename({'TaxParcelN':'base_prcl'}, axis=1, inplace=True)
    base_gdf = base_gdf[['base_prcl', 'geometry']]
    
    # read in current parcel layer
    current_gdf = gpd.read_file(current_path)
    current_gdf = current_gdf.to_crs(config['crs'])
    current_gdf = current_gdf[current_gdf['TaxParcelT'] == 'Base Parcel']
    
    # create points from polygons
    current_pts = current_gdf[['TaxParcelN', 'geometry']].copy()
    current_pts.geometry = current_pts.representative_point()
    current_pts['x_coord'] = current_pts.geometry.x
    current_pts['y_coord'] = current_pts.geometry.y
    
    # spatial joins of desired features
    current_base_join = current_pts.sjoin(base_gdf, how='left')
    current_base_join.drop(columns='index_right', inplace=True)
    
    current_base_join = current_base_join.sjoin(juris, how='left')
    current_base_join.drop(columns='index_right', inplace=True)
    
    current_base_join = current_base_join.sjoin(tracts, how='left')
    current_base_join.drop(columns='index_right', inplace=True)
    
    # join current points back to polygons
    current_base_df = pd.DataFrame(current_base_join.drop(columns='geometry'))
    
    current_base_gdf = pd.merge(current_gdf, current_base_df,
                                left_on='TaxParcelN', right_on='TaxParcelN',
                                how='left')
    
    current_base_gdf = current_base_gdf[field_list]
    
    return(current_base_gdf)

def process_assessor_parcels(county):
    """Run processing functions for either Kitsap or Pierce County."""
    if county.lower() == 'kitsap':
        parcel_output = process_kitsap_parcels()
        
    elif county.lower() == 'pierce':
        parcel_output = process_pierce_parcels()
        
    else:
        sys.exit('Unable to run. Supply a valid county name: Kitsap or Pierce')
    
    return parcel_output