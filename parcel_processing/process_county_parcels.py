import geopandas as gpd
import yaml
import os

os.chdir('C:/Users/GGibson/GitHub/PSRC/assessor-housing-estimates/parcel_processing')
import parcel_utils as pu

config = yaml.safe_load(open('parcel_config.yaml'))
ko_path = f"{config['project_path']}{config['kitsap_output_path']}"
po_path = f"{config['project_path']}{config['pierce_output_path']}"

juris = pu.get_juris(config)
tracts = pu.get_tracts(config)
kitsap_output = pu.process_kitsap_parcels(config, juris, tracts)
pierce_output = pu.process_pierce_parcels(config, juris, tracts)

# Write shapefiles to project folders
kitsap_output.to_file(ko_path)
pierce_output.to_file(po_path)
