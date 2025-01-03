library(tidyverse)
library(odbc)
library(DBI)
library(sf)

# Read in county tables -----------------------------------------------------------------------

input_path <- "J:/Projects/Assessor/assessor_permit/data_products/2024/elmer/"
output_path <- "J:/Projects/Assessor/assessor_permit/data_products/2024/elmer/GIS/"
project_year <- 2024

setwd(input_path)

load("king_tables.rda")
load("kitsap_tables.rda")
load("pierce_tables.rda")
load("snohomish_tables.rda")

king_parcel_tbl$geoid20 <- as.character(king_parcel_tbl$geoid20)
king_tract_units_long$tract <- as.character(king_tract_units_long$tract)
snohomish_parcel_tbl$geoid20 <- as.character(snohomish_parcel_tbl$geoid20)
snohomish_tract_units_long$tract <- as.character(snohomish_tract_units_long$tract)


# Combine parcel tables for shapefile ---------------------------------------------------------

region_parcels <- bind_rows(king_parcel_tbl, kitsap_parcel_tbl, pierce_parcel_tbl, snohomish_parcel_tbl) %>% 
  # rename column names >10 char in length for ESRI driver
  rename(proj_year = project_year,
         str_type = structure_type,
         dvlpment = development,
         juris = jurisdiction,
         cnty_fips = county_fips)

rows_p <- seq(1, NROW(region_parcels), 1)

region_parcels_sf <- region_parcels %>% 
  mutate(psrc_id = sprintf(paste0(project_year, "P", "%06d"), rows_p), .before = 1) %>% 
  st_as_sf(., coords = c("x_coord", "y_coord"), crs = 2285, remove = FALSE)

st_write(region_parcels_sf, dsn = paste0(output_path, "psrc_assessor_housing_estimates_2024.shp"),
         delete_layer = TRUE)


# Combine summary tables for Elmer import -----------------------------------------------------

# county
region_county <- bind_rows(king_county_units_long,
                           kitsap_county_units_long,
                           pierce_county_units_long,
                           snohomish_county_units_long)



# juris
region_juris <- bind_rows(king_juris_units_long,
                          kitsap_juris_units_long,
                          pierce_juris_units_long,
                          snohomish_juris_units_long)



# tract
region_tract <- bind_rows(king_tract_units_long,
                          kitsap_tract_units_long,
                          pierce_tract_units_long,
                          snohomish_tract_units_long)



