# Pierce processing script
library(tidyverse)
library(writexl)
library(odbc)
library(DBI)
library(sf)

# Define file paths and other variables -------------------------------------------------------

# ElmerGeo db connection
geo_conn <- dbConnect(odbc::odbc(),
                      driver = "ODBC Driver 17 for SQL Server",
                      server = "AWS-PROD-SQL\\Sockeye",
                      database = "ElmerGeo",
                      trusted_connection = "yes"
)

# source data file paths
current_file_path <- "J:/Projects/Assessor/assessor_permit/pierce/data/research_phase/extracts/current_year/"
current_appraisal_file_name <- "appraisal_account_2020.csv"
current_improvement_file_name <- "improvement_2020.csv"
current_builtas_file_name <- "improvement_builtas_2020.csv"

base_file_path <- "J:/Projects/Assessor/assessor_permit/pierce/data/research_phase/extracts/base_year/"
base_appraisal_file_name <- "appraisal_account_2012.csv"
base_improvement_file_name <- "improvement_2012.csv"
base_builtas_file_name <- "improvement_builtas_2012.csv"

shapefile_path <- "J:/Projects/Assessor/assessor_permit/pierce/data/research_phase/GIS/"

output_file_path <- "J:/Projects/Assessor/assessor_permit/pierce/data/research_phase/script_outputs/"

year_start <- 2012
year_end <- 2019


# Load data from source -----------------------------------------------------------------------

juris <- dbGetQuery(geo_conn,
                    "SELECT juris, feat_type FROM ElmerGeo.dbo.PSRC_REGION
                     WHERE cnty_name = 'Pierce' AND feat_type <> 'water'") %>% 
  mutate(juris = ifelse(feat_type %in% c("uninc", "rural"), "Unincorporated Pierce", juris)) %>% 
  select(-feat_type) %>% 
  distinct() %>% 
  arrange()

tracts <- dbGetQuery(geo_conn,
                     "SELECT geoid10 FROM ElmerGeo.dbo.TRACT2010 WHERE county_name = 'Pierce'")

dbDisconnect(geo_conn)
rm(geo_conn)

current_improvement <- read_csv(paste0(current_file_path, current_improvement_file_name),
                                col_types = cols(
                                  parcel_number = col_character()
                                )) %>% 
  filter(!(property_type %in% c("Industrial", "Out Building")))

current_builtas <- read_csv(paste0(current_file_path, current_builtas_file_name),
                            col_types = cols(
                              parcel_number = col_character()
                            )) %>% 
  filter(year_built >= 2012 & year_built <= 2019
         & built_as_id %in% c(1, 4, 5, 7, 8, 9, 10, 11, 12, 13,
                              14, 15, 16, 17, 18, 21, 25, 51, 55, 57,
                              58, 61, 65, 67, 68, 71, 75, 77, 78,
                              300, 352, 1300, 1459))

current_appraisal <- read_csv(paste0(current_file_path, current_appraisal_file_name),
                              col_types = cols(
                                parcel_number = col_character()
                              )) %>% 
  filter(!is.na(latitude))

base_improvement <- read_csv(paste0(base_file_path, base_improvement_file_name),
                             col_types = cols(
                               parcel_number = col_character()
                             )) %>% 
  filter(!(property_type %in% c("Industrial", "Out Building")))

base_builtas <- read_csv(paste0(base_file_path, base_builtas_file_name),
                         col_types = cols(
                           parcel_number = col_character()
                         )) %>% 
  filter(year_built < 2012
         & built_as_id %in% c(1, 4, 5, 7, 8, 9, 10, 11, 12, 13,
                              14, 15, 16, 17, 18, 21, 25, 51, 55, 57,
                              58, 61, 65, 67, 68, 71, 75, 77, 78,
                              300, 352, 1300, 1459))

base_appraisal <- read_csv(paste0(base_file_path, base_appraisal_file_name),
                           col_types = cols(
                             parcel_number = col_character()
                           ))

# Read in current year base parcel shapefile with 2012 PINs
# This is created in ArcMap prior to R processing, using psrc_region layer
parcels_2020_2012 <- st_read(paste0(shapefile_path, "pierce_parcels_2020_2012_region19_tract10.shp"),
                             crs = 2285, stringsAsFactors = FALSE) %>% 
  rename(current_prcl = TaxParcelN)

# Read in base year condo parcel shapefile with base parcel PINs
# This is created in ArcMap prior to R processing
condo_parcels_2012 <- st_read(paste0(shapefile_path, "pierce_condos_2012.shp"),
                              crs = 2285, stringsAsFactors = FALSE)


# Aggregate & transform current year data -----------------------------------------------------

current_year <- left_join(current_builtas, current_improvement,
                          by = join_by(parcel_number, building_id)) %>% 
  left_join(., current_appraisal, by = join_by(parcel_number)) %>% 
  filter(appraisal_account_type != "Condominium" | (appraisal_account_type == "Condominium" & units != 1))

current_year_condos <- left_join(current_builtas, current_improvement,
                                 by = join_by(parcel_number, building_id)) %>% 
  left_join(., current_appraisal, by = join_by(parcel_number)) %>% 
  filter(appraisal_account_type == "Condominium" & units == 1)

# Fix 0 unit counts based on built_as_id
current_year$units <- if_else(current_year$built_as_id %in% c(1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 21, 25, 68)
                              & current_year$units == 0, 1, current_year$units)

current_year$units[current_year$built_as_id == 58 & current_year$units %in% c(0, 1)] <- 2

current_year$units[current_year$built_as_id == 78 & current_year$units == 0] <- 3

#### UNIQUE TO THIS DATA - CHECK EVERY YEAR!
# Delete rows from current table with non-unit buildings (i.e. apartment offices)
current_year <- current_year[!(current_year$parcel_number == "220132086" & current_year$building_id == 8), ]
current_year$buildings[current_year$parcel_number == "220132086"] <- 7

current_year <- current_year[!(current_year$parcel_number == "220142041" & current_year$units == 0), ]
current_year <- current_year[!(current_year$parcel_number == "8950003316" & current_year$units == 0), ]

current_year <- current_year[!(current_year$parcel_number == "2078140051"), ]

# Delete rows from current table with 0 units (new construction)
current_year <- current_year[!(current_year$parcel_number == "420346013"), ]
current_year <- current_year[!(current_year$parcel_number == "420346014"), ]
current_year <- current_year[!(current_year$parcel_number == "219123117"), ]
####

# Fix null unit counts
current_year$units[is.na(current_year$units)] <- 1

# Assign structure type based on built_as_id
current_year$str_type <- case_when(current_year$built_as_id %in% c(14, 15, 16, 21) ~ "MH",
                                   current_year$built_as_id %in% c(1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 25) ~ "SFD",
                                   current_year$built_as_id %in% c(61, 65, 68) ~ "SFA")

# Create sf tables of current tables
current_year_sf <- st_as_sf(current_year,
                              coords = c("longitude", "latitude"),
                              crs = 4326)
current_year_sf <- st_transform(current_year_sf, 2285)

current_year_condos_sf <- st_as_sf(current_year_condos,
                             coords = c("longitude", "latitude"),
                             crs = 4326)
current_year_condos_sf <- st_transform(current_year_condos_sf, 2285)

# Join current tables to parcel shapefile via spatial join
current_year_sf <- st_join(current_year_sf, left = TRUE, parcels_2020_2012)

current_year_condos_sf <- st_join(current_year_condos_sf, left = TRUE, parcels_2020_2012)

# Pull data tables from sf
current_prcl_join <- st_drop_geometry(current_year_sf)

current_condos_prcl_join <- st_drop_geometry(current_year_condos_sf)

# Summarize current table by current base PIN
current_year_sum <- current_prcl_join %>% 
  group_by(current_prcl) %>% 
  summarize(parcel_number = first(parcel_number),
            unique_unit_count = n_distinct(units),
            units = sum(units),
            buildings = NROW(building_id),
            years = list(sort(unique(year_built))),
            year_count = n_distinct(year_built),
            year_built = min(year_built),
            year_remodeled = min(year_remodeled),
            adjusted_year_built = min(adjusted_year_built),
            built_as_ids = list(sort(unique(built_as_id))),
            built_as_descriptions = list(sort(unique(built_as_description))),
            str_type = case_when(!is.na(str_type) ~ list(sort(unique(str_type)))),
            group_acct_number = first(group_acct_number),
            appraisal_account_type = first(appraisal_account_type),
            base_prcl = first(base_prcl),
            juris = first(juris),
            tractid = first(tractid),
            tract10 = first(tract10)
  ) %>% 
  distinct(current_prcl, .keep_all = TRUE)

# Summarize current condos table by current base PIN
# This step constructs condo buildings from individual records
current_year_condos_sum <- current_condos_prcl_join %>% 
  group_by(current_prcl) %>% 
  summarize(parcel_number = first(parcel_number),
            unique_unit_count = n_distinct(units),
            units = sum(units),
            buildings = first(buildings),
            years = list(sort(unique(year_built))),
            year_count = n_distinct(year_built),
            year_built = min(year_built),
            year_remodeled = min(year_remodeled),
            adjusted_year_built = min(adjusted_year_built),
            built_as_ids = list(sort(unique(built_as_id))),
            built_as_descriptions = list(sort(unique(built_as_description))),
            group_acct_number = first(group_acct_number),
            appraisal_account_type = first(appraisal_account_type),
            base_prcl = first(base_prcl),
            juris = first(juris),
            tractid = first(tractid),
            tract10 = first(tract10)
  )

# Combine summarized tables
current_year_join <- rbind(current_year_sum, current_year_condos_sum)

# Add units per building column
current_year_join$units_per_bldg <- round(current_year_join$units / current_year_join$buildings, digits = 0)

# Determine current year str_type by units_per_bldg
current_year_join$str_type <- ifelse(current_year_join$str_type == "NULL"
                                     & current_year_join$units_per_bldg >= 2 & current_year_join$units_per_bldg <= 4,
                                     "MF2-4", current_year_join$str_type)
current_year_join$str_type <- ifelse(current_year_join$str_type == "NULL"
                                     & current_year_join$units_per_bldg >= 5 & current_year_join$units_per_bldg <= 9,
                                     "MF5-9", current_year_join$str_type)
current_year_join$str_type <- ifelse(current_year_join$str_type == "NULL"
                                     & current_year_join$units_per_bldg >= 10 & current_year_join$units_per_bldg <= 19,
                                     "MF10-19", current_year_join$str_type)
current_year_join$str_type <- ifelse(current_year_join$str_type == "NULL"
                                     & current_year_join$units_per_bldg >= 20 & current_year_join$units_per_bldg <= 49,
                                     "MF20-49", current_year_join$str_type)
current_year_join$str_type <- ifelse(current_year_join$str_type == "NULL"
                                     & current_year_join$units_per_bldg >= 50,
                                     "MF50+", current_year_join$str_type)


current_year_join$str_type <- ordered(current_year_join$str_type,
                                      levels = c("SFA",
                                                 "SFD",
                                                 "MF2-4",
                                                 "MF5-9",
                                                 "MF10-19",
                                                 "MF20-49",
                                                 "MF50+",
                                                 "MH"))

rm(current_appraisal, current_builtas, current_improvement, current_year, current_year_condos,
   current_year_sf, current_year_condos_sf, current_prcl_join, current_condos_prcl_join,
   current_year_sum, current_year_condos_sum, parcels_2020_2012)


# Aggregate & transform base year data --------------------------------------------------------

base_year <- left_join(base_builtas, base_improvement,
                       by = join_by(parcel_number, building_id)) %>% 
  left_join(., base_appraisal, by = join_by(parcel_number)) %>% 
  filter(!(appraisal_account_type %in% c("Com Condo", "Res Com Condo", "Industrial"))
         | (appraisal_account_type %in% c("Com Condo", "Res Com Condo") & units > 1)
         | (appraisal_account_type == "Commercial" & units > 0))

base_year_condos <- left_join(base_builtas, base_improvement,
                              by = join_by(parcel_number, building_id)) %>% 
  left_join(., base_appraisal_condos, by = join_by(parcel_number)) %>% 
  filter(appraisal_account_type %in% c("Com Condo", "Res Com Condo")
         & units %in% c(0, 1))

# Fix 0 unit counts based on built_as_id
base_year$units <- if_else(base_year$built_as_id %in% c(1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 21, 25, 61, 68)
                                & base_year$units == 0, 1, base_year$units)

#### UNIQUE TO THIS DATA - CHECK EVERY YEAR!
# Delete rows from base table with non-unit buildings (i.e. apartment offices)
base_year <- base_year[!(base_year$parcel_number == "220224060" & base_year$building_id == 13), ]
base_year$buildings[base_year$parcel_number == "220224060"] <- 11

base_year <- base_year[!(base_year$parcel_number == "319052154" & base_year$building_id == 17), ]
base_year$buildings[base_year$parcel_number == "319052154"] <- 28

base_year <- base_year[!(base_year$parcel_number == "320322118" & base_year$building_id == 11), ]
base_year$buildings[base_year$parcel_number == "320322118"] <- 10

base_year <- base_year[!(base_year$parcel_number == "9705000010" & base_year$building_id == 3), ]
####

base_year <- base_year[!(base_year$built_as_id %in% c(352, 1459) & base_year$units == 0), ]

base_year_condos$units[base_year_condos$units == 0] <- 1

# Assign structure type based on built_as_id
base_year$base_str_type <- case_when(base_year$built_as_id %in% c(14, 15, 16, 21) ~ "MH",
                                     base_year$built_as_id %in% c(1, 4, 5, 7, 8, 9, 10, 11, 12, 13, 25) ~ "SFD",
                                     base_year$built_as_id %in% c(61, 65, 68) ~ "SFA")

# Pull data table from sf
condo_parcels_2012 <- st_drop_geometry(condo_parcels_2012) %>% 
  distinct(., condo_prcl, .keep_all = TRUE)

# Summarize base year table by PIN
base_year_sum <- base_year %>% 
  group_by(parcel_number) %>% 
  summarize(base_unique_unit_count = n_distinct(units),
            base_units = sum(units),
            base_buildings = NROW(building_id),
            base_years = list(sort(unique(year_built))),
            base_year_count = n_distinct(year_built),
            base_year_built = max(year_built),
            base_year_remodeled = max(year_remodeled),
            base_adjusted_year_built = max(adjusted_year_built),
            base_built_as_ids = list(sort(unique(built_as_id))),
            base_built_as_descriptions = list(sort(unique(built_as_description))),
            base_str_type = case_when(!is.na(base_str_type) ~ list(sort(unique(base_str_type))))
  ) %>% 
  distinct(parcel_number, .keep_all = TRUE)

# Join base condo records to base condo parcels on condo PIN
base_condos_join <- left_join(base_year_condos, condo_parcels_2012, by = c("parcel_number" = "condo_prcl"))

# Summarize base year condos table by PIN
# This step constructs condo buildings from individual records
base_year_condos_sum <- base_condos_join %>% 
  group_by(base_prcl) %>% 
  summarize(base_unique_unit_count = n_distinct(units),
            base_units = sum(units),
            base_buildings = NROW(building_id),
            base_years = list(sort(unique(year_built))),
            base_year_count = n_distinct(year_built),
            base_year_built = max(year_built),
            base_year_remodeled = max(year_remodeled),
            base_adjusted_year_built = max(adjusted_year_built),
            base_built_as_ids = list(sort(unique(built_as_id))),
            base_built_as_descriptions = list(sort(unique(built_as_description)))
  ) %>% 
  rename(parcel_number = base_prcl)

# Combine summarized tables
base_year_join <- rbind(base_year_sum, base_year_condos_sum)
base_year_join <- base_year_join[!is.na(base_year_join$parcel_number), ]

# Add units per building column
base_year_join$base_units_per_bldg <- round(base_year_join$base_units / base_year_join$base_buildings, digits = 0)

# Determine base year str_type by units_per_bldg
base_year_join$base_str_type <- ifelse(base_year_join$base_str_type == "NULL"
                                       & base_year_join$base_units_per_bldg >= 2 & base_year_join$base_units_per_bldg <= 4,
                                       "MF2-4", base_year_join$base_str_type)
base_year_join$base_str_type <- ifelse(base_year_join$base_str_type == "NULL"
                                       & base_year_join$base_units_per_bldg >= 5 & base_year_join$base_units_per_bldg <= 9,
                                       "MF5-9", base_year_join$base_str_type)
base_year_join$base_str_type <- ifelse(base_year_join$base_str_type == "NULL"
                                       & base_year_join$base_units_per_bldg >= 10 & base_year_join$base_units_per_bldg <= 19,
                                       "MF10-19", base_year_join$base_str_type)
base_year_join$base_str_type <- ifelse(base_year_join$base_str_type == "NULL"
                                       & base_year_join$base_units_per_bldg >= 20 & base_year_join$base_units_per_bldg <= 49,
                                       "MF20-49", base_year_join$base_str_type)
base_year_join$base_str_type <- ifelse(base_year_join$base_str_type == "NULL"
                                       & base_year_join$base_units_per_bldg >= 50,
                                       "MF50+", base_year_join$base_str_type)

base_year_join$base_str_type <- ordered(base_year_join$base_str_type,
                                        levels = c("SFA",
                                                   "SFD",
                                                   "MF2-4",
                                                   "MF5-9",
                                                   "MF10-19",
                                                   "MF20-49",
                                                   "MF50+",
                                                   "MH"))

rm(base_appraisal, base_builtas, base_improvement, base_year, base_year_condos,
   base_year_sum, base_condos_join, base_year_condos_sum, condo_parcels_2012)


# Combine current & base year data and add new fields -----------------------------------------

# Join current year to base year on PIN (run inner join first to test)
# current_base_test <- inner_join(current_year_join, base_year_join,
#                                 by = c("base_prcl" = "parcel_number"))

current_base_join <- left_join(current_year_join, base_year_join,
                               by = c("base_prcl" = "parcel_number"))

current_base_join <- current_base_join[!is.na(current_base_join$current_prcl), ]

#### UNIQUE TO THIS DATA - CHECK EVERY YEAR!
current_base_join$str_type[current_base_join$current_prcl %in% c("0022272011", "0416104046", "0417084029", "0417173702")] <- "SFD"
current_base_join$str_type[current_base_join$current_prcl %in% c("4002890023", "4002890026")] <- "SFA"
current_base_join$str_type[current_base_join$current_prcl == "7108000290"] <- "MH"
current_base_join$base_str_type[current_base_join$current_prcl == "4005000254"] <- "SFD"
####

# Specify development type and demolition
base_pins <- current_base_join %>% 
  select(current_prcl, base_prcl) %>% 
  filter(!is.na(base_prcl)) %>% 
  group_by(base_prcl) %>% 
  summarize(base_pin_count = NROW(current_prcl))

current_base_join <- left_join(current_base_join, base_pins, by = c("base_prcl" = "base_prcl"))

current_base_join <- current_base_join %>% 
  mutate(development = case_when(is.na(base_year_built) ~ "new",
                                 year_built > base_year_built & units == base_units & base_pin_count == 1 ~ "rebuild",
                                 (year_built > base_year_built & units != base_units)
                                 | (year_built > base_year_built & base_pin_count > 1) ~ "redevelopment")
  )

# Compute new units, demo units
current_base_join$new_units <- if_else(current_base_join$development %in% c("new", "redevelopment"),
                                       current_base_join$units, 0)

#### UNIQUE TO THIS DATA!
current_base_join$development[current_base_join$current_prcl == "6025250981"] <- "new"
####

demos <- current_base_join %>% 
  filter(development == "redevelopment") %>% 
  distinct(base_prcl, .keep_all = TRUE) %>% 
  mutate(demo_units = base_units * -1)

rm(current_year_join, base_year_join, base_pins)


# Create net unit output tables ---------------------------------------------------------------

# County
new_units_county <- current_base_join %>% 
  rename(structure_type = str_type) %>% 
  group_by(year_built, structure_type) %>% 
  summarize(new_unit_sum = sum(new_units)) %>% 
  ungroup()

demo_units_county <- demos %>% 
  filter(demo_units != 0) %>% 
  rename(structure_type = base_str_type) %>% 
  group_by(year_built, structure_type) %>% 
  summarize(demo_unit_sum = sum(demo_units)) %>% 
  ungroup()

county_units <- full_join(new_units_county, demo_units_county, by = join_by("year_built", "structure_type")) %>% 
  replace_na(list(new_unit_sum = 0, demo_unit_sum = 0)) %>% 
  mutate(net_units = new_unit_sum + demo_unit_sum,
         structure_type = factor(structure_type,
                                 levels = c("SFA", "SFD", "MF2-4", "MF5-9", "MF10-19", "MF20-49", "MF50+", "MH"))) %>% 
  pivot_wider(id_cols = year_built,
              names_from = structure_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !year_built), na.rm = TRUE), .before = SFA)

# Juris
format_juris <- function(x) {
  x %>%
    full_join(juris, by = c("juris" = "juris")) %>%
    replace(is.na(.), 0) %>% 
    select(-year_built) %>% 
    arrange(juris)
}

new_units_juris <- current_base_join %>% 
  rename(structure_type = str_type) %>% 
  group_by(year_built, juris, structure_type) %>% 
  summarize(new_unit_sum = sum(new_units)) %>% 
  ungroup()

demo_units_juris <- demos %>% 
  filter(demo_units != 0) %>% 
  rename(structure_type = base_str_type) %>% 
  group_by(year_built, juris, structure_type) %>% 
  summarize(demo_unit_sum = sum(demo_units)) %>% 
  ungroup()

juris_units <- full_join(new_units_juris, demo_units_juris, by = join_by("juris", "year_built", "structure_type")) %>% 
  replace_na(list(new_unit_sum = 0, demo_unit_sum = 0)) %>% 
  mutate(net_units = new_unit_sum + demo_unit_sum,
         structure_type = factor(structure_type,
                                 levels = c("SFA", "SFD", "MF2-4", "MF5-9", "MF10-19", "MF20-49", "MF50+", "MH"))) %>% 
  pivot_wider(id_cols = c(year_built, juris),
              names_from = structure_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !year_built), na.rm = TRUE), .before = SFA) %>% 
  split(., .$year_built) %>% 
  lapply(format_juris)

# Tract
format_tracts <- function(x) {
  x %>%
    full_join(tracts, by = c("tractid" = "geoid10")) %>%
    replace(is.na(.), 0) %>% 
    select(-year_built) %>% 
    arrange(tractid)
}

new_units_tract <- current_base_join %>% 
  rename(structure_type = str_type) %>% 
  group_by(year_built, tractid, structure_type) %>% 
  summarize(new_unit_sum = sum(new_units)) %>% 
  ungroup()

demo_units_tract <- demos %>% 
  filter(demo_units != 0) %>% 
  rename(structure_type = base_str_type) %>% 
  group_by(year_built, tractid, structure_type) %>% 
  summarize(demo_unit_sum = sum(demo_units)) %>% 
  ungroup()

tract_units <- full_join(new_units_tract, demo_units_tract, by = join_by("tractid", "year_built", "structure_type")) %>% 
  replace_na(list(new_unit_sum = 0, demo_unit_sum = 0)) %>% 
  mutate(net_units = new_unit_sum + demo_unit_sum,
         structure_type = factor(structure_type,
                                 levels = c("SFA", "SFD", "MF2-4", "MF5-9", "MF10-19", "MF20-49", "MF50+", "MH"))) %>% 
  pivot_wider(id_cols = c(year_built, tractid),
              names_from = structure_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !year_built), na.rm = TRUE), .before = SFA) %>% 
  split(., .$year_built) %>% 
  lapply(format_tracts)

# Write to xlsx
file_name_county <- paste0("pierce_unit_estimates_county_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_juris <- paste0("pierce_unit_estimates_juris_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_tract <- paste0("pierce_unit_estimates_tract10_", format(Sys.Date(), "%Y%m%d"), ".xlsx")

write_xlsx(x = county_units, path = paste0(output_file_path, file_name_county))
write_xlsx(x = juris_units, path = paste0(output_file_path, file_name_juris))
write_xlsx(x = tract_units, path = paste0(output_file_path, file_name_tract))