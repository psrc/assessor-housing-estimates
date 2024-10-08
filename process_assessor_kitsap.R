library(tidyverse)
library(readxl)
library(writexl)
library(odbc)
library(DBI)
library(sf)
library(psrcelmer)

# Define file paths and other variables -------------------------------------------------------

# source data file paths
current_file_path <- "J:/Projects/Assessor/assessor_permit/kitsap/data/2024/extracts/"
current_file_name <- "KITSAP_COUNTY_PARCELS_09182024.shp"

base_file_path <- "J:/Projects/Assessor/assessor_permit/kitsap/data/base_year/extracts/"
base_file_name <- "Kitsap_Housing_2011.xlsx"
base_improvements_name <- "improvements.csv"

current_base_shapefile_path <- "J:/Projects/Assessor/assessor_permit/kitsap/data/2024/GIS/"
current_base_shapefile_name <- "parcels_2024_2010_region23_tract20.shp"

juris_query <- "SELECT juris, feat_type FROM dbo.PSRC_REGION WHERE cnty_name = 'Kitsap' AND feat_type <> 'water'"

tract_query <- "SELECT geoid20 FROM dbo.TRACT2020 WHERE county_name = 'Kitsap'"

output_file_path <- "J:/Projects/Assessor/assessor_permit/kitsap/data/2024/script_outputs/"

year_start <- 2010
year_end <- 2023
proj_year <- 2024


# Load data from source -----------------------------------------------------------------------

juris <- get_query(sql = juris_query, db_name = "ElmerGeo") %>% 
  mutate(juris = ifelse(feat_type %in% c("uninc", "rural"), "Unincorporated Kitsap", juris)) %>% 
  select(-feat_type) %>% 
  distinct() %>% 
  arrange(juris)

tracts <- get_query(sql = tract_query, db_name = "ElmerGeo")

current_year <- st_read(paste0(current_file_path, current_file_name),
                        crs = 2285, stringsAsFactors = FALSE) %>% 
  st_drop_geometry() %>% 
  filter(YEAR_BUILT >= year_start & YEAR_BUILT <= year_end) %>% 
  mutate(RP_ACCT_ID = as.character(RP_ACCT_ID),
         ACCT_NO = str_remove_all(ACCT_NO, "-"))

base_year <- read_xlsx(paste0(base_file_path, base_file_name)) %>% 
  mutate(RP_ACCT_ID = as.character(RP_ACCT_ID),
         ACCT_NO = str_remove_all(ACCT_NO, "-"))

base_improvements <- read_csv(paste0(base_file_path, base_improvements_name),
                              col_types = cols(
                                RP_ACCT_ID = col_character()
                              )) %>% 
  filter(IMP_TYPE %in% c("DWELL", "MHOME", "APART", "CABIN", "ROOMHSE", "MULTRESH")) %>% 
  mutate(IMP_TYPE = ifelse(IMP_TYPE == "CABIN", "DWELL", IMP_TYPE),
         ACCT_NO = str_remove_all(ACCT_NO, "-"))

# Read in current year base parcel shapefile with base parcel PINs
# This is created using the parcel preprocessing python script
parcels_current_base <- st_read(paste0(current_base_shapefile_path, current_base_shapefile_name),
                                crs = 2285, stringsAsFactors = FALSE) %>% 
  st_drop_geometry() %>%
  filter(RP_ACCT_ID != 0) %>% 
  mutate(RP_ACCT_ID = as.character(RP_ACCT_ID),
         base_rid = as.character(base_rid)) %>% 
  mutate(base_rid = ifelse(base_rid == "0", RP_ACCT_ID, base_rid)) %>% 
  rename(current_rid = RP_ACCT_ID) %>% 
  distinct()

# Read in dwellings table from July 2024 extract and filter for townhouse designation
townhouses <- read_delim("J:/Projects/Assessor/extracts/2024/July_24/Kitsap/Dwellings.txt",
                         delim = "\t",
                         col_types = cols(
                           rp_acct_id = col_character()
                         )) %>% 
  filter(house_type == "146 Townhouse"
  ) %>% 
  group_by(rp_acct_id) %>% 
  summarize(house_type = "townhouse",
            bldgs = NROW(rp_acct_id))


# Aggregate & transform current year data -----------------------------------------------------

current_year_condos <- filter(current_year, PROP_CLASS == "141")

# n_distinct(current_year_condos$RP_ACCT_ID)  # 24
# all SF; incorporate into summary table

# summarize current year table by RP_ACCT_ID
current_year_sum <- current_year %>% 
  filter(PROP_CLASS %in% c("111", "118", "119", "121", "122", "123",
                           "131", "132", "133", "134", "135", "136",
                           "137", "138", "180", "198")) %>% 
  bind_rows(., current_year_condos) %>% 
  group_by(RP_ACCT_ID) %>% 
  summarize(ACCT_NO = first(ACCT_NO),
            units = sum(TOTAL_UNIT),
            buildings = ifelse(NUM_COMM > 0 & PROP_CLASS != "111", sum(NUM_COMM), sum(NUM_DWELL)),
            years = list(sort(unique(YEAR_BUILT))),
            year_count = n_distinct(YEAR_BUILT),
            year_built = min(YEAR_BUILT),
            prop_classes = ifelse(length(unique(PROP_CLASS)) > 1,
                                  list(sort(unique(PROP_CLASS))),
                                  unique(PROP_CLASS)),
            prop_class_descs = ifelse(length(unique(DESCRIPTIO)) > 1,
                                      list(sort(unique(DESCRIPTIO))),
                                      unique(DESCRIPTIO))
  ) %>% 
  mutate(units = case_when(prop_classes %in% c("111", "118", "119", "180", "198") & units == 0 ~ 1,
                           prop_classes == "121" ~ units * 2,
                           TRUE ~ units)
  ) %>% 
  mutate(buildings = ifelse(buildings == 0, 1, buildings))

# left join to townhouse table
current_year_sum <- left_join(current_year_sum, townhouses, by = c("RP_ACCT_ID" = "rp_acct_id")) %>% 
  select(-bldgs)

#### UNIQUE TO THIS DATA - CHECK EVERY YEAR!
current_year_sum$units[current_year_sum$RP_ACCT_ID == "2561223"] <- 35

# fix building count based on townhouse records
current_year_sum$buildings[current_year_sum$RP_ACCT_ID == "2414415"] <- 7
current_year_sum$house_type[current_year_sum$RP_ACCT_ID == "2414415"] <- NA

# delete records with 0 units (unfinished construction)
# View(filter(current_year_sum, units == 0))
current_year_sum <- current_year_sum[!(current_year_sum$RP_ACCT_ID == "1490747"), ]
current_year_sum <- current_year_sum[!(current_year_sum$RP_ACCT_ID == "2647287"), ]
current_year_sum <- current_year_sum[!(current_year_sum$RP_ACCT_ID == "2680080"), ]
current_year_sum <- current_year_sum[!(current_year_sum$RP_ACCT_ID == "2687820"), ]
current_year_sum <- current_year_sum[!(current_year_sum$RP_ACCT_ID == "2660363"), ]
current_year_sum <- current_year_sum[!(current_year_sum$RP_ACCT_ID == "2665792"), ]
current_year_sum <- current_year_sum[!(current_year_sum$RP_ACCT_ID == "2685980"), ]
current_year_sum <- current_year_sum[!(current_year_sum$RP_ACCT_ID == "2687838"), ]
####

current_year_sum$units_per_bldg <- round(current_year_sum$units / current_year_sum$buildings, 0)

# create structure type based on property classes or units per building
current_year_sum$str_type <- case_when(
  current_year_sum$prop_classes %in% c("118", "119") ~ "mobile homes",
  current_year_sum$house_type == "townhouse" ~ "single family attached",
  current_year_sum$units_per_bldg == 1 ~ "single family detached",
  current_year_sum$units_per_bldg >= 2 & current_year_sum$units_per_bldg <= 4 ~ "multifamily 2-4 units",
  current_year_sum$units_per_bldg >= 5 & current_year_sum$units_per_bldg <= 9 ~ "multifamily 5-9 units",
  current_year_sum$units_per_bldg >= 10 & current_year_sum$units_per_bldg <= 19 ~ "multifamily 10-19 units",
  current_year_sum$units_per_bldg >= 20 & current_year_sum$units_per_bldg <= 49 ~ "multifamily 20-49 units",
  current_year_sum$units_per_bldg >= 50 ~ "multifamily 50+ units"
)

current_year_sum$str_type <- ordered(current_year_sum$str_type,
                                     levels = c("single family detached",
                                                "single family attached",
                                                "multifamily 2-4 units",
                                                "multifamily 5-9 units",
                                                "multifamily 10-19 units",
                                                "multifamily 20-49 units",
                                                "multifamily 50+ units",
                                                "mobile homes"))

# join current year summary table to shapefile table to add 2020 tracts and juris
current_year_sum <- left_join(current_year_sum, parcels_current_base, by = c("RP_ACCT_ID" = "current_rid"))

#### UNIQUE TO THIS DATA - CHECK EVERY YEAR!
# parcel centroids didn't match to other polygons in ArcMap processing
# View(filter(current_year_sum, is.na(juris)))
# current_year_sum$juris[is.na(current_year_sum$juris)] <- "Unincorporated Kitsap"

# View(filter(current_year_sum, is.na(tractid)))
# View(filter(current_year_sum, is.na(tract20)))
# current_year_sum$tractid[current_year_sum$RP_ACCT_ID %in% c("2691137", "2692077", "2693067", "2693075",
#                                                             "2693406", "2693414", "2693448", "2693463")] <- "53035090102"
# current_year_sum$tract20[current_year_sum$RP_ACCT_ID %in% c("2691137", "2692077", "2693067", "2693075",
#                                                             "2693406", "2693414", "2693448", "2693463")] <- "901.02"
# 
# current_year_sum$tractid[current_year_sum$RP_ACCT_ID == "2691178"] <- "53035092701"
# current_year_sum$tract20[current_year_sum$RP_ACCT_ID == "2691178"] <- "927.01"

# View(filter(current_year_sum, is.na(x_coord)))
# View(filter(current_year_sum, is.na(y_coord)))
# current_year_sum$x_coord[current_year_sum$RP_ACCT_ID == "2691137"] <- 1227859
# current_year_sum$y_coord[current_year_sum$RP_ACCT_ID == "2691137"] <- 319380
# 
# current_year_sum$x_coord[current_year_sum$RP_ACCT_ID == "2691178"] <- 1209310
# current_year_sum$y_coord[current_year_sum$RP_ACCT_ID == "2691178"] <- 178877
# 
# current_year_sum$x_coord[current_year_sum$RP_ACCT_ID == "2692077"] <- 1228922
# current_year_sum$y_coord[current_year_sum$RP_ACCT_ID == "2692077"] <- 287443
# 
# current_year_sum$x_coord[current_year_sum$RP_ACCT_ID == "2693067"] <- 1226249
# current_year_sum$y_coord[current_year_sum$RP_ACCT_ID == "2693067"] <- 286994
# 
# current_year_sum$x_coord[current_year_sum$RP_ACCT_ID == "2693075"] <- 1226248
# current_year_sum$y_coord[current_year_sum$RP_ACCT_ID == "2693075"] <- 286944
# 
# current_year_sum$x_coord[current_year_sum$RP_ACCT_ID == "2693406"] <- 1226128
# current_year_sum$y_coord[current_year_sum$RP_ACCT_ID == "2693406"] <- 288160
# 
# current_year_sum$x_coord[current_year_sum$RP_ACCT_ID == "2693414"] <- 1226112
# current_year_sum$y_coord[current_year_sum$RP_ACCT_ID == "2693414"] <- 288113
# 
# current_year_sum$x_coord[current_year_sum$RP_ACCT_ID == "2693448"] <- 1226063
# current_year_sum$y_coord[current_year_sum$RP_ACCT_ID == "2693448"] <- 287961
# 
# current_year_sum$x_coord[current_year_sum$RP_ACCT_ID == "2693463"] <- 1226051
# current_year_sum$y_coord[current_year_sum$RP_ACCT_ID == "2693463"] <- 287861
####

rm(current_year, parcels_current_base)


# Aggregate & transform base year data --------------------------------------------------------

base_year_condos <- filter(base_year, PROP_CLASS == "141" & YEAR_BUILT < year_start)

# n_distinct(base_year_condos$RP_ACCT_ID) # 54
# all SF; incorporate into summary table

improvements_baseyear <- base_improvements %>% 
  group_by(RP_ACCT_ID) %>% 
  summarize(year_built_a = max(YEAR_BUILT))

# join assessor improvements table to base year parcel table to get accurate year_built
base_year <- bind_rows(base_year, base_year_condos) %>% 
  left_join(., improvements_baseyear, by = c("RP_ACCT_ID" = "RP_ACCT_ID"))

# summarize base year table by RP_ACCT_ID
base_year_sum <- base_year %>% 
  filter(PROP_CLASS %in% c("111", "118", "119", "121", "122", "123",
                           "131", "132", "133", "134", "135", "136",
                           "137", "138", "180", "198")
         & year_built_a < year_start) %>% 
  group_by(RP_ACCT_ID) %>% 
  summarize(ACCT_NO = first(ACCT_NO),
            base_units = sum(NUM_DWELL),
            base_buildings = sum(NUM_DWELL),
            base_years = list(sort(unique(year_built_a))),
            base_year_count = n_distinct(year_built_a),
            base_year_built = min(year_built_a),
            base_prop_classes = ifelse(length(unique(PROP_CLASS)) > 1,
                                       list(sort(unique(PROP_CLASS))),
                                       unique(PROP_CLASS)),
            base_prop_class_descs = ifelse(length(unique(DESCRIPTIO)) > 1,
                                           list(sort(unique(DESCRIPTIO))),
                                           unique(DESCRIPTIO))
  ) %>% 
  mutate(base_units = ifelse(base_prop_classes %in% c("111", "118", "119", "180", "198") & base_units != base_buildings,
                             base_buildings, base_units)) %>% 
  mutate(base_units = case_when(base_prop_classes == "121" ~ 2 * base_buildings,
                                base_prop_classes == "122" ~ 3 * base_buildings,
                                base_prop_classes == "123" ~ 4 * base_buildings,
                                base_prop_classes == "131" & base_units < 5 ~ 0,
                                TRUE ~ base_units)) %>% 
  mutate(base_buildings = ifelse(base_prop_classes == "131" & base_units > 0, 1, base_buildings))

# left join to townhouse table
base_year_sum <- left_join(base_year_sum, townhouses, by = c("RP_ACCT_ID" = "rp_acct_id")) %>% 
  select(-bldgs)

#### UNIQUE TO THIS DATA - CHECK EVERY YEAR!
base_year_sum$base_buildings[base_year_sum$RP_ACCT_ID == "2135655"] <- 20
base_year_sum$base_buildings[base_year_sum$RP_ACCT_ID == "2125912"] <- 10
base_year_sum$base_buildings[base_year_sum$RP_ACCT_ID == "1719210"] <- 14
####

base_year_sum$base_units_per_bldg <- round(base_year_sum$base_units / base_year_sum$base_buildings, 0)

# create structure type based on property classes or units per building
base_year_sum$base_str_type <- case_when(
  base_year_sum$base_prop_classes %in% c("118", "119") ~ "mobile homes",
  base_year_sum$house_type == "townhouse" ~ "single family attached",
  base_year_sum$base_units_per_bldg == 1 ~ "single family detached",
  base_year_sum$base_units_per_bldg >= 2 & base_year_sum$base_units_per_bldg <= 4 ~ "multifamily 2-4 units",
  (base_year_sum$base_units_per_bldg >= 5 & base_year_sum$base_units_per_bldg <= 9)
  | base_year_sum$base_prop_classes == "131" ~ "multifamily 5-9 units",
  (base_year_sum$base_units_per_bldg >= 10 & base_year_sum$base_units_per_bldg <= 19)
  | base_year_sum$base_prop_classes %in% c("132", "133") ~ "multifamily 10-19 units",
  (base_year_sum$base_units_per_bldg >= 20 & base_year_sum$base_units_per_bldg <= 49)
  | base_year_sum$base_prop_classes %in% c("134", "135", "136") ~ "multifamily 20-49 units",
  base_year_sum$base_units_per_bldg >= 50 | base_year_sum$base_prop_classes == "137" ~ "multifamily 50+ units"
)

base_year_sum$base_str_type <- ifelse(base_year_sum$base_units == 0, NA, base_year_sum$base_str_type)

rm(base_year, base_improvements, improvements_baseyear)


# Combine current & base year data and add new fields -----------------------------------------

current_base_join <- left_join(current_year_sum, base_year_sum, by = c("base_rid" = "RP_ACCT_ID"))

# Specify development type
base_pins <- current_base_join %>% 
  select(RP_ACCT_ID, base_rid) %>% 
  filter(!is.na(base_rid)) %>% 
  group_by(base_rid) %>% 
  summarize(base_pin_count = NROW(RP_ACCT_ID))

current_base_join <- left_join(current_base_join, base_pins, by = c("base_rid" = "base_rid"))

current_base_join <- current_base_join %>% 
  mutate(development = case_when(is.na(base_year_built) ~ "new development",
                                 year_built > base_year_built & units == base_units & base_pin_count == 1 ~ "rebuild or remodel",
                                 (year_built > base_year_built & units != base_units)
                                 | (year_built > base_year_built & base_pin_count > 1) ~ "redevelopment"))

#### UNIQUE TO THIS DATA - CHECK EVERY YEAR!
# phased townhouse development
current_base_join$development[current_base_join$RP_ACCT_ID %in% c("2453934", "2453942", "2453959", "2453967",
                                                                  "2454064", "2454072", "2454080",
                                                                  "2454098", "2454106", "2454114",
                                                                  "2454122", "2454130", "2454148")] <- "new development"
####

# Compute new units & demo units
current_base_join$new_units <- if_else(current_base_join$development %in% c("new development", "redevelopment", "rebuild or remodel"),
                                       current_base_join$units, 0)

demos <- current_base_join %>% 
  filter(development %in% c("redevelopment", "rebuild or remodel")) %>% 
  distinct(base_rid, .keep_all = TRUE) %>% 
  mutate(demo_units = base_units * -1)

current_base_join$juris <- ordered(current_base_join$juris,
                                   levels = c("Bainbridge Island",
                                              "Bremerton",
                                              "Port Orchard",
                                              "Poulsbo",
                                              "Unincorporated Kitsap"))

rm(base_pins)


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
                                 levels = c("single family detached",
                                            "single family attached",
                                            "multifamily 2-4 units",
                                            "multifamily 5-9 units",
                                            "multifamily 10-19 units",
                                            "multifamily 20-49 units",
                                            "multifamily 50+ units",
                                            "mobile homes"))) %>% 
  pivot_wider(id_cols = year_built,
              names_from = structure_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !year_built), na.rm = TRUE), .before = `single family detached`)

# Juris
format_juris <- function(x) {
  x %>%
    full_join(juris, by = c("juris" = "juris")) %>%
    replace(is.na(.), 0) %>% 
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
                                 levels = c("single family detached",
                                            "single family attached",
                                            "multifamily 2-4 units",
                                            "multifamily 5-9 units",
                                            "multifamily 10-19 units",
                                            "multifamily 20-49 units",
                                            "multifamily 50+ units",
                                            "mobile homes"))) %>% 
  pivot_wider(id_cols = c(year_built, juris),
              names_from = structure_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !year_built), na.rm = TRUE), .before = `single family detached`) %>% 
  group_by(year_built) %>% 
  group_modify(~ format_juris(.x)) %>% 
  ungroup()

# Tract
format_tracts <- function(x) {
  x %>%
    full_join(tracts, by = c("tractid" = "geoid20")) %>%
    replace(is.na(.), 0) %>% 
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
  group_by(tractid, year_built, structure_type) %>% 
  summarize(demo_unit_sum = sum(demo_units)) %>% 
  ungroup()

tract_units <- full_join(new_units_tract, demo_units_tract, by = join_by("tractid", "year_built", "structure_type")) %>% 
  replace_na(list(new_unit_sum = 0, demo_unit_sum = 0)) %>% 
  mutate(net_units = new_unit_sum + demo_unit_sum,
         structure_type = factor(structure_type,
                                 levels = c("single family detached",
                                            "single family attached",
                                            "multifamily 2-4 units",
                                            "multifamily 5-9 units",
                                            "multifamily 10-19 units",
                                            "multifamily 20-49 units",
                                            "multifamily 50+ units",
                                            "mobile homes"))) %>% 
  pivot_wider(id_cols = c(year_built, tractid),
              names_from = structure_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !year_built), na.rm = TRUE), .before = `single family detached`) %>% 
  group_by(year_built) %>% 
  group_modify(~ format_tracts(.x)) %>% 
  ungroup()

# Write to xlsx
file_name_county <- paste0("kitsap_unit_estimates_county_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_juris <- paste0("kitsap_unit_estimates_juris_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_tract <- paste0("kitsap_unit_estimates_tract20_", format(Sys.Date(), "%Y%m%d"), ".xlsx")

write_xlsx(x = county_units, path = paste0(output_file_path, file_name_county))
write_xlsx(x = split(juris_units, juris_units$year_built) %>% map(., ~ (.x %>% select(-year_built))),
           path = paste0(output_file_path, file_name_juris))
write_xlsx(x = split(tract_units, tract_units$year_built) %>% map(., ~ (.x %>% select(-year_built))),
           path = paste0(output_file_path, file_name_tract))


# Create outputs for combined region processing -----------------------------------------------
# parcel table for shapefile
parcel_new <- current_base_join %>% 
  mutate(project_year = proj_year,
         county = "Kitsap",
         county_fips = "035") %>% 
  select(project_year,
         pin = RP_ACCT_ID,
         year = year_built,
         units = new_units,
         buildings,
         structure_type = str_type,
         development,
         jurisdiction = juris,
         geoid20 = tractid,
         county,
         county_fips,
         x_coord,
         y_coord)

parcel_demo <- demos %>% 
  filter(demo_units != 0) %>% 
  mutate(project_year = proj_year,
         county = "Kitsap",
         county_fips = "035",
         development = "demolition") %>% 
  select(project_year,
         pin = RP_ACCT_ID,
         year = year_built,
         units = demo_units,
         buildings = base_buildings,
         structure_type = base_str_type,
         development,
         jurisdiction = juris,
         geoid20 = tractid,
         county,
         county_fips,
         x_coord,
         y_coord)

kitsap_parcel_tbl <- bind_rows(parcel_new, parcel_demo)

# summary tables for Elmer/Data Portal
kitsap_county_units_long <- county_units %>% 
  pivot_longer(cols = net_total:`mobile homes`,
               names_to = "structure_type",
               values_to = "net_units") %>% 
  mutate(project_year = proj_year, 
         county = "Kitsap") %>% 
  select(project_year, county, year = year_built, structure_type, net_units)

kitsap_juris_units_long <- juris_units %>% 
  pivot_longer(cols = net_total:`mobile homes`,
               names_to = "structure_type",
               values_to = "net_units") %>% 
  mutate(project_year = proj_year, 
         county = "Kitsap") %>% 
  select(project_year, county, juris, year = year_built, structure_type, net_units)

kitsap_tract_units_long <- tract_units %>% 
  pivot_longer(cols = net_total:`mobile homes`,
               names_to = "structure_type",
               values_to = "net_units") %>% 
  mutate(project_year = proj_year, 
         county = "Kitsap") %>% 
  select(project_year, county, tract = tractid, year = year_built, structure_type, net_units)

# save tables to .rda for combining script
save(kitsap_parcel_tbl, kitsap_county_units_long, kitsap_juris_units_long, kitsap_tract_units_long,
     file = "J:/Projects/Assessor/assessor_permit/data_products/2024/elmer/kitsap_tables.rda")
