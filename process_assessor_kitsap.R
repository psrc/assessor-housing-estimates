library(tidyverse)
library(readxl)
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
current_file_path <- "J:/Projects/Assessor/assessor_permit/kitsap/data/research_phase/extracts/current_year/"
current_file_name <- "KITSAP_PARCEL_DWELLING_DETAILS_03292022.xlsx"

base_file_path <- "J:/Projects/Assessor/assessor_permit/kitsap/data/research_phase/extracts/base_year/"
base_file_name <- "Kitsap_Housing_2011.xlsx"
base_improvements_name <- "improvements.csv"

parcels_current_base_path <- "J:/Projects/Assessor/assessor_permit/kitsap/data/research_phase/script_inputs/"
parcels_current_base_name <- "kitsap_parcels_current_base.csv"

output_file_path <- "J:/Projects/Assessor/assessor_permit/kitsap/data/research_phase/script_outputs/"

year_start <- 2010
year_end <- 2019


# Load data from source -----------------------------------------------------------------------

juris <- dbGetQuery(geo_conn,
                    "SELECT juris, feat_type FROM ElmerGeo.dbo.PSRC_REGION
                     WHERE cnty_name = 'Kitsap' AND feat_type <> 'water'") %>% 
  mutate(juris = ifelse(feat_type %in% c("uninc", "rural"), "Unincorporated Kitsap", juris)) %>% 
  select(-feat_type) %>% 
  distinct() %>% 
  arrange()

tracts <- dbGetQuery(geo_conn,
                     "SELECT geoid10 FROM ElmerGeo.dbo.TRACT2010 WHERE county_name = 'Kitsap'")

dbDisconnect(geo_conn)
rm(geo_conn)

current_year <- read_xlsx(paste0(current_file_path, current_file_name)) %>% 
  mutate(RP_ACCT_ID = as.character(RP_ACCT_ID),
         ACCT_NO = str_remove_all(ACCT_NO, "-"))

parcels_current_base <- read_csv(paste0(parcels_current_base_path, parcels_current_base_name),
                                 col_types = cols(
                                   rp_acct_id = col_character(),
                                   base_rid = col_character()
                                 )) %>%
  filter(!(rp_acct_id == "0")) %>% 
  mutate(base_rid = ifelse(base_rid == "0", rp_acct_id, base_rid)) %>% 
  distinct()

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

# replace below with ElmerGeo-R process?
parcels_tract <- st_read("H:/Projects/Assessor/Kitsap/GIS/kitsap_parcels_tract10_juris.shp",
                         crs = 2285, stringsAsFactors = FALSE) %>% 
  st_drop_geometry() %>% 
  filter(!(rp_acct_id == 0)) %>% 
  group_by(rp_acct_id) %>% 
  summarize(tract10 = first(tract10),
            tractid = first(tractid),
            juris = first(juris)) %>% 
  mutate(rp_acct_id = as.character(rp_acct_id))


# Aggregate & transform current year data -----------------------------------------------------

current_year_condos <- filter(current_year,
                              PROP_CLASS == "141" & YEAR_BUILT >= year_start & YEAR_BUILT <= year_end)

# summarize current year table by RP_ACCT_ID
current_year_sum <- current_year %>% 
  filter(PROP_CLASS %in% c("111", "118", "119", "121", "122", "123",
                           "131", "132", "133", "134", "135", "136",
                           "137", "138", "180", "198")
         & YEAR_BUILT >= year_start & YEAR_BUILT <= year_end) %>% 
  group_by(RP_ACCT_ID) %>% 
  summarize(ACCT_NO = first(ACCT_NO),
            units = sum(NUM_DWELL),
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

#### UNIQUE TO THIS DATA - CHECK EVERY YEAR!
current_year_sum$units[current_year_sum$RP_ACCT_ID == "2561223"] <- 35
####

current_year_sum$units_per_bldg <- round(current_year_sum$units / current_year_sum$buildings, 0)

# create structure type based on property classes or units per building
current_year_sum$str_type <- case_when(
  current_year_sum$prop_classes %in% c("118", "119") ~ "MH",
  current_year_sum$units_per_bldg == 1 ~ "SFD",
  current_year_sum$units_per_bldg >= 2 & current_year_sum$units_per_bldg <= 4 ~ "MF2-4",
  current_year_sum$units_per_bldg >= 5 & current_year_sum$units_per_bldg <= 9 ~ "MF5-9",
  current_year_sum$units_per_bldg >= 10 & current_year_sum$units_per_bldg <= 19 ~ "MF10-19",
  current_year_sum$units_per_bldg >= 20 & current_year_sum$units_per_bldg <= 49 ~ "MF20-49",
  current_year_sum$units_per_bldg >= 50 ~ "MF50+"
)

current_year_sum$str_type <- ordered(current_year_sum$str_type,
                                     levels = c("SFD",
                                                "MF2-4",
                                                "MF5-9",
                                                "MF10-19",
                                                "MF20-49",
                                                "MF50+",
                                                "MH"))

# join current year summary table to shapefile table to add 2010 tracts and juris
current_year_sum <- left_join(current_year_sum, parcels_tract, by = c("RP_ACCT_ID" = "rp_acct_id"))

current_year_sum <- left_join(current_year_sum, parcels_current_base, by = c("RP_ACCT_ID" = "rp_acct_id"))

rm(current_year, parcels_current_base, parcels_tract)


# Aggregate & transform base year data --------------------------------------------------------

base_year_condos <- filter(base_year, PROP_CLASS == "141" & YEAR_BUILT < year_start)

improvements_baseyear <- base_improvements %>% 
  group_by(RP_ACCT_ID) %>% 
  summarize(year_built_a = max(YEAR_BUILT))

# join assessor improvements table to base year parcel table to get accurate year_built
base_year <- left_join(base_year, improvements_baseyear, by = c("RP_ACCT_ID" = "RP_ACCT_ID"))

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

#### UNIQUE TO THIS DATA - CHECK EVERY YEAR!
base_year_sum$base_buildings[base_year_sum$RP_ACCT_ID == "2135655"] <- 20
base_year_sum$base_buildings[base_year_sum$RP_ACCT_ID == "2125912"] <- 10
base_year_sum$base_buildings[base_year_sum$RP_ACCT_ID == "1719210"] <- 14
####

base_year_sum$base_units_per_bldg <- round(base_year_sum$base_units / base_year_sum$base_buildings, 0)

# create structure type based on property classes or units per building
base_year_sum$base_str_type <- case_when(
  base_year_sum$base_prop_classes %in% c("118", "119") ~ "MH",
  base_year_sum$base_units_per_bldg == 1 ~ "SFD",
  base_year_sum$base_units_per_bldg >= 2 & base_year_sum$base_units_per_bldg <= 4 ~ "MF2-4",
  (base_year_sum$base_units_per_bldg >= 5 & base_year_sum$base_units_per_bldg <= 9)
  | base_year_sum$base_prop_classes == "131" ~ "MF5-9",
  (base_year_sum$base_units_per_bldg >= 10 & base_year_sum$base_units_per_bldg <= 19)
  | base_year_sum$base_prop_classes %in% c("132", "133") ~ "MF10-19",
  (base_year_sum$base_units_per_bldg >= 20 & base_year_sum$base_units_per_bldg <= 49)
  | base_year_sum$base_prop_classes %in% c("134", "135", "136") ~ "MF20-49",
  base_year_sum$base_units_per_bldg >= 50 | base_year_sum$base_prop_classes == "137" ~ "MF50+"
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
  mutate(development = case_when(is.na(base_year_built) ~ "new",
                                 year_built > base_year_built & units == base_units & base_pin_count == 1 ~ "rebuild",
                                 (year_built > base_year_built & units != base_units)
                                 | (year_built > base_year_built & base_pin_count > 1) ~ "redevelopment"))

# Compute new units & demo units
current_base_join$new_units <- if_else(current_base_join$development %in% c("new", "redevelopment"),
                                       current_base_join$units, 0)

demos <- current_base_join %>% 
  filter(development == "redevelopment") %>% 
  distinct(base_rid, .keep_all = TRUE) %>% 
  mutate(demo_units = base_units * -1)

current_base_join$juris <- ifelse(is.na(current_base_join$juris), "Unknown", current_base_join$juris)
current_base_join$tractid <- ifelse(is.na(current_base_join$tractid), "Unknown", current_base_join$tractid)
current_base_join$tract10 <- ifelse(is.na(current_base_join$tract10), "Unknown", current_base_join$tract10)

current_base_join$juris <- ordered(current_base_join$juris,
                                   levels = c("Bainbridge Island",
                                              "Bremerton",
                                              "Port Orchard",
                                              "Poulsbo",
                                              "Unincorporated Kitsap",
                                              "Unknown"))

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
                                 levels = c("SFD", "MF2-4", "MF5-9", "MF10-19", "MF20-49", "MF50+", "MH"))) %>% 
  pivot_wider(id_cols = year_built,
              names_from = structure_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !year_built), na.rm = TRUE), .before = SFD)

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
                                 levels = c("SFD", "MF2-4", "MF5-9", "MF10-19", "MF20-49", "MF50+", "MH"))) %>% 
  pivot_wider(id_cols = c(year_built, juris),
              names_from = structure_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !year_built), na.rm = TRUE), .before = SFD) %>% 
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
  group_by(tractid, year_built, structure_type) %>% 
  summarize(demo_unit_sum = sum(demo_units)) %>% 
  ungroup()

tract_units <- full_join(new_units_tract, demo_units_tract, by = join_by("tractid", "year_built", "structure_type")) %>% 
  replace_na(list(new_unit_sum = 0, demo_unit_sum = 0)) %>% 
  mutate(net_units = new_unit_sum + demo_unit_sum,
         structure_type = factor(structure_type,
                                 levels = c("SFD", "MF2-4", "MF5-9", "MF10-19", "MF20-49", "MF50+", "MH"))) %>% 
  pivot_wider(id_cols = c(year_built, tractid),
              names_from = structure_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !year_built), na.rm = TRUE), .before = SFD) %>% 
  split(., .$year_built) %>% 
  lapply(format_tracts)

# Write to xlsx
file_name_county <- paste0("kitsap_unit_estimates_county_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_juris <- paste0("kitsap_unit_estimates_juris_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_tract <- paste0("kitsap_unit_estimates_tract10_", format(Sys.Date(), "%Y%m%d"), ".xlsx")

write_xlsx(x = county_units, path = paste0(output_file_path, file_name_county))
write_xlsx(x = juris_units, path = paste0(output_file_path, file_name_juris))
write_xlsx(x = tract_units, path = paste0(output_file_path, file_name_tract))
