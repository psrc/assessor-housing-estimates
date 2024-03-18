#Install library packages before running

library(tidyverse)
library(writexl)
library(data.table)

-----------------------------------------------------------------------------------------------------------------------------------------------------------
  
# Data directories
data_dir <- "J:/Projects/Assessor/assessor_permit/snohomish/data/2023"
input_dir <- file.path(data_dir, "script_inputs")
output_dir <- "J:/Projects/Assessor/assessor_permit/snohomish/data/2023/script_outputs/"

# file names
current_base_join_file_name <- "current_base_join_gis_output.csv"
full_city_list_file_name <- "full_city_list.csv"
full_tract_list_file_name <- "full_tract20_list.csv"

# Import input file csv
current_base_join <- read.csv(file.path(input_dir, current_base_join_file_name))
full_city_list <- read.csv(file.path(input_dir, full_city_list_file_name))
full_tract_list <- read.csv(file.path(input_dir, full_tract_list_file_name))

# Sets frequency/units fields to numeric
current_base_join$frequency <- as.numeric(as.character(current_base_join$frequency))
current_base_join$frequency_10 <- as.numeric(as.character(current_base_join$frequency_10))
current_base_join$units <- as.numeric(as.character(current_base_join$units))
current_base_join$units_10 <- as.numeric(as.character(current_base_join$units_10))

# Populate unit count fields based on structure type (multi-family records should have already been populated in GIS)
current_base_join$units[is.na(current_base_join$units)] <- 0
current_base_join$units[current_base_join$usedesc == "Duplex"] <- current_base_join$frequency[current_base_join$usedesc == "Duplex"] * 2
current_base_join$units[current_base_join$usedesc == "Triplex"] <- current_base_join$frequency[current_base_join$usedesc == "Triplex"] * 3
current_base_join$units[!(current_base_join$usedesc %in% c("Apartment, High Rise, Shell", "Apartments", "4-6 family", "Mixed Retail w/Residential", "Duplex", "Triplex"))] <- current_base_join$frequency[!(current_base_join$usedesc %in% c("Apartment, High Rise, Shell", "Apartments", "4-6 family", "Mixed Retail w/Residential", "Duplex", "Triplex"))] * 1

current_base_join$units_10[is.na(current_base_join$units_10)] <- 0
current_base_join$units_10[current_base_join$usedesc_10 == "Duplex"] <- current_base_join$frequency_10[current_base_join$usedesc_10 == "Duplex"] * 2
current_base_join$units_10[current_base_join$usedesc_10 == "Triplex"] <- current_base_join$frequency_10[current_base_join$usedesc_10 == "Triplex"] * 3
current_base_join$units_10[!(current_base_join$usedesc_10 %in% c("5", "Apartment, High Rise, Shell", "Apartments", "4-6 family", "Mixed Retail w/Residential", "Duplex", "Triplex"))] <- 1
current_base_join$units_10[current_base_join$usedesc_10 == ""] <- 0
current_base_join$units_10[!(current_base_join$usedesc_10 %in% c("Apartment, High Rise, Shell", "Apartments", "4-6 family", "Mixed Retail w/Residential", "Duplex", "Triplex"))] <- current_base_join$frequency_10[!(current_base_join$usedesc_10 %in% c("Apartment, High Rise, Shell", "Apartments", "4-6 family", "Mixed Retail w/Residential", "Duplex", "Triplex"))]

# Populate building count fields for current records that were not populated manually in GIS
current_base_join$buildings[is.na(current_base_join$buildings)] <- 0
current_base_join$buildings[!(current_base_join$usedesc %in% c("Apartment, High Rise, Shell", "Apartments", "4-6 family", "Mixed Retail w/Residential", "Other residential","Condo - Other", "Condo - Owner"))] <- current_base_join$frequency[!(current_base_join$usedesc %in% c("Apartment, High Rise, Shell", "Apartments", "4-6 family", "Mixed Retail w/Residential", "Other residential","Condo - Other", "Condo - Owner"))]
current_base_join$buildings[current_base_join$usedesc %in% c("Condo - Other", "Condo - Owner") & current_base_join$frequency == 1] <- 1

current_base_join$buildings_10[is.na(current_base_join$buildings_10)] <- 0
current_base_join$buildings_10[!(current_base_join$usedesc_10 %in% c("Apartment, High Rise, Shell", "Apartments", "4-6 family", "Mixed Retail w/Residential", "Other residential","Condo - Other", "Condo - Owner"))] <- current_base_join$frequency_10[!(current_base_join$usedesc_10 %in% c("Apartment, High Rise, Shell", "Apartments", "4-6 family", "Mixed Retail w/Residential", "Other residential","Condo - Other", "Condo - Owner"))]
current_base_join$buildings_10[current_base_join$usedesc_10 %in% c("Condo - Other", "Condo - Owner") & current_base_join$frequency_10 == 1] <- 1

# Removes records with 0 units
current_base_join <- filter(current_base_join,units != 0)

# Creates a "new units" field that takes into account phased developments
current_base_join <- current_base_join %>%
  mutate(new_units = units) %>%
  mutate(new_units = ifelse(usedesc %in% c("Apartments","Apartment, High Rise, Shell") & notes == "phased development", units - units_10, new_units))

# Summarizes number of occurrences of a base pin in the final table
base_pin_count <- current_base_join %>%
  group_by(group_id_10) %>%
  summarise(base_pins = n()) %>%
  na.omit()

# Joins number of occurrences of base pin field to the final table
current_base_join <- left_join(current_base_join,base_pin_count,by = 'group_id_10')

# Assigns activity type categories by comparing current year records against base year records
current_base_join$development[current_base_join$units_10==0] <- "new development"
current_base_join$development[is.na(current_base_join$yrbuilt_10)] <- "new development"
current_base_join$development[current_base_join$no_demolition == 1] <- "new development"
current_base_join$development[current_base_join$usedesc == 5] <- "new development"
current_base_join$development[current_base_join$new_units == current_base_join$units_10 & is.na(current_base_join$development) & current_base_join$base_pins == 1] <- "rebuild or remodel"
current_base_join$development[current_base_join$new_units != current_base_join$units_10 & is.na(current_base_join$development) | is.na(current_base_join$development) & current_base_join$base_pins >1] <- "redevelopment"

# Tags demolition records based on activity type
current_base_join$demolition[current_base_join$development == "redevelopment"] <- 1
current_base_join$demolition[current_base_join$development == "new development"] <- 0
current_base_join$demolition[current_base_join$development == "rebuild or remodel"] <- 1
current_base_join$demolition[current_base_join$no_demolition == 1] <- 0
current_base_join$demolition[current_base_join$mobile_home_park_closure == 1] <- 1

# Removes NAs from no_demolition field
current_base_join$no_demolition[is.na(current_base_join$no_demolition)] <- 0

# Creates units per building column
current_base_join$units_per_bldg <- round(current_base_join$new_units/current_base_join$buildings,digits=0)
current_base_join$units_per_bldg_10 <- round(current_base_join$units_10/current_base_join$buildings_10,digits=0)

#Assigns structure types
current_base_join$structure_type[current_base_join$units_per_bldg == 1|current_base_join$usedesc == "Single family"] <- "single family detached"
current_base_join$structure_type[current_base_join$sf_attached == 1] <- "single family attached"
current_base_join$structure_type[current_base_join$units_per_bldg >= 2 & current_base_join$units_per_bldg <= 4] <- "multifamily 2-4 units"
current_base_join$structure_type[current_base_join$units_per_bldg >= 5 & current_base_join$units_per_bldg <= 9] <- "multifamily 5-9 units"
current_base_join$structure_type[current_base_join$units_per_bldg >= 10 & current_base_join$units_per_bldg <= 19] <- "multifamily 10-19 units"
current_base_join$structure_type[current_base_join$units_per_bldg >= 20 & current_base_join$units_per_bldg <= 49] <- "multifamily 20-49 units"
current_base_join$structure_type[current_base_join$units_per_bldg >= 50] <- "multifamily 50+ units"
current_base_join$structure_type[current_base_join$usedesc == 5] <- "mobile homes"

current_base_join$structure_type_10[current_base_join$units_per_bldg_10 == 1|current_base_join$usedesc_10 == "Single family"] <- "single family detached"
current_base_join$structure_type_10[current_base_join$sf_attached_10 == 1] <- "single family attached"
current_base_join$structure_type_10[current_base_join$units_per_bldg_10 >= 2 & current_base_join$units_per_bldg_10 <= 4] <- "multifamily 2-4 units"
current_base_join$structure_type_10[current_base_join$units_per_bldg_10 >= 5 & current_base_join$units_per_bldg_10 <= 9] <- "multifamily 5-9 units"
current_base_join$structure_type_10[current_base_join$units_per_bldg_10 >= 10 & current_base_join$units_per_bldg_10 <= 19] <- "multifamily 10-19 units"
current_base_join$structure_type_10[current_base_join$units_per_bldg_10 >= 20 & current_base_join$units_per_bldg_10 <= 49] <- "multifamily 20-49 units"
current_base_join$structure_type_10[current_base_join$units_per_bldg_10 >= 50] <- "multifamily 50+ units"
current_base_join$structure_type_10[current_base_join$usedesc_10 == 5] <- "mobile homes"

# Sets base units to negative and zeroes out non-demolition units
current_base_join$units_10 <- current_base_join$units_10*(-1)
current_base_join$units_10[current_base_join$demolition!=1] <- 0

# Creates new table with only the relevant years used for time series and removes mobile home parks
current_base_join_final <- current_base_join %>%
  filter(is.na(mobile_home_park)) %>%
  filter(is.na(mobile_home_park_10)) %>%
  filter(yrbuilt %in% c(2011:2022)) %>%
  rename('juris' = 'jurisdiction')

# Creates a demolitions table that removes the duplication found in the joined current-base table
demos <- current_base_join_final %>%
  filter(demolition==1) %>%
  distinct(group_id_10,.keep_all=TRUE) %>%
  mutate(development = 'demolition')

# Removes mobile home park closure records so they only show up in the demos table
current_base_join_final <- current_base_join_final %>%
  filter(mobile_home_park_closure != 1)
  
## Creates functions to summarize the net change estimates by county total, jurisdiction, and census tract

# These functions format the final tables and any missing cities/tracts that don't have data
format_cities <- function(x) {
  x %>%
    full_join(full_city_list,by='juris') %>%
    replace(is.na(.), 0) %>%
    arrange(juris)
}

format_tracts <- function(x) {
  x %>%
    full_join(full_tract_list,by='geoid20') %>%
    replace(is.na(.), 0) %>%
    arrange(geoid20)
}

## Creates final net change summaries by county/jurisdiction/tract

#total
new_total <- current_base_join_final %>%
    group_by(structure_type,yrbuilt) %>%
    rename(str_type=structure_type) %>%
    summarise(new_units=sum(new_units))

lost_total <- demos %>%
    group_by(structure_type_10,yrbuilt) %>%
    rename(str_type=structure_type_10) %>%
    summarise(lost_units=sum(units_10))

total_net_summary <- full_join(new_total, lost_total, by = join_by("yrbuilt", "str_type")) %>%
  replace_na(list(new_units = 0, lost_units = 0)) %>%
  mutate(net_units = new_units + lost_units,
         str_type = factor(str_type,
                                 levels = c("single family detached", 
                                            "single family attached", 
                                            "multifamily 2-4 units", 
                                            "multifamily 5-9 units", 
                                            "multifamily 10-19 units", 
                                            "multifamily 20-49 units", 
                                            "multifamily 50+ units", 
                                            "mobile homes"))) %>% 
  pivot_wider(id_cols = c(yrbuilt),
              names_from = str_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !yrbuilt), na.rm = TRUE), .before = `single family detached`)%>%
  arrange(yrbuilt)

#city
new_city <- current_base_join_final %>%
    group_by(juris,structure_type,yrbuilt) %>%
    rename(str_type=structure_type) %>%
    summarise(new_units=sum(new_units))

lost_city <- demos %>%
    group_by(juris,structure_type_10,yrbuilt) %>%
    rename(str_type=structure_type_10) %>%
    summarise(lost_units=sum(units_10))

city_net_summary <- full_join(new_city, lost_city, by = join_by("juris","yrbuilt", "str_type")) %>%
  replace_na(list(new_units = 0, lost_units = 0)) %>%
  mutate(net_units = new_units + lost_units,
         str_type = factor(str_type,
                                 levels = c("single family detached", 
                                            "single family attached", 
                                            "multifamily 2-4 units", 
                                            "multifamily 5-9 units", 
                                            "multifamily 10-19 units", 
                                            "multifamily 20-49 units", 
                                            "multifamily 50+ units", 
                                            "mobile homes"))) %>% 
  pivot_wider(id_cols = c(yrbuilt,juris),
              names_from = str_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !yrbuilt), na.rm = TRUE), .before = `single family detached`) %>%
  group_by(yrbuilt) %>% 
  group_modify(~ format_cities(.x)) %>% 
  ungroup()

#tract
new_tract <- current_base_join_final %>%
    group_by(geoid20,structure_type,yrbuilt) %>%
    rename(str_type=structure_type) %>%
    summarise(new_units=sum(new_units))

lost_tract <- demos %>%
    group_by(geoid20,structure_type_10,yrbuilt) %>%
    rename(str_type=structure_type_10) %>%
    summarise(lost_units=sum(units_10))

tract_net_summary <- full_join(new_tract, lost_tract, by = join_by("geoid20","yrbuilt", "str_type")) %>%
  replace_na(list(new_units = 0, lost_units = 0)) %>%
  mutate(net_units = new_units + lost_units,
         str_type = factor(str_type,
                           levels = c("single family detached", 
                                      "single family attached", 
                                      "multifamily 2-4 units", 
                                      "multifamily 5-9 units", 
                                      "multifamily 10-19 units", 
                                      "multifamily 20-49 units", 
                                      "multifamily 50+ units", 
                                      "mobile homes"))) %>% 
  pivot_wider(id_cols = c(yrbuilt,geoid20),
              names_from = str_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !yrbuilt), na.rm = TRUE), .before = `single family detached`) %>% 
  group_by(yrbuilt) %>% 
  group_modify(~ format_tracts(.x)) %>%
  ungroup()

# Creates final parcel table
new_unit_parcel_records <- current_base_join_final %>%
  mutate(project_year = 2023) %>%
  mutate(county = "Snohomish") %>%
  mutate(county_fips = "061") %>%
  mutate(pin=NA) %>%
  select(project_year,
         pin,
         year = yrbuilt,
         units,
         buildings,
         structure_type,
         development,
         jurisdiction = juris,
         geoid20,
         county,
         county_fips,
         x_coord = x,
         y_coord = y)

lost_unit_parcel_records <- demos %>%
  mutate(project_year = 2023) %>%
  mutate(county = "Snohomish") %>%
  mutate(county_fips = "061") %>%
  mutate(pin=NA) %>%
  select(project_year,
         pin,
         year = yrbuilt,
         units = units_10,
         buildings = buildings_10,
         structure_type = structure_type_10,
         development,
         jurisdiction = juris,
         geoid20,
         county,
         county_fips,
         x_coord = x,
         y_coord = y)
  
snohomish_parcel_tbl <- rbind(new_unit_parcel_records,lost_unit_parcel_records)

# Creates elmer-ready summary tables
snohomish_county_units_long <- total_net_summary %>% 
  pivot_longer(cols = 'net_total':'mobile homes',
               names_to = "structure_type",
               values_to = "net_units") %>% 
  mutate(project_year = 2023, 
         county = "Snohomish") %>% 
  select(project_year, county, year = yrbuilt, structure_type, net_units)

snohomish_juris_units_long <- city_net_summary %>%
  pivot_longer(cols = 'net_total':'mobile homes',
               names_to = "structure_type",
               values_to = "net_units") %>% 
  mutate(project_year = 2023, 
         county = "Snohomish") %>% 
  select(project_year, county, juris, year = yrbuilt, structure_type, net_units)

snohomish_tract_units_long <- tract_net_summary %>%
  pivot_longer(cols = 'net_total':'mobile homes',
               names_to = "structure_type",
               values_to = "net_units") %>% 
  mutate(project_year = 2023, 
         county = "Snohomish") %>% 
  select(project_year, county, tract = geoid20, year = yrbuilt, structure_type, net_units)

# Write to xlsx
file_name_county <- paste0("snohomish_unit_estimates_county_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_juris <- paste0("snohomish_unit_estimates_juris_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_tract <- paste0("snohomish_unit_estimates_tract20_", format(Sys.Date(), "%Y%m%d"), ".xlsx")


write_xlsx(x = total_net_summary, path = paste0(output_dir, file_name_county))
write_xlsx(x = split(city_net_summary, city_net_summary$yrbuilt) %>% map(., ~ (.x %>% select(-yrbuilt))),
           path = paste0(output_dir, file_name_juris))
write_xlsx(x = split(tract_net_summary, tract_net_summary$yrbuilt) %>% map(., ~ (.x %>% select(-yrbuilt))),
           path = paste0(output_dir, file_name_tract))

# save tables to .rda for combining script
save(snohomish_parcel_tbl, snohomish_county_units_long, snohomish_juris_units_long, snohomish_tract_units_long,
     file = "J:/Projects/Assessor/assessor_permit/data_products/2023/elmer/snohomish_tables.rda")
