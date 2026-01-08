#Install library packages before running

library(tidyverse)
library(writexl)
library(data.table)
library(rlist)

-----------------------------------------------------------------------------------------------------------------------------------------------------------
#Current Year Processing

# Data directories
data_dir <- "J:/Projects/Assessor/assessor_permit/king/data/2025"
current_data_dir <- file.path(data_dir, "extracts/current")
base_data_dir <- file.path(data_dir, "extracts/base_year_2009")
inputs_data_dir <- file.path(data_dir, "script_inputs")
outputs_data_dir <- "J:/Projects/Assessor/assessor_permit/king/data/2025/script_outputs/"

# file names
id_files <- list(pin_translation = "PIN_Translation_2024_2009.csv",
                 city_tract = "city_tract.csv",
                 full_city_list = "full_city_list.csv",
                 full_tract_list = "full_tract20_list.csv",
                 mobile_home_park_openings = "mobile_home_park_openings.csv",
                 mobile_home_park_closures = "mobile_home_park_closures.csv")
                 
bldg_files <- list(apts = "EXTR_AptComplex.csv",
                 resbldgs = "EXTR_ResBldg.csv",
                 condo_complex = "EXTR_CondoComplex.csv",
                 condo_units = "EXTR_CondoUnit2.csv",
                 comm_bldg = "EXTR_CommBldg.csv",
                 comm_bldg_section = "EXTR_CommBldgSection.csv",
                 parcels = "EXTR_Parcel.csv",
                 unit_breakdown = "EXTR_UnitBreakdown.csv",
                 look_up = "EXTR_LookUp.csv",
                 apts_09 = "aptcomplex_extr_2009.txt",
                 resbldgs_09 = "resbldg_extr_2009.txt",
                 condo_complex_09 = "condocomplex_extr_2009.txt",
                 condo_units_09 = "condounit_extr_2009.txt",
                 comm_bldg_section_09 = "commbldgsection_extr_2009.txt",
                 parcels_09 = "parcel_extr_2009.txt",
                 look_up_09 = "lookup_extr_2009.txt")


# formats list of id files
id_dfs <- list() # initiate empty list

for(id_file in id_files) {
  df <- read.csv(file.path(inputs_data_dir, id_file))
  names(df) <- tolower(names(df)) # set column names to lowercase
  # id_dfs[[i]] <- df # Option 1: store file in list
  id_dfs[[names(which(id_files == id_file))]] <- df # Option 2: store file in list with names for each element
}


# formats list of building files 
bldg_dfs <- list()


for(b in bldg_files) {
  element_name <- names(which(bldg_files == b))
  if(element_name %in% c('apts','resbldgs','condo_complex','condo_units','comm_bldg','comm_bldg_section','parcels','unit_breakdown','look_up')) {
    dir <- current_data_dir
  } else if(element_name %in% c('apts_09','resbldg_09','condo_complex_09','condo_units_09','comm_bldg_section_09','parcels_09','look_up_09')) {
    dir <- base_data_dir
  }
  
  df <- read.csv(file.path(dir, b))
  names(df) <- tolower(names(df)) # lowercase columns
  
  if(element_name %in% c('look_up','look_up_09')) {
    bldg_dfs[[element_name]] <- df
    next
  }
  if (element_name %in% c('condo_complex','condo_complex_09')) {
    df$minor <- "0000"
  } else df$minor <- str_pad(df$minor, 4, pad = 0)
  
  df$major <- str_pad(df$major, 6, pad = 0) # padding
  df$pin <- paste(df$major, df$minor, sep="")
  
  bldg_dfs[[element_name]] <- df 
}


# Select residential buildings built 2010 and after
bldg_dfs[['resbldgs']] <- filter(bldg_dfs[['resbldgs']], yrbuilt >= 2010)
bldg_dfs[['resbldgs']]$table = "resbldg"
bldg_dfs[['apts']] <- filter(bldg_dfs[['apts']], yrbuilt >= 2010)
bldg_dfs[['apts']]$table = "apts"
bldg_dfs[['condo_complex']] <- filter(bldg_dfs[['condo_complex']], yrbuilt >= 2010)
bldg_dfs[['condo_complex']]$table = "condo_complex"

# Change column name in ResBldg table to be consistent with apt/condo tables
bldg_dfs[['resbldgs']]$nbrunits <- bldg_dfs[['resbldgs']]$nbrlivingunits

# Summarize residential/commercial buildings by Parcel to remove duplicate pins
bldg_dfs[['resbldgs']] <- bldg_dfs[['resbldgs']] %>%
  group_by(pin) %>%
  summarise(nbrunits=sum(nbrunits),nbrbldgs=NROW(pin),yrbuilt=max(yrbuilt),yrrenovated=min(yrrenovated),address=first(address),zipcode=first(zipcode),table="resbldg")

bldg_dfs[['comm_bldg']] <- bldg_dfs[['comm_bldg']] %>%
  group_by(pin )%>%
  summarise(nbrbldgs=NROW(pin),yrbuilt=max(yrbuilt),address=first(address),zipcode=first(zipcode),Table="Commercial")

# Remove commercial-only condo complexes and denotes any complex type that is a mobile home or floating home
bldg_dfs[['condo_complex']] <- filter(bldg_dfs[['condo_complex']],!complextype %in% c(3,8)) %>%
                 mutate(mh_fm=case_when(complextype %in% c(6,7)~'yes'))


## Looks at the "Use" field in the commercial building section table to classify apartment complexes as 'residential','group quarters', or 'other'

# filter commercial building data frame for two columns
sec_use <- bldg_dfs[['comm_bldg_section']] %>% 
  select(pin, sectionuse)

# create joined data frame
sec_use_apts <- left_join(bldg_dfs[['apts']], sec_use, by = 'pin')

# section uses by group
res <- c(300,984,352,348,351,459)
gq <- c(982,321,324,424,451,710,589,551,313,335)

# create new intermediate column 'unit_category' and apply criteria with case_when().
# result is one-to-many table
df_code <- sec_use_apts %>% 
  select(pin, complexdescr, sectionuse) %>% 
  mutate(unit_category = case_when(sectionuse %in% res ~ 'Residential',
                                   sectionuse %in% gq ~ 'Group Quarters',
                                   TRUE ~ 'Other')) %>% 
  mutate(unit_bin = case_when(unit_category == 'Residential' ~ 100,
                              unit_category == 'Group Quarters' ~ 1))

# group by pin and Complex Name. Sum of numbers will determine label
df_cat <- df_code %>% 
  drop_na(unit_bin) %>% 
  group_by(pin, complexdescr) %>% 
  summarise(sum_unit_bin = sum(unit_bin))

# final one-to-one table, apply labels
bldg_dfs[['apts']] <- bldg_dfs[['apts']] %>% 
  left_join(df_cat, by = c('pin', 'complexdescr')) %>% 
  mutate(complex_category = case_when(sum_unit_bin >= 100 ~ 'Residential',
                                      sum_unit_bin < 100 ~ 'Group Quarters',
                                      is.na(sum_unit_bin) ~ 'Other'))

# Present Use codes found in look up table
present_use <- filter(bldg_dfs[['look_up']],lutype==102)

# Merges residential records into a single table, selects key fields, and removed group quarter/non-res complexes
current_res_merge <- merge(bldg_dfs[['resbldgs']], bldg_dfs[['apts']], all=TRUE) %>%
  merge(bldg_dfs[['condo_complex']], all=TRUE) %>%
  left_join(bldg_dfs[['parcels']], by ='pin') %>%
  left_join(present_use, by = c('presentuse' = 'luitem')) %>%
  select(pin, yrbuilt,nbrbldgs, nbrunits, complex_category, address, zipcode, table, districtname, ludescription) %>%
  filter(!complex_category %in% c('Group Quarters','Other')) %>%
  filter(nbrunits >0)


-----------------------------------------------------------------------------------------------------------------------------------------------------------
#2009 Base Year Processing
  

# Add column specifying the table source
bldg_dfs[['resbldgs_09']]$table = "resbldg"
bldg_dfs[['apts_09']]$table = "apts"
bldg_dfs[['condo_complex_09']]$table = "condo complex"

# Summarize condo complex buildings by Parcel to remove duplicate pins
bldg_dfs[['condo_complex_09']] <- bldg_dfs[['condo_complex_09']] %>%
  group_by(pin) %>%
  summarise(nbrunits=sum(nbrunits),nbrbldgs=sum(nbrbldgs),yrbuilt=max(yrbuilt),complex_type=first(complextype),address=first(situsaddress),zipcode=first(zipcode),table=first(table))

# Summarize residential buildings by Parcel to remove duplicate pins
bldg_dfs[['resbldgs_09']] <- bldg_dfs[['resbldgs_09']] %>%
  group_by(pin) %>%
  summarise(nbrunits=sum(nbrlivingunits),nbrbldgs=NROW(pin),yrbuilt=max(yrbuilt),address=first(situsaddress),zipcode=first(zipcode),table=first(table))

# Remove commercial-only condo complexes and denotes any complex type that is a mobile home or floating home
bldg_dfs[['condo_complex_09']] <- filter(bldg_dfs[['condo_complex_09']],!complex_type %in% c(3,8)) %>% 
  mutate(MH_FH=case_when(complex_type %in% c(6,7)~'Yes'))

## Looks at the "Use" field in the commercial building section table to classify apartment complexes as 'residential'or 'group quarters'

# filter commercial building data frame for two columns
sec_use_09 <- bldg_dfs[['comm_bldg_section_09']] %>% 
  select(pin, sectionuse)

# create joined data frame
sec_use_apts_09 <- left_join(bldg_dfs[['apts_09']], sec_use_09, by = 'pin')

# section uses by group
res_09 <- c(300,348,352,551,459)
gq_09 <- c(321,323,324,335,424,451,313)

# create new intermediate column 'unit_category' and apply criteria with case_when().
# result is one-to-many table
df_code_09 <- sec_use_apts_09 %>% 
  select(pin, description, sectionuse) %>% 
  mutate(unit_category = case_when(sectionuse %in% res_09 ~ 'Residential',
                                   sectionuse %in% gq_09 ~ 'Group Quarters',
                                   TRUE ~ 'Other')) %>% 
  mutate(unit_bin = case_when(unit_category == 'Residential' ~ 100,
                              unit_category == 'Group Quarters' ~ 1))

# group by pin and Complex Name. Sum of numbers will determine label
df_cat_09 <- df_code_09 %>% 
  drop_na(unit_bin) %>% 
  group_by(pin, description) %>% 
  summarise(sum_unit_bin = sum(unit_bin))

# final one-to-one table, apply labels
bldg_dfs[['apts_09']] <- bldg_dfs[['apts_09']] %>% 
  left_join(df_cat_09, by = c('pin', 'description')) %>% 
  mutate(complex_category = case_when(sum_unit_bin >= 100 ~ 'Residential',
                                      sum_unit_bin < 100 ~ 'Group Quarters',
                                      is.na(sum_unit_bin) ~ 'Residential'))

# Filter out un-needed columns from apt complex table and change names to be consistent with other tables
bldg_dfs[['apts_09']] <- bldg_dfs[['apts_09']] %>%
  summarise(pin=pin,nbrbldgs=numbldgs,yrbuilt=yrbuilt,nbrunits=numunits,address=situsaddress,table=table,complex_category=complex_category)

# Present Use codes found in look up table
present_use_09 <- filter(bldg_dfs[['look_up_09']],lutype==102)

# Changes 'present use' field to integer to match 'luitem' field for join
bldg_dfs[['parcels_09']]$presentuse <- as.integer(bldg_dfs[['parcels_09']]$presentuse)

# Merges resbldg/apt/condo records and removes pins in apts table that are also found in ResBldg table (ResBldg pins were built more recently)
base_res_merge <- merge(bldg_dfs[['resbldgs_09']],bldg_dfs[['apts_09']],all=TRUE) %>%
  merge(bldg_dfs[['condo_complex_09']],all=TRUE) %>%
  arrange(pin,desc(table)) %>%
  filter(duplicated(pin) == FALSE) %>%
  left_join(bldg_dfs[['parcels_09']], by ='pin') %>%
  left_join(present_use_09, by = c('presentuse' = 'luitem')) %>%
  select(pin, nbrbldgs, nbrunits, yrbuilt, table, ludescription, complex_category) %>%
  rename("pin_09" = "pin", "nbrbldgs_09" = "nbrbldgs", "nbrunits_09" = "nbrunits", "yrbuilt_09" = "yrbuilt", "table_09" = "table", "ludescription_09" = "ludescription", "complex_category_09" = "complex_category") %>%
  filter(!complex_category_09 %in% c('Group Quarters','Other'))

-----------------------------------------------------------------------------------------------------------------------------------------------------------
# Joining current year records to base year records and tabulate net change summaries

# Determines which pins from the current year records are found in the base year records and populates a new "Join_Method" column with "Table"
joined_pins <- merge(current_res_merge,base_res_merge,by.x="pin",by.y="pin_09",all=FALSE) %>%
  select("pin") %>%
  rename("pin_join"="pin")
  joined_pins$join_method <- "table"

current_res_merge <- merge(current_res_merge,joined_pins,by.x="pin",by.y="pin_join",all=TRUE)
current_res_merge$join_method[is.na(current_res_merge$join_method)] <- "spatial join"

id_dfs[['pin_translation']]$pin_2009 <- as.character(id_dfs[['pin_translation']]$pin_2009)
id_dfs[['pin_translation']]$pin <- as.character(id_dfs[['pin_translation']]$pin)

# Joins GIS pin translation table to the current records and populates pin_2009 field
current_res_merge <- current_res_merge %>%
  left_join(select(id_dfs[['pin_translation']],pin,pin_2009),by=c("pin"))

current_res_merge$pin_2009[current_res_merge$join_method=='table'] <- current_res_merge$pin[current_res_merge$join_method=='table']

# Joins base record attributes to current records  and cleans up columns in output
base_current_join <- left_join(current_res_merge,base_res_merge,by=c("pin_2009"="pin_09")) %>%
  rename("presentuse"="ludescription","presentuse_09"="ludescription_09")

# Summarizes number of occurrences of a base pin in the final table
base_pin_count <- base_current_join %>%
  group_by(pin_2009) %>%
  summarise(base_pins = n()) %>%
  na.omit()

# Joins number of occurrences of base pin field to the final table
base_current_join <- left_join(base_current_join,base_pin_count,by = 'pin_2009')

# Assigns activity type categories by comparing current year records against base year records
base_current_join$development[base_current_join$nbrunits_09==0 | is.na(base_current_join$yrbuilt_09)] <- "new development"
base_current_join$development[base_current_join$nbrunits == base_current_join$nbrunits_09 & is.na(base_current_join$development) & base_current_join$base_pins == 1] <- "rebuild or remodel"
base_current_join$development[base_current_join$nbrunits != base_current_join$nbrunits_09 & is.na(base_current_join$development) | is.na(base_current_join$development) & base_current_join$base_pins >1] <- "redevelopment"

# Tags demolition records based on activity type
base_current_join$demolition[base_current_join$development=="redevelopment"] <- 1
base_current_join$demolition[base_current_join$development=="new development"] <- 0
base_current_join$demolition[base_current_join$development=="rebuild or remodel"] <- 1

# Creates units per building column
base_current_join$units_per_bldg <- round(base_current_join$nbrunits/base_current_join$nbrbldgs,digits=0)
base_current_join$units_per_bldg_09 <- round(base_current_join$nbrunits_09/base_current_join$nbrbldgs_09,digits=0)

# Updates 'presentuse' field with spaces removed in order to make it easier to query
base_current_join$presentuse <- str_remove_all(base_current_join$presentuse," ")
base_current_join$presentuse_09 <- str_remove_all(base_current_join$presentuse_09," ")

# Creates a new field denoting single family parcels as defined by the present use field
base_current_join$sf[base_current_join$presentuse %in% c("SingleFamily(C/IUse)","SingleFamily(C/IZone)","SingleFamily(ResUse/Zone)","Vacant(Single-family)")] <- 1
base_current_join$sf_09[base_current_join$presentuse_09 %in% c("SingleFamily(C/IUse)","SingleFamily(C/IZone)","SingleFamily(ResUse/Zone)","Vacant(Single-family)")] <- 1

# Assigns structure type
base_current_join$structure_type[base_current_join$units_per_bldg == 1|base_current_join$sf ==1] <- "single family detached"
base_current_join$structure_type[base_current_join$presentuse =="TownhousePlat"] <- "single family attached"
base_current_join$structure_type[(base_current_join$units_per_bldg >= 2 & base_current_join$units_per_bldg <= 4)] <- "multifamily 2-4 units"
base_current_join$structure_type[(base_current_join$units_per_bldg >= 5 & base_current_join$units_per_bldg <= 9)] <- "multifamily 5-9 units"
base_current_join$structure_type[(base_current_join$units_per_bldg >= 10 & base_current_join$units_per_bldg <= 19)] <- "multifamily 10-19 units"
base_current_join$structure_type[(base_current_join$units_per_bldg >= 20 & base_current_join$units_per_bldg <= 49)] <- "multifamily 20-49 units"
base_current_join$structure_type[(base_current_join$units_per_bldg >= 50)] <- "multifamily 50+ units"
base_current_join$structure_type[base_current_join$presentuse =="MobileHome"] <- "mobile homes"

base_current_join$structure_type_09[base_current_join$units_per_bldg_09 == 1|base_current_join$sf_09 ==1] <- "single family detached"
base_current_join$structure_type_09[base_current_join$presentuse_09 =="TownhousePlat"] <- "single family attached"
base_current_join$structure_type_09[(base_current_join$units_per_bldg_09 >= 2 & base_current_join$units_per_bldg_09 <= 4)] <- "multifamily 2-4 units"
base_current_join$structure_type_09[(base_current_join$units_per_bldg_09 >= 5 & base_current_join$units_per_bldg_09 <= 9)] <- "multifamily 5-9 units"
base_current_join$structure_type_09[(base_current_join$units_per_bldg_09 >= 10 & base_current_join$units_per_bldg_09 <= 19)] <- "multifamily 10-19 units"
base_current_join$structure_type_09[(base_current_join$units_per_bldg_09 >= 20 & base_current_join$units_per_bldg_09 <= 49)] <- "multifamily 20-49 units"
base_current_join$structure_type_09[(base_current_join$units_per_bldg_09 >= 50)] <- "multifamily 50+ units"
base_current_join$structure_type_09[base_current_join$presentuse_09 =="MobileHome"] <- "mobile homes"

# Sets demolitions to negative numbers
base_current_join$demo_units[base_current_join$demolition==1] <- base_current_join$nbrunits_09[base_current_join$demolition==1]*(-1)
base_current_join$demo_units[base_current_join$demolition!=1] <- 0

# Changes Tacoma records to Federal Way to fix GIS boundary issue
id_dfs[['city_tract']]$juris[id_dfs[['city_tract']]$juris == 'Tacoma'] <- "Federal Way"

# Adds city/tract fields and filters for relevant years in time series
base_current_join <- base_current_join %>%
  left_join(id_dfs[['city_tract']],by='pin') %>%
  filter(yrbuilt %in% c(2010:2024)) %>%
  mutate(juris = ifelse(is.na(juris),'Z-Missing',juris))

# If a record does not have a jurisdiction name it show up as 'Z-Missing' in the summary tables and need to be fixed
base_current_join$districtname <- str_to_title(base_current_join$districtname)
base_current_join$juris[base_current_join$juris == 'Z-Missing'] <- base_current_join$districtname[base_current_join$juris == 'Z-Missing']
base_current_join$juris[base_current_join$juris == 'Seatac'] <- 'SeaTac'

# Reclassifies certain developments that need to be corrected (manual review of the highest demolition counts is needed as some of them are not actually demolitions)
base_current_join$demolition[base_current_join$pin_2009 == '1978200470'] <- 0
base_current_join$development[base_current_join$pin_2009 == '1978200470'] <- "new development"
base_current_join$demolition[base_current_join$pin_2009 == '0623049257'] <- 0
base_current_join$development[base_current_join$pin_2009 == '0623049257'] <- "new development"
base_current_join$demolition[base_current_join$pin_2009 == '2354600000'] <- 0
base_current_join$development[base_current_join$pin_2009 == '2354600000'] <- "new development"
base_current_join$demolition[base_current_join$pin_2009 == '7954000005'] <- 0
base_current_join$development[base_current_join$pin_2009 == '7954000005'] <- "new development"
base_current_join$demolition[base_current_join$pin_2009 == '1926049216'] <- 0
base_current_join$development[base_current_join$pin_2009 == '1926049216'] <- "new development"
base_current_join$demolition[base_current_join$pin_2009 == '3388360000'] <- 0
base_current_join$development[base_current_join$pin_2009 == '3388360000'] <- "new development"
base_current_join$demolition[base_current_join$pin_2009 == '8663270025'] <- 0
base_current_join$development[base_current_join$pin_2009 == '8663270025'] <- "new development"
base_current_join$demolition[base_current_join$pin_2009 == '7954000005'] <- 0
base_current_join$development[base_current_join$pin_2009 == '7954000005'] <- "new development"
base_current_join$demolition[base_current_join$pin_2009 == '2149800242'] <- 0
base_current_join$development[base_current_join$pin_2009 == '2149800242'] <- "new development"

# Creates a demolitions table that removes the duplication found in the joined current-base table
demos <- base_current_join %>%
  filter(demolition==1) %>%
  distinct(pin_2009,.keep_all=TRUE) %>%
  mutate(development = 'demolition')

# Edits/additions to data based on supplementary data obtained from OFM related to mobile home park openings/closures
base_current_join$development[base_current_join$pin %in% c('2724201800','1620400190','1620400180','1620400170','1620400160','1620400150',
'1620400140','1620400130','1620400120','1620400110','1620400100','1620400090','1620400080','1620400070','1620400060','1620400050',
'1620400040','1620400030','1620400020','1620400010')] <- 'redevelopment'

base_current_join$structure_type_09[base_current_join$pin == '9516100050'] <- 'mobile homes'
demos$structure_type_09[demos$pin == '9516100010'] <- 'mobile homes'

demos <- rbind(demos,id_dfs[["mobile_home_park_closures"]])
base_current_join <- rbind(base_current_join,id_dfs[["mobile_home_park_openings"]])


## Creates functions to summarize the net change estimates by county total, jurisdiction, and census tract

# These functions format the final tables and any missing cities/tracts that don't have data
format_cities <- function(x) {
  x %>%
    full_join(id_dfs[['full_city_list']],by='juris') %>%
    replace(is.na(.), 0) %>%
    arrange(juris)
}

format_tracts <- function(x) {
  x %>%
    full_join(id_dfs[['full_tract_list']],by='geoid20') %>%
    replace(is.na(.), 0) %>%
    arrange(geoid20)
}


# Creates final net change summaries by county/jurisdiction/tract

#total
new_total <- base_current_join %>%
  group_by(structure_type,yrbuilt) %>%
  rename(str_type=structure_type) %>%
  summarise(new_units=sum(nbrunits))

lost_total <- demos %>%
  group_by(structure_type_09,yrbuilt) %>%
  rename(str_type=structure_type_09) %>%
  summarise(lost_units=sum(demo_units))

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
  mutate(net_total = rowSums(across(where(is.numeric) & !yrbuilt), na.rm = TRUE), .before = `single family detached`) %>%
  arrange(yrbuilt)
 

#city
new_city <- base_current_join %>%
  group_by(juris,structure_type,yrbuilt) %>%
  rename(str_type=structure_type) %>%
  summarise(new_units=sum(nbrunits))

lost_city <- demos %>%
  group_by(juris,structure_type_09,yrbuilt) %>%
  rename(str_type=structure_type_09) %>%
  summarise(lost_units=sum(demo_units))

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
new_tract <- base_current_join %>%
  group_by(geoid20,structure_type,yrbuilt) %>%
  rename(str_type=structure_type) %>%
  summarise(new_units=sum(nbrunits))

lost_tract <- demos %>%
  group_by(geoid20,structure_type_09,yrbuilt) %>%
  rename(str_type=structure_type_09) %>%
  summarise(lost_units=sum(demo_units))

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
new_unit_parcel_records <- base_current_join %>%
  mutate(project_year = 2025) %>%
  mutate(county = "King") %>%
  mutate(county_fips = "033") %>%
  select(project_year,
         pin,
         year = yrbuilt,
         units = nbrunits,
         buildings = nbrbldgs,
         structure_type,
         development,
         jurisdiction = juris,
         geoid20,
         county,
         county_fips,
         x_coord,
         y_coord)

lost_unit_parcel_records <- demos %>%
  mutate(project_year = 2025) %>%
  mutate(county = "King") %>%
  mutate(county_fips = "033") %>%
  select(project_year,
         pin,
         year = yrbuilt,
         units = demo_units,
         buildings = nbrbldgs_09,
         structure_type = structure_type_09,
         development,
         jurisdiction = juris,
         geoid20,
         county,
         county_fips,
         x_coord,
         y_coord)

king_parcel_tbl <- rbind(new_unit_parcel_records,lost_unit_parcel_records)

# Creates elmer-ready summary tables
king_county_units_long <- total_net_summary %>% 
  pivot_longer(cols = 'net_total':'mobile homes',
               names_to = "structure_type",
               values_to = "net_units") %>% 
  mutate(project_year = 2025, 
         county = "King") %>% 
  select(project_year, county, year = yrbuilt, structure_type, net_units)

king_juris_units_long <- city_net_summary %>%
  pivot_longer(cols = 'net_total':'mobile homes',
               names_to = "structure_type",
               values_to = "net_units") %>% 
  mutate(project_year = 2025, 
         county = "King") %>% 
  select(project_year, county, juris, year = yrbuilt, structure_type, net_units)

king_tract_units_long <- tract_net_summary %>%
  pivot_longer(cols = 'net_total':'mobile homes',
               names_to = "structure_type",
               values_to = "net_units") %>% 
  mutate(project_year = 2025, 
         county = "King") %>% 
  select(project_year, county, tract = geoid20, year = yrbuilt, structure_type, net_units)


# Write to xlsx
file_name_county <- paste0("king_unit_estimates_county_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_juris <- paste0("king_unit_estimates_juris_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_tract <- paste0("king_unit_estimates_tract20_", format(Sys.Date(), "%Y%m%d"), ".xlsx")
file_name_parcel <- paste0("king_unit_estimates_parcel_", format(Sys.Date(), "%Y%m%d"), ".xlsx")

write_xlsx(x = total_net_summary, path = paste0(outputs_data_dir, file_name_county))
write_xlsx(x = split(city_net_summary, city_net_summary$yrbuilt) %>% map(., ~ (.x %>% select(-yrbuilt))),
           path = paste0(outputs_data_dir, file_name_juris))
write_xlsx(x = split(tract_net_summary, tract_net_summary$yrbuilt) %>% map(., ~ (.x %>% select(-yrbuilt))),
           path = paste0(outputs_data_dir, file_name_tract))
write_xlsx(x = king_parcel_tbl, path = paste0(outputs_data_dir, file_name_parcel))

# save tables to .rda for combining script
save(king_parcel_tbl, king_county_units_long, king_juris_units_long, king_tract_units_long,
     file = "J:/Projects/Assessor/assessor_permit/data_products/2025/elmer/king_tables.rda")
