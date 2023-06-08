install.packages("data.table")
install.packages("rlist")
install.packages("dplyr")
install.packages("tidyverse")
install.packages("writexl")
install.packages("stringr")

### remove install.packages lines ----
### remove dplyr & stringr b/c they're part of the tidyverse suite already ----

library(dplyr)
library(tidyverse)
library(writexl)
library(data.table)
library(rlist)
library(stringr)

#-----------------------------------------------------------------------------------------------------------------------------------------------------------
#Current Year Processing

# Set working directory
setwd("J:/Projects/Assessor/assessor_permit/scripts/assessor-housing-estimates")

### set root data dir, and then concatenate end URL ----

# Data directories
current_data_dir <- "J:/Projects/Assessor/assessor_permit/king/data/data_research_phase/extracts/current"
base_data_dir <- "J:/Projects/Assessor/assessor_permit/king/data/data_research_phase/extracts/base_year_2009"
inputs_data_dir <- "J:/Projects/Assessor/assessor_permit/king/data/data_research_phase/script_inputs"
outputs_data_dir <- "J:/Projects/Assessor/assessor_permit/king/data/data_research_phase/script_outputs"

### store filenames in a list (or named vector) ----

id_files <- list(pin_tran = "PIN_Translation_2021_2009.csv",
                 city_tract = "city_tract.csv",
                 city_list = "full_city_list.csv",
                 tract_list = "full_tract10_list.csv")

###
pin_translation_file_name <- "PIN_Translation_2021_2009.csv"
city_tract_file_name <- "city_tract.csv"
full_city_list_file_name <- "full_city_list.csv"
full_tract_list_file_name <- "full_tract10_list.csv"

### store filesnames in a list (or named vector) ----

bldg_files <- list(apts = "EXTR_AptComplex.csv",
                   resbldg = "EXTR_ResBldg.csv")

###
apts_file_name <- "EXTR_AptComplex.csv"
resbldgs_file_name <- "EXTR_ResBldg.csv"
condo_complex_file_name <- "EXTR_CondoComplex.csv"
condo_units_file_name <- "EXTR_CondoUnit2.csv"
comm_bldg_file_name <- "EXTR_CommBldg.csv"
comm_bldg_section_file_name <- "EXTR_CommBldgSection.csv"
look_up_file_name <- "EXTR_LookUp.csv"
parcels_file_name <- "EXTR_Parcel.csv"
unit_breakdown_file_name <- "EXTR_UnitBreakdown.csv"

apts_09_file_name <- "aptcomplex_extr_2009.txt"
resbldgs_09_file_name <- "resbldg_extr_2009.txt"
condo_complex_09_file_name <- "condocomplex_extr_2009.txt"
condo_units_09_file_name <- "condounit_extr_2009.txt"
comm_bldg_section_09_file_name <- "commbldgsection_extr_2009.txt"
look_up_09_file_name <- "lookup_extr_2009.txt"
parcels_09_file_name <- "parcel_extr_2009.txt"

### read in files with list(above) & loop ----
id_dfs <- list() # initiate empty list

for(i in id_files) {
  df <- read.csv(file.path(inputs_data_dir, i))
  names(df) <- tolower(names(df)) # set column names to lowercase
  # id_dfs[[i]] <- df # Option 1: store file in list
  id_dfs[[names(which(id_files == i))]] <- df # Option 2: store file in list with names for each element
}

###
pin_translation <- read.csv(file.path(inputs_data_dir, pin_translation_file_name))
city_tract <- read.csv(file.path(inputs_data_dir, city_tract_file_name))
full_city_list <- read.csv(file.path(inputs_data_dir, full_city_list_file_name))
full_tract_list <- read.csv(file.path(inputs_data_dir, full_tract_list_file_name))

### read in files with list(above) & loop ----
bldg_dfs <- list()

for(b in bldg_files) {
  df <- read.csv(file.path(current_data_dir, b))
  names(df) <- tolower(names(df)) # lowercase columns
  df$major <- str_pad(df$major, 6, pad = 0) # padding
  df$minor <- str_pad(df$minor, 4, pad = 0)
  df$pin <- paste(df$major, df$minor, sep="") # create new pin
  # can add in exception for condo minor with if/else...
  bldg_dfs[[names(which(bldg_files == b))]] <- df 
}

###
apts <- read.csv(file.path(current_data_dir, apts_file_name))
resbldgs <- read.csv(file.path(current_data_dir, resbldgs_file_name))
condo_complex <- read.csv(file.path(current_data_dir, condo_complex_file_name))
condo_units <- read.csv(file.path(current_data_dir, condo_units_file_name))
comm_bldg <- read.csv(file.path(current_data_dir, comm_bldg_file_name))
comm_bldg_section <- read.csv(file.path(current_data_dir, comm_bldg_section_file_name))
look_up <- read.csv(file.path(current_data_dir, look_up_file_name))
parcels <- read.csv(file.path(current_data_dir, parcels_file_name))
unit_breakdown <- read.csv(file.path(current_data_dir, unit_breakdown_file_name))

apts_09 <- read.csv(file.path(base_data_dir, apts_09_file_name))
resbldgs_09 <- read.csv(file.path(base_data_dir, resbldgs_09_file_name))
condo_complex_09 <- read.csv(file.path(base_data_dir, condo_complex_09_file_name))
condo_units_09 <- read.csv(file.path(base_data_dir, condo_units_09_file_name))
comm_bldg_section_09 <- read.csv(file.path(base_data_dir, comm_bldg_section_09_file_name))
look_up_09 <- read.csv(file.path(base_data_dir, look_up_09_file_name))
parcels_09 <- read.csv(file.path(base_data_dir, parcels_09_file_name))

### lower field names in loop (see above) ----
names(pin_translation) <- tolower(names(pin_translation))
names(city_tract) <- tolower(names(city_tract))
names(full_city_list) <- tolower(names(full_city_list))
names(full_tract_list) <- tolower(names(full_tract_list))

names(apts) <- tolower(names(apts))
names(resbldgs) <- tolower(names(resbldgs))
names(condo_complex) <- tolower(names(condo_complex))
names(condo_units) <- tolower(names(condo_units))
names(comm_bldg) <- tolower(names(comm_bldg))
names(comm_bldg_section) <- tolower(names(comm_bldg_section))
names(look_up) <- tolower(names(look_up))
names(parcels) <- tolower(names(parcels))
names(unit_breakdown) <- tolower(names(unit_breakdown))

names(apts_09) <- tolower(names(apts_09))
names(resbldgs_09) <- tolower(names(resbldgs_09))
names(condo_complex_09) <- tolower(names(condo_complex_09))
names(condo_units_09) <- tolower(names(condo_units_09))
names(comm_bldg_section_09) <- tolower(names(comm_bldg_section_09))
names(look_up_09) <- tolower(names(look_up_09))
names(parcels_09) <- tolower(names(parcels_09))

# Add 0's to major/minor fields so new PIN field will be 10 digits to match parcel shapefile

### set padding in loop (see above) ----
apts$major <- str_pad(apts$major, 6, pad = 0)
apts$minor <- str_pad(apts$minor, 4, pad = 0)
resbldgs$major <- str_pad(resbldgs$major, 6, pad = 0)
resbldgs$minor <- str_pad(resbldgs$minor, 4, pad = 0)
condo_complex$major <- str_pad(condo_complex$major, 6, pad = 0)
condo_complex$minor<- "0000"
condo_units$major <- str_pad(condo_units$major, 6, pad = 0)
condo_units$minor <- str_pad(condo_units$minor, 4, pad = 0)
comm_bldg$major <- str_pad(comm_bldg$major, 6, pad = 0)
comm_bldg$minor <- str_pad(comm_bldg$minor, 4, pad = 0)
comm_bldg_section$major <- str_pad(comm_bldg_section$major, 6, pad = 0)
comm_bldg_section$minor <- str_pad(comm_bldg_section$minor, 4, pad = 0)
parcels$major <- str_pad(parcels$major, 6, pad = 0)
parcels$minor <- str_pad(parcels$minor, 4, pad = 0)
unit_breakdown$major <- str_pad(unit_breakdown$major, 6, pad = 0)
unit_breakdown$minor <- str_pad(unit_breakdown$minor, 4, pad = 0)

apts_09$major <- str_pad(apts_09$major, 6, pad = 0)
apts_09$minor <- str_pad(apts_09$minor, 4, pad = 0)
resbldgs_09$major <- str_pad(resbldgs_09$major, 6, pad = 0)
resbldgs_09$minor <- str_pad(resbldgs_09$minor, 4, pad = 0)
condo_complex_09$major <- str_pad(condo_complex_09$major, 6, pad = 0)
condo_complex_09$minor<- "0000"
condo_units_09$major <- str_pad(condo_units_09$major, 6, pad = 0)
condo_units_09$minor <- str_pad(condo_units_09$minor, 4, pad = 0)
comm_bldg_section_09$major <- str_pad(comm_bldg_section_09$major, 6, pad = 0)
comm_bldg_section_09$minor <- str_pad(comm_bldg_section_09$minor, 4, pad = 0)
parcels_09$major <- str_pad(parcels_09$major, 6, pad = 0)
parcels_09$minor <- str_pad(parcels_09$minor, 4, pad = 0)

# Deletes existing pins fields because some of the pins are not 10 digits and can't be joined to the parcel shapefile pins

### can remove field in a loop/list combo ----
apts_09 <- select(apts_09,-pin)
resbldgs_09 <- select(resbldgs_09,-pin)
condo_complex_09 <- select(condo_complex_09,-pin)
condo_units_09 <- select(condo_units_09,-pin)
comm_bldg_section_09 <- select(comm_bldg_section_09,-pin)
parcels_09 <- select(parcels_09,-pin)

# Concatenate major/minors fields into new 'pin' field

### create new field in loop (see above) ----
parcels$pin <- paste(parcels$major, parcels$minor, sep="")
resbldgs$pin <- paste(resbldgs$major, resbldgs$minor, sep="")
condo_units$pin <- paste(condo_units$major, condo_units$minor, sep="")
condo_complex$pin <- paste(condo_complex$major, condo_complex$minor, sep="")
apts$pin <- paste(apts$major, apts$minor, sep="")
comm_bldg_section$pin <- paste(comm_bldg_section$major, comm_bldg_section$minor, sep="")
comm_bldg$pin <- paste(comm_bldg$major, comm_bldg$minor,sep="")
unit_breakdown$pin <- paste(unit_breakdown$major, unit_breakdown$minor, sep="")

parcels_09$pin <- paste(parcels_09$major, parcels_09$minor, sep="")
resbldgs_09$pin <- paste(resbldgs_09$major, resbldgs_09$minor, sep="")
condo_units_09$pin <- paste(condo_units_09$major, condo_units_09$minor, sep="")
condo_complex_09$pin <- paste(condo_complex_09$major, condo_complex_09$minor, sep="")
apts_09$pin <- paste(apts_09$major, apts_09$minor, sep="")
comm_bldg_section_09$pin <- paste(comm_bldg_section_09$major, comm_bldg_section_09$minor, sep="")

# Select residential buildings built 2010 and after

### use consistent symbology: <- instead of = when assigning----

resbldgs <-filter(resbldgs, yrbuilt >= 2010)
resbldgs$table = "resbldg"
apts <-filter(apts, yrbuilt >= 2010)
apts$table = "apts"
condo_complex <-filter(condo_complex, yrbuilt >= 2010)
condo_complex$table = "condo complex"

# Change column name in ResBldg table to be consistent with apt/condo tables
resbldgs$nbrunits <- resbldgs$nbrlivingunits

# Summarize residential/commercial buildings by Parcel to remove duplicate pins
resbldgs <- resbldgs%>%
  group_by(pin)%>%
  summarise(nbrunits=sum(nbrunits),nbrbldgs=NROW(pin),yrbuilt=min(yrbuilt),yrrenovated=min(yrrenovated),address=first(address),zipcode=first(zipcode),table="resbldg")

comm_bldg <- comm_bldg%>%
  group_by(pin)%>%
  summarise(nbrbldgs=NROW(pin),yrbuilt=min(yrbuilt),address=first(address),zipcode=first(zipcode),Table="Commercial")

# Remove commercial-only condo complexes and denotes any complex type that is a mobile home or floating home
condo_complex <- filter(condo_complex,!complextype %in% c(3,8)) %>%
                 mutate(mh_fm=case_when(complextype %in% c(6,7)~'yes'))


## Looks at the "Use" field in the commercial building section table to classify apartment complexes as 'residential','group quarters', or 'other'

# Filter commercial building data frame for two columns
sec_use <- comm_bldg_section %>% 
  select(pin, sectionuse)

# Join section use info to apts data frame
sec_use_apts <- left_join(apts, sec_use, by = 'pin')

# Define section uses by group
res <- c(300,984,352,348,596,587,351)
gq <- c(982,321,324,424,451,710,589,551,985,782,784,783)

# Create 'unit_category' column and assign numeric values to residential and qroup quarter types
df_code <- sec_use_apts %>% 
  select(pin, complexdescr, sectionuse) %>% 
  mutate(unit_category = case_when(sectionuse %in% res ~ 'Residential',
                                   sectionuse %in% gq ~ 'Group Quarters',
                                   TRUE ~ 'Other')) %>% 
  mutate(unit_bin = case_when(unit_category == 'Residential' ~ 100,
                              unit_category == 'Group Quarters' ~ 1))

# Group by pin and complex name and sum 'unit_bin' category
df_cat <- df_code %>% 
  drop_na(unit_bin) %>% 
  group_by(pin, complexdescr) %>% 
  summarise(sum_unit_bin = sum(unit_bin))

# Use summarised 'unit_bin' values to determine final apt complex classifications
apts <- apts %>% 
  left_join(df_cat, by = c('pin', 'complexdescr')) %>% 
  mutate(complex_category = case_when(sum_unit_bin >= 100 ~ 'Residential',
                                      sum_unit_bin < 100 ~ 'Group Quarters',
                                      is.na(sum_unit_bin) ~ 'Other'))


# Present Use codes found in look up table
present_use <- filter(look_up,lutype==102)

# Merges residential records into a single table, selects key fields, and removed group quarter/non-res complexes
current_res_merge <- merge(resbldgs, apts, all=TRUE) %>%
  merge(condo_complex, all=TRUE) %>%
  left_join(parcels, by ='pin') %>%
  left_join(present_use, by = c('presentuse' = 'luitem')) %>%
  select(pin, yrbuilt,nbrbldgs, nbrunits, complex_category, address, zipcode, table, districtname, ludescription) %>%
  filter(!complex_category %in% c('Group Quarters','Other')) %>%
  filter(nbrunits >0)


-----------------------------------------------------------------------------------------------------------------------------------------------------------
#2009 Base Year Processing
  

# Add column specifying the table source
resbldgs_09$table = "resbldg"
apts_09$table = "apts"
condo_complex_09$table = "condo complex"

# Summarize condo complex buildings by Parcel to remove duplicate pins
condo_complex_09 <- condo_complex_09 %>%
  group_by(pin) %>%
  summarise(nbrunits=sum(nbrunits),nbrbldgs=sum(nbrbldgs),yrbuilt=max(yrbuilt),complex_type=first(complextype),address=first(situsaddress),zipcode=first(zipcode),table=first(table))

# Summarize residential buildings by Parcel to remove duplicate pins
resbldgs_09 <- resbldgs_09 %>%
  group_by(pin) %>%
  summarise(nbrunits=sum(nbrlivingunits),nbrbldgs=NROW(pin),yrbuilt=max(yrbuilt),address=first(situsaddress),zipcode=first(zipcode),table=first(table))

# Remove commercial-only condo complexes and denotes any complex type that is a mobile home or floating home
condo_complex_09 <- filter(condo_complex_09,!complex_type %in% c(3,8)) %>% 
  mutate(MH_FH=case_when(complex_type %in% c(6,7)~'Yes'))

## Looks at the "Use" field in the commercial building section table to classify apartment complexes as 'residential'or 'group quarters'

# filter commercial building data frame for two columns
sec_use_09 <- comm_bldg_section_09 %>% 
  select(pin, sectionuse)

# create joined data frame
sec_use_apts_09 <- left_join(apts_09, sec_use_09, by = 'pin')

# section uses by group
res_09 <- c(300,348,352,551)
gq_09 <- c(321,323,324,335,424,451)

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
apts_09 <- apts_09 %>% 
  left_join(df_cat_09, by = c('pin', 'description')) %>% 
  mutate(complex_category = case_when(sum_unit_bin >= 100 ~ 'Residential',
                                      sum_unit_bin < 100 ~ 'Group Quarters',
                                      is.na(sum_unit_bin) ~ 'Residential'))

# Filter out un-needed columns from apt complex table and change names to be consistent with other tables
apts_09 <- apts_09 %>%
  summarise(pin=pin,nbrbldgs=numbldgs,yrbuilt=yrbuilt,nbrunits=numunits,address=situsaddress,table=table,complex_category=complex_category)

# Present Use codes found in look up table
present_use_09 <- filter(look_up_09,lutype==102)

# Changes 'present use' field to integer to match 'luitem' field for join
parcels_09$presentuse <- as.integer(parcels_09$presentuse)

# Merges resbldg/apt/condo records and removes pins in apts table that are also found in ResBldg table (ResBldg pins were built more recently)
base_res_merge <- merge(resbldgs_09,apts_09,all=TRUE) %>%
  merge(condo_complex_09,all=TRUE) %>%
  arrange(pin,desc(table)) %>%
  filter(duplicated(pin) == FALSE) %>%
  left_join(parcels_09, by ='pin') %>%
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

pin_translation$pin_2009 <- as.character(pin_translation$pin_2009)
pin_translation$pin <- as.character(pin_translation$pin)

# Joins GIS pin translation table to the current records
current_res_merge <- current_res_merge %>%
  left_join(select(pin_translation,pin,pin_2009),by=c("pin"))

# Populates the remaining 2009 pin column NAs with current year pins
current_res_merge$pin_2009[is.na(current_res_merge$pin_2009)] <- current_res_merge$pin[is.na(current_res_merge$pin_2009)]

# Updates the Join_Method column to specify which records were assigned 2009 pins from GIS spatial join method
current_res_merge$join_method[is.na(current_res_merge$join_method)] <- "spatial join"

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

# Adds city/tract fields and filters for relevant years in time series

### can sub long vector with 2010:2019 ----
base_current_join <- base_current_join %>%
  left_join(city_tract,by='pin') %>%
  filter(yrbuilt %in% c(2010,2011,2012,2013,2014,2015,2016,2017,2018,2019)) %>%
  mutate(juris = ifelse(is.na(juris),'Z-Missing',juris))

# Creates a demolitions table that removes the duplication found in the joined current-base table
demos <- base_current_join %>%
  filter(demolition==1) %>%
  distinct(pin_2009,.keep_all=TRUE)

## Creates functions to summarize the net change estimates by county total, jurisdiction, and census tract

# These functions format the final tables and any missing cities/tracts that don't have data
format_total <- function(x) {
  x %>%
    relocate(net_total, .after = yrbuilt)
}

format_cities <- function(x) {
  x %>%
    full_join(full_city_list,by='juris') %>%
    replace(is.na(.), 0) %>%
    relocate(net_total, .after = juris) %>%
    arrange(juris)
}

format_tracts <- function(x) {
  x %>%
    full_join(full_tract_list,by='geoid10') %>%
    replace(is.na(.), 0) %>%
    relocate(net_total, .after = geoid10) %>%
    arrange(geoid10)
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
                           levels = c("single family attached", "single family detached", "multifamily 2-4 units", "multifamily 5-9 units", "multifamily 10-19 units", "multifamily 20-49 units", "multifamily 50+ units", "mobile homes"))) %>% 
  pivot_wider(id_cols = c(yrbuilt),
              names_from = str_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !yrbuilt), na.rm=TRUE)) %>%
  split(.,.$yrbuilt) %>%
  lapply(format_total)

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
                           levels = c("single family attached", "single family detached", "multifamily 2-4 units", "multifamily 5-9 units", "multifamily 10-19 units", "multifamily 20-49 units", "multifamily 50+ units", "mobile homes"))) %>% 
  pivot_wider(id_cols = c(yrbuilt,juris),
              names_from = str_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !yrbuilt), na.rm=TRUE)) %>%
  split(.,.$yrbuilt) %>%
  lapply(format_cities)

#tract
new_tract <- base_current_join %>%
  group_by(geoid10,structure_type,yrbuilt) %>%
  rename(str_type=structure_type) %>%
  summarise(new_units=sum(nbrunits))

lost_tract <- demos %>%
  group_by(geoid10,structure_type_09,yrbuilt) %>%
  rename(str_type=structure_type_09) %>%
  summarise(lost_units=sum(demo_units))

tract_net_summary <- full_join(new_tract, lost_tract, by = join_by("geoid10","yrbuilt", "str_type")) %>%
  replace_na(list(new_units = 0, lost_units = 0)) %>%
  mutate(net_units = new_units + lost_units,
         str_type = factor(str_type,
                           levels = c("single family attached", "single family detached", "multifamily 2-4 units", "multifamily 5-9 units", "multifamily 10-19 units", "multifamily 20-49 units", "multifamily 50+ units", "mobile homes"))) %>% 
  pivot_wider(id_cols = c(yrbuilt,geoid10),
              names_from = str_type,
              names_sort = TRUE,
              values_from = net_units,
              values_fill = 0) %>% 
  mutate(net_total = rowSums(across(where(is.numeric) & !yrbuilt), na.rm=TRUE)) %>%
  split(.,.$yrbuilt) %>%
  lapply(format_tracts)

# Creates final excel spreadsheets 
write_xlsx(total_net_summary,file.path(outputs_data_dir,"totals.xlsx"))
write_xlsx(city_net_summary,file.path(outputs_data_dir,"city.xlsx"))
write_xlsx(tract_net_summary,file.path(outputs_data_dir,"tract.xlsx"))

## 'Z-Missing' represents totals for PIN records not found in the parcel shapefile so they were not assigned a city or tract. These numbers will have to be manually assigned in the final spreadsheet
