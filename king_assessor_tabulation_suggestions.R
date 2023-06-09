library(tidyverse)

data_dir <- "J:/Projects/Assessor/assessor_permit/king/data/data_research_phase"
inputs_data_dir <- file.path(data_dir, "script_inputs")
current_data_dir <- file.path(data_dir, "extracts/current")

# names ----
id_files <- list(pin_tran = "PIN_Translation_2021_2009.csv",
                 city_tract = "city_tract.csv",
                 city_list = "full_city_list.csv",
                 tract_list = "full_tract10_list.csv")

bldg_files <- list(apts = "EXTR_AptComplex.csv",
                   resbldg = "EXTR_ResBldg.csv")

# id files ----
id_dfs <- list() # initiate empty list

for(i in id_files) {
  df <- read.csv(file.path(inputs_data_dir, i))
  names(df) <- tolower(names(df)) # set column names to lowercase
  # id_dfs[[i]] <- df # Option 1: store file in list
  id_dfs[[names(which(id_files == i))]] <- df # Option 2: store file in list with names for each element
}

# building files ----
bldg_dfs <- list()

for(b in bldg_files) {
  df <- read.csv(file.path(current_data_dir, b))
  names(df) <- tolower(names(df)) # lowercase columns
  df$major <- str_pad(df$major, 6, pad = 0) # padding
  # can add in exception for condo minor with if/else...
  df$minor <- str_pad(df$minor, 4, pad = 0)
  df$pin <- paste(df$major, df$minor, sep="") # create new pin
  
  bldg_dfs[[names(which(bldg_files == b))]] <- df 
}

# 2010:2019 instead of c(2010, 2011, ..., 2019)