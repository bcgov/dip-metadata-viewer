# Copyright 2021 Province of British Columbia
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and limitations under the License.


library(bcdata)
library(dplyr)
library(purrr)
library(janitor)


#dataframe of dip metadata records in the B.C. Data Catalogue
dip_records_df <- bcdc_list_group_records("data-innovation-program") 


## function to concatenate all metadata resources (files) for one record 
concatenate_all_record_resources <- function(record){
  
resources_df <- bcdc_tidy_resources(record) %>% 
  filter(format == "csv")

df <- map2_dfr(resources_df$id, 
               resources_df$name,
              ~bcdc_get_data(record, .x,
                             col_types = readr::cols(.default = "c")) %>% 
               mutate(bcdc_resource_name = .y))
df
}

## test concatenate_all_record_resources()
# concatenate_all_record_resources("3849cd71-3c04-4eea-a6f1-fb0012cde168")


## grab concatenated metadata files for each dip record into a list
metadata_by_record <- map(dip_records_df$id,
                          ~ concatenate_all_record_resources(.x))  %>%
  setNames(dip_records_df$title)


## save list to /tmp
# if (!exists("tmp")) dir.create("tmp", showWarnings = FALSE)
# saveRDS(metadata_by_record, "tmp/metadata-list.rds")
# metadata_by_record <- readRDS("tmp/metadata-list.rds")


## Explore and tidy some of the metadata
map(metadata_by_record, ~ colnames(.x))

#temp clean-up of Metadata for Health Medical Services Plan (MSP) Payment Information File
metadata_by_record$`Metadata for Health Medical Services Plan (MSP) Payment Information File` <- 
  metadata_by_record$`Metadata for Health Medical Services Plan (MSP) Payment Information File` %>%
  rename(`File Name` = `MANDATORY MINIMUM FIELD METADATA COMPONENTS`,
         `Field Name` = X2, 
         `Identifier Classification` = X3,  
         `Field Description` = `ADDITIONAL INFORMATION`) %>% 
  slice(-1)
  
#temp clean-up of Metadata for Income Bands by Postal Code                  
metadata_by_record$`Metadata for Income Bands by Postal Code` <- 
  metadata_by_record$`Metadata for Income Bands by Postal Code` %>%
  select(-"Variable Classification\npostal area", 
         -"Variable Classification\nplace | name | geo")

               
## concatenate metadata files into 1 file
#functions to rename to one column name design
tidy_classification <- function(x){
  names(x)[names(x)=="variable_classification"] <- "identifier_classification"
}

tidy_description <- function(x){
  names(x)[names(x)=="field_description_and_notes"] <- "field_description"
  }
  

#function to tidy the dfs
tidy_metadata <- function(x){
   
  x %>% 
    clean_names() %>%
    rename_with(tidy_classification, .cols = matches("variable_classification")) %>% 
    rename_with(tidy_description, .cols = matches("field_description_and_notes")) %>% 
    select(
      bcdc_resource_name,
      file_name,
      field_name,
      identifier_classification,
      field_description
    )
}


df_metadata <- map_dfr(metadata_by_record, ~ tidy_metadata(.x))


## save df to /data
# if (!dir.exists("data")) dir.create("data", showWarnings = FALSE)
# saveRDS(df_metadata, "data/df-metadata.rds")
# df_metadata <- readRDS("data/df-metadata.rds")


