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
               mutate(csv_filename = .y))
df
}

## test concatenate_all_record_resources()
# concatenate_all_record_resources("5b5c8ce1-debb-4ec9-b46d-7838a4f61477")


## grab concatenated metadata files for each dip record into a list
metadata_by_record <- map(dip_records_df$id,
                          ~ concatenate_all_record_resources(.x))  %>%
  setNames(dip_records_df$title)

## save list to /tmp
# if (!exists("tmp")) dir.create("tmp", showWarnings = FALSE)
# saveRDS(metadata_by_record, "tmp/metadata-list.rds")
# metadata_by_record <- readRDS("tmp/metadata-list.rds")

## concatenate metadata files into 1 file for each dip record
map(metadata_by_record, ~ colnames(.x))

metadata_by_record[[7]] %>%
  clean_names() %>% 
  select()


  


