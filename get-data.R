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
library(stringr)


## function to concatenate all metadata resources (files) for one bcdc record 
concatenate_all_record_resources <- function(record){
  
record_title <- bcdc_get_record(record) %>%
  pluck("title")
  
resources_df <- bcdc_tidy_resources(record) %>% 
  filter(format == "csv")

d <- map2_dfr(resources_df$id, 
               resources_df$name,
              ~bcdc_get_data(record, .x,
                             col_types = readr::cols(.default = "c")) %>% 
               remove_empty(which = c("rows", "cols")) %>% 
               mutate(bcdc_resource_name = .y,
                      title = record_title))

d2 <- d %>% 
  left_join(resources_df, by = c("bcdc_resource_name" = "name"))

d2
}

## test concatenate_all_record_resources() on one record
# concatenate_all_record_resources("1cfcec36-6252-4177-9bfd-b5820e392ca7")


## data frame of all dip metadata records in the B.C. Data Catalogue
dip_records_df <- bcdc_list_group_records("data-innovation-program")


## grab and concatenate metadata files for each dip record into a list
metadata_by_record <- map(dip_records_df$id,
                          ~ concatenate_all_record_resources(.x))  %>%
  setNames(dip_records_df$title)


## save list to /tmp
# dir.create("tmp", showWarnings = FALSE)
# saveRDS(metadata_by_record, "tmp/metadata-list.rds")
# metadata_by_record <- readRDS("tmp/metadata-list.rds")


## Explore metadata column names for each DIP record
map(metadata_by_record, ~ colnames(.x))
# metadata_by_record[[3]]


## Tidying a Few Records/Resources

## Metadata for Health Medical Services Plan (MSP) Payment Information File
# extra header row in source csv
metadata_by_record$`Metadata for Medical Services Plan (MSP) Payment Information File` <- 
  metadata_by_record$`Metadata for Medical Services Plan (MSP) Payment Information File` %>%
  rename(`File Name` = `MANDATORY MINIMUM FIELD METADATA COMPONENTS`,
         `Field Name` = ...2, 
         `Identifier Classification` = ...3,  
         `Field Description` = `ADDITIONAL INFORMATION`) %>% 
  slice(-1)

## Metadata for Income Bands - Standard
# duplicate column names that make it difficult to auto tidy
metadata_by_record$`Metadata for Income Bands - Standard` <- 
  metadata_by_record$`Metadata for Income Bands - Standard` %>%
  select(-"Variable Classification\npostal area", 
         -"Variable Classification\nplace | name | geo") 

## Metadata for Income Bands - Custom
# duplicate column names that make it difficult to auto tidy
metadata_by_record$`Metadata for Income Bands - Custom` <- 
  metadata_by_record$`Metadata for Income Bands - Custom` %>%
  select(-"Variable Classification\npostal area", 
         -"Variable Classification\nplace | name | geo")

## Metadata for Home and Community Care  
# concatenated resources contain both classification variable names
metadata_by_record$`Metadata for Home and Community Care` <- 
  metadata_by_record$`Metadata for Home and Community Care` %>%
  mutate(`Identifier Classification` = case_when(is.na(`Identifier Classification`) ~ `Variable Classification`,
    TRUE ~ `Identifier Classification`)) %>% 
  select(-`Variable Classification`) 

## Metadata for Health - National Ambulatory Care Reporting System (NACRS)
# unique classification column name
metadata_by_record$`Metadata for Health - National Ambulatory Care Reporting System (NACRS)` <-
  metadata_by_record$`Metadata for Health - National Ambulatory Care Reporting System (NACRS)` %>%
  rename("Identifier Classification" = "V03 Identifier Classification")


## Concatenate metadata files into 1 file

## functions to rename to a consistent-column-name design
tidy_classification <- function(x){
  names(x)[names(x)=="variable_classification"] <- "identifier_classification"
}

tidy_description <- function(x){
  names(x)[names(x)=="field_description_and_notes"] <- "field_description"
  }
  
## function to tidy the dfs
tidy_metadata <- function(x){
   
  x %>% 
    clean_names() %>%
    rename_with(tidy_classification, .cols = matches("variable_classification")) %>% 
    rename_with(tidy_description, .cols = matches("field_description_and_notes")) %>% 
    select(
      title,
      bcdc_resource_name,
      file_name,
      field_name,
      identifier_classification,
      field_description,
      url
    )
}

df_metadata <- map_dfr(metadata_by_record, ~ tidy_metadata(.x))

## Final tidying step
tidy_metadata <- df_metadata %>%
  mutate(
    identifier_classification = str_replace(identifier_classification, "�", "-"),
    field_description = str_replace_all(field_description, "�", " ")
  ) %>%
  mutate(
    data_provider = case_when(
      str_detect(bcdc_resource_name, "MOH") ~ "Ministry of Health",
      str_detect(bcdc_resource_name, "Clients_case_metadata") ~ "Ministry of Health",
      str_detect(bcdc_resource_name, "Statscan") ~ "Statistics Canada",
      # str_detect(bcdc_resource_name, "census geodata") ~ "Statistics Canada,
      str_detect(bcdc_resource_name, "SDPR") ~ "Ministry of Social Development and Poverty Reduction",
      str_detect(bcdc_resource_name, "MED") ~ "Ministry of Education",
      str_detect(bcdc_resource_name, "EDUC") ~ "Ministry of Education",
      str_detect(bcdc_resource_name, "LMID") ~ "Ministry of Advanced Education",
      str_detect(bcdc_resource_name, "MCFD") ~ "Ministry of Children and Family Development",
      str_detect(bcdc_resource_name, "BCHC") ~ "BC Housing",
      str_detect(bcdc_resource_name, "AG") ~ "Attorney General's Office",
      str_detect(bcdc_resource_name, "registration") ~ "Ministry of Health",
      str_detect(bcdc_resource_name, "PHSA") ~ "Ministry of Health",
      str_detect(bcdc_resource_name, "SES") ~ "Ministry of Education",
      str_detect(bcdc_resource_name, "Health") ~ "Ministry of Health",
      str_detect(bcdc_resource_name, "CLBC") ~ "Community Living BC",
      str_detect(bcdc_resource_name, "ICBC") ~ "Insurance Corporation of British Columbia",
      TRUE ~ NA_character_
    ),
    bcdc_record_url = str_sub(url, 1, 77)
  )  %>%
  select(
    data_provider,
    title,
    bcdc_resource_name,
    file_name,
    field_name,
    identifier_classification,
    field_description,
    bcdc_record_url
  )
  
## save tidy data frame to /data
dir.create("data", showWarnings = FALSE)
saveRDS(df_metadata, "data/df-metadata.rds")
saveRDS(tidy_metadata, "data/tidy-metadata.rds")



