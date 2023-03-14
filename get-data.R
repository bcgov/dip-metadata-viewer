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

library(bcdata) #uses dev version of {bcdata} remotes::install_github("bcgov/bcdata")
library(dplyr)
library(purrr)
library(janitor)
library(stringr)
library(tidyr)

## function to concatenate all metadata resources (files) for one bcdc record
concatenate_all_record_resources <- function(record) {
  bcdc_record <- bcdc_get_record(record)
  
  record_title <- bcdc_record %>%
    pluck("title")
  
  resources_df <- bcdc_tidy_resources(bcdc_record)
  
  d <- map2_dfr(resources_df$id,
                resources_df$name,
                ~ if (resources_df[resources_df$id == .x, "ext"] == "json") {
                  bcdc_get_data(bcdc_record, .x, simplifyVector = TRUE) |>
                    data.frame() |>
                    rename_with(~ gsub("fields.", "", .x),
                                .cols = starts_with("fields")) |>
                    select(-any_of(c("missingValues", "constraints"))) |>
                    mutate(bcdc_resource_name = .y,
                           title = record_title)
                  
                } else if (resources_df[resources_df$id == .x, "ext"] == "csv")
                {
                  bcdc_get_data(bcdc_record, .x,
                                col_types = readr::cols(.default = "c")) %>%
                    remove_empty(which = c("rows", "cols")) %>%
                    mutate(bcdc_resource_name = .y,
                           title = record_title)
                }
                else
                  (message("Sorry, this resource will not be read since it is not CSV or JSON-formatted")))
  
  d2 <- d |>
    left_join(resources_df, by = c("bcdc_resource_name" = "name"))
  
  d2
}

## test concatenate_all_record_resources() on one record
# concatenate_all_record_resources("184b49e0-8e60-4e1b-9886-854db71c1a0e") #json
# concatenate_all_record_resources("36ad8c2b-a884-44b6-b846-d230673142f2") #csv
# concatenate_all_record_resources("94b68713-d696-4749-8cc9-bf63ffa09084") #csv+pdf

## data frame of all dip metadata records in the B.C. Data Catalogue
# dip_records_df <- bcdc_list_group_records("data-innovation-program")
dip_records <-
  bcdc_search(organization = "data-innovation-program-dip")

# rm "extras" sub-element as not all elements have it
dip_records_no_extras <-
  lapply(dip_records, function(x)
    x[names(x) != "extras"])

dip_records_df <-
  data.frame(purrr::reduce(dip_records_no_extras, rbind))

dip_records_active <- dip_records_df |>
  filter(!str_detect(name, "superseded")) #remove deprecated records

## grab and concatenate metadata files for each dip record into a list
metadata_by_record <- map(dip_records_active$id,
                          ~ concatenate_all_record_resources(.x)) |>
  setNames(dip_records_active$title)

## save list to /tmp
# dir.create("tmp", showWarnings = FALSE)
# saveRDS(metadata_by_record, "tmp/metadata-list.rds")
# metadata_by_record <- readRDS("tmp/metadata-list.rds")


## Tidy JSON API elements

json_metdata_list <- metadata_by_record |>
  keep( ~ ("json" %in% .$ext))

tidy_json_metadata <- function(x) {
  x |>
    select(any_of(
      c(
        "title",
        "bcdc_resource_name",
        "name",
        "var_class",
        "type",
        "description",
        "url"
      )
    ))
}

## map over json API metadata records
df_metadata_json <- map_dfr(json_metdata_list, ~ tidy_json_metadata(.x))

## Tidy CSV API elements

## Explore metadata column names for each DIP record
map(metadata_by_record, ~ colnames(.x))
metadata_by_record[[1]]
csv <- metadata_by_record[["Metadata for Mental Health Services"]]
json <- metadata_by_record[["Metadata for Child and Youth Mental Health - E02"]]

## Tidying a Few Records/Resources

csv_list <- metadata_by_record |> 
  keep(~("csv" %in% .$ext))



## Metadata for Income Bands - Standard
# duplicate column names that make it difficult to auto tidy
metadata_by_record$`Metadata for Income Bands - Standard` <- 
  metadata_by_record$`Metadata for Income Bands - Standard` %>%
  select(-"Variable Classification\npostal area", 
         -"Variable Classification\nplace | name | geo") 


## Concatenate CSV metadata files into 1 file

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
      file_name, #drop this
      field_name,
      identifier_classification,
      field_description,
      url
    )
}

# tidy_metadata(metadata_by_record[[2]])

## map over metadata records and tidy the dfs
df_metadata <- map_dfr(metadata_by_record, ~ tidy_metadata(.x))


## Collapse the code table rows to 1 per resource (since the attributes are all NA)
code_files <- df_metadata %>%
  filter(is.na(field_name)) %>%
  group_by(bcdc_resource_name) %>% 
  slice(1L)

code_names <- code_files %>% pull("bcdc_resource_name")

df_metadata_reduced <-
  df_metadata  %>%
  filter(!bcdc_resource_name %in% code_names) %>%
  bind_rows(code_files)


## Final tidying step
tidy_metadata <- df_metadata_reduced %>%
  mutate(
    identifier_classification = str_replace(identifier_classification, "�", "-"),
    field_description = str_replace_all(field_description, "�", " ")
  ) %>%
  mutate(
    data_provider = case_when(
      str_detect(bcdc_resource_name, "AG") ~ "Ministry of Attorney General",
      str_detect(bcdc_resource_name, "MOH") ~ "Ministry of Health",
      str_detect(bcdc_resource_name, "Clients_case_metadata") ~ "Ministry of Health",
      str_detect(bcdc_resource_name, "Statscan") ~ "Statistics Canada",
      str_detect(bcdc_resource_name, "IncomeBands") ~ "Statistics Canada",
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
      str_detect(bcdc_resource_name, "MUNI") ~ "Ministry of Municipal Affairs",
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



