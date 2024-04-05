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


## Load libraries
library(bcdata)
library(dplyr)
library(janitor)
library(purrr)
library(stringr)
library(tidyr)


## Function to concatenate all metadata resources (files) for one 
## BCDC record -- only works with DIP CSV files
concatenate_all_record_resources <- function(record) {
  bcdc_record <- bcdc_get_record(record)
  
  record_title <- bcdc_record %>%
    pluck("title")
  
  resources_df <- bcdc_tidy_resources(bcdc_record) |>
    rename("resource_format" = "format")
  
  d <- map2_dfr(resources_df$id,
                resources_df$name,
                ~ if (resources_df[resources_df$id == .x, "ext"] == "csv")
                {
                  bcdc_get_data(bcdc_record, .x,
                                col_types = readr::cols(.default = "c")) %>%
                    remove_empty(which = c("rows", "cols")) %>%
                    mutate(bcdc_resource_name = .y,
                           title = record_title)
                }
                else
                  (message(paste0("Sorry, the resource ", resources_df[resources_df$id == .x, "name"], " will not be read since it is not CSV-formatted"))))
  
  d2 <- d |>
    left_join(resources_df, by = c("bcdc_resource_name" = "name"))
  
  d2
}


## Test concatenate_all_record_resources() on one record
# #https://catalogue.data.gov.bc.ca/dataset/metadata-for-community-living-programs---e03
# concatenate_all_record_resources("659d8cf7-0b60-4944-b93a-0872e2989d4d") #csv
# #https://catalogue.data.gov.bc.ca/dataset/metadata-for-bc-demographic-survey-e01
# concatenate_all_record_resources("381445d7-c58b-452e-bf2b-9ad8c6fac499") #csv+pdf


## Data frame of all DIP metadata records in the BCDC
dip_records_df <- bcdc_list_organization_records("data-innovation-program-dip")


## Grab and concatenate metadata files for each DIP record into a list
metadata_by_record <- map(dip_records_df$id,
                          ~ concatenate_all_record_resources(.x)) |>
  setNames(dip_records_df$title)

## save list to /tmp
dir.create("tmp", showWarnings = FALSE)
saveRDS(metadata_by_record, "tmp/metadata-list.rds")
# metadata_by_record <- readRDS("tmp/metadata-list.rds")


## Check list of records
map(metadata_by_record, ~ colnames(.x))
metadata_by_record[[5]]


## Tidy DIP CSV Resources
tidy_csv_metadata <- function(x) {
  x |>
    filter(!str_detect(bcdc_resource_name, "Codes|Code")) |> 
    select(any_of(
      c(
        "title",
        "bcdc_resource_name",
        "dip_resource_name" = "resources.name",
        "variable" = "resources.schema.fields.name",
        "variable_classification" = "resources.schema.fields.var_class",
        "description" = "resources.schema.fields.description",
        "bcdc_resource_url" = "url"
      )
    ))
}


# map over CSV API metadata records
df_metadata_csv <- map_dfr(metadata_by_record,
                           ~ tidy_csv_metadata(.x))


tidy_metadata <- df_metadata_csv |> 
  mutate(
    variable_classification = str_replace(variable_classification, "�", "-"),
    description = str_replace_all(description, "�", " "),
    bcdc_record_url = str_sub(bcdc_resource_url, 1, 77)
  ) |> 
  select(-bcdc_resource_url,
         -bcdc_resource_name)


## save tidy data frame to /data
dir.create("data", showWarnings = FALSE)
saveRDS(tidy_metadata, "data/tidy-metadata.rds")



## Explore codes ---------------------------------------------------------------

#all code rows except demographic survey
tidy_csv_codes <- function(x) {
  x |>
    filter(str_detect(bcdc_resource_name, "Codes|Code")) |> 
    filter(bcdc_resource_name != "Metadata Code Table for BC Demographic Survey") |> 
  select(any_of(
    c(
      "title",
      "bcdc_resource_name",
      "dip_resource_name" = "resources.name",
      "variable" = "resources.schema.fields.name",
      "variable_classification" = "resources.schema.fields.var_class",
      "description" = "resources.schema.fields.description",
      "variable_code_value" = "resources.schema.fields.constraints.enum",
      "bcdc_resource_url" = "url"
    )
  ))
}

df_codes_csv <- map_dfr(metadata_by_record,
                           ~ tidy_csv_codes(.x))

codes_survey <- metadata_by_record[["Metadata for BC Demographic Survey - E01"]] |> 
  filter(str_detect(bcdc_resource_name, "Codes|Code")) |> 
  mutate("variable_classification" = "99a. Research Content") |> 
  select(any_of(
    c(
      "title",
      "bcdc_resource_name",
      "variable" = "Q",
      "variable_classification",
      "description" = "QC_DESC",
      "variable_code_value" = "Q_CODE",
      "bcdc_resource_url" = "url"
    ))) |> 
  mutate(dip_resource_name = NA)


tidy_code_data <- df_codes_csv |> 
  bind_rows(df_codes_csv) |> 
  mutate(
    variable_classification = str_replace(variable_classification, "�", "-"),
    description = str_replace_all(description, "�", " "),
    bcdc_record_url = str_sub(bcdc_resource_url, 1, 77)
  ) |> 
  select(
    "title",
    "bcdc_resource_name",
    "dip_resource_name",
    "variable",
    "variable_classification",
    "description",
    "variable_code_value",
    "bcdc_record_url"
  )


## save tidy code dataframe to /data
dir.create("data", showWarnings = FALSE)
saveRDS(tidy_code_data, "data/tidy-code-data.rds")

