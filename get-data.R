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
library(purrr)
library(janitor)
library(stringr)
library(tidyr)


## Function to concatenate all metadata resources (files) for one 
## BCDC record -- only works with DIP CSV or DIP JSON files
concatenate_all_record_resources <- function(record) {
  bcdc_record <- bcdc_get_record(record) 
  
  record_title <- bcdc_record %>%
    pluck("title")
  
  resources_df <- bcdc_tidy_resources(bcdc_record) |> 
    rename("resource_format" = "format")
  
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

## Test concatenate_all_record_resources() on one record
# concatenate_all_record_resources("184b49e0-8e60-4e1b-9886-854db71c1a0e") #json
# concatenate_all_record_resources("36ad8c2b-a884-44b6-b846-d230673142f2") #csv
# concatenate_all_record_resources("94b68713-d696-4749-8cc9-bf63ffa09084") #csv+pdf


## Data frame of all DIP metadata records in the BCDC
dip_records_df <- bcdc_list_organization_records("data-innovation-program-dip")

dip_records_active <- dip_records_df |>
  filter(!str_detect(name, "superseded")) #remove deprecated records

## Grab and concatenate metadata files for each DIP record into a list
metadata_by_record <- map(dip_records_active$id,
                          ~ concatenate_all_record_resources(.x)) |>
  setNames(dip_records_active$title)

## save list to /tmp
# dir.create("tmp", showWarnings = FALSE)
# saveRDS(metadata_by_record, "tmp/metadata-list.rds")
# metadata_by_record <- readRDS("tmp/metadata-list.rds")

# map(metadata_by_record, ~ colnames(.x))
# metadata_by_record[[1]]


## Tidy dip JSON API resources
json_metadata_list <- metadata_by_record |>
  keep( ~ ("json" %in% .$ext))

map(json_metadata_list, ~ colnames(.x))
# json_metadata_list[[1]]

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

# map over JSON API metadata records
df_metadata_json <- map_dfr(json_metadata_list,
                            ~ tidy_json_metadata(.x))


## Tidy DIP CSV API resources
csv_metadata_list <- metadata_by_record |> 
  keep(~("csv" %in% .$ext))

map(csv_metadata_list, ~ colnames(.x))

tidy_csv_metadata <- function(x) {
  x |>
    clean_names() |> 
    select(any_of(
      c(
        "title",
        "bcdc_resource_name",
        "name" = "field_name",
        "var_class" = "variable_classification",
        "var_class" = "identifier_classification",
        "type" = "field_type",
        "description" = "field_description",
        "description" = "field_description_and_notes",
        "url"
      )
    ))
}

# map over CSV API metadata records
df_metadata_csv <- map_dfr(csv_metadata_list,
                            ~ tidy_csv_metadata(.x))

## Collapse the csv "code table" rows to 1 per resource (since the attributes are all NA)
code_files <- df_metadata_csv |> 
  filter(is.na(name)) |> 
  group_by(bcdc_resource_name) |> 
  slice(1L)

code_names <- code_files |>  pull("bcdc_resource_name")

df_metadata_csv_reduced <-
  df_metadata_csv  |> 
  filter(!bcdc_resource_name %in% code_names) |> 
  bind_rows(code_files)


## Join JSON+CSV tables and final tidying
tidy_metadata <- df_metadata_csv_reduced |> 
  bind_rows(df_metadata_json) |> 
  mutate(
    var_class = str_replace(var_class, "�", "-"),
    description = str_replace_all(description, "�", " "),
    bcdc_record_url = str_sub(url, 1, 77)
  ) |> 
  select(
    title,
    bcdc_resource_name,
    name,
    var_class,
    type,
    description,
    bcdc_record_url
  )
  
## save tidy data frame to /data
dir.create("data", showWarnings = FALSE)
saveRDS(tidy_metadata, "data/tidy-metadata.rds")

