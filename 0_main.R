########### Scripts for developing local climate measures ----
########### based on narrative safety events
rm(list = ls())
beepr::beep()
library(tidyverse)
library(here)
Sys.setenv(R_CONFIG_ACTIVE = 'calculon')
config <- config::get()
# source('1_funcs.R')
########### Read in raw events ----
########### cleaning is different by source
source('1b_buildDatabases.R')
source('1b_injestEvents.R')
# NRC ====

nrc_df <- get_and_clean_NRC_events(
  f = here(config$nrc_data_path,'event_reports_nrc.csv')) |> # original data pull
  bind_rows(get_and_clean_NRC_events(
              f = here(config$nrc_data_path,'new_event_reports_nrc.csv'))) |> # second data pull
  distinct() |>
  harmonize_key_vars(
    sys_source = 'nrc'
  ) |> janitor::clean_names()

# PHMSA ====
phmsa_df <- get_and_clean_phmsa( # only one data pull for this (behind a log in ID now)
  f = here(config$phmsa_data_path,'phmsa_reports_combined_03-11-2024.csv')) |>
  distinct() |>
  harmonize_key_vars(
    sys_source = 'phmsa'
  ) |> janitor::clean_names()

# Rail ====
rail_df <- get_and_clean_rail_events( # all data in one pull
  f = here(config$rail_data_path,'Rail_Equipment_Accident_Incident_Data__Form_54__20241018.csv')
) |>
  distinct() |>
  harmonize_key_vars(
    sys_source = 'rail'
  ) |> janitor::clean_names()

# ASRS ====
asrs_df <- get_and_clean_ASRS(
  asrs.files = 
    list.files(path = config$asrs_data_path,pattern = '.xlsx',full.names = TRUE)
) |>
  distinct() |>
  harmonize_key_vars(
    sys_source = 'asrs'
  ) |> janitor::clean_names()

########### Store raw events ----
########### raw table is unique for each source

# NRC ====
nrc_df <- updateLinkTable(
  con = con,
  df = nrc_df,
  sys_source = 'nrc',
  return_eid = TRUE
)
create_raw_table(
  con = con,
  df = nrc_df[,1:23],
  table_name = "nrc_raw",
  update_values = TRUE)

# PHMSA ====
phmsa_df <- updateLinkTable(
  con = con,
  df = phmsa_df,
  sys_source = 'phmsa',
  return_eid = TRUE
)
create_raw_table(
  con = con,
  df = phmsa_df[,1:14], # update end point
  table_name = "phmsa_raw",
  update_values = TRUE)
# Rail ====
rail_df <- updateLinkTable(
  con = con,
  df = rail_df,
  sys_source = 'rail',
  return_eid = TRUE
)
create_raw_table(
  con = con,
  df = rail_df[,1:162], # update end point
  table_name = "rail_raw",
  update_values = TRUE)

# ASRS ====
asrs_df <- updateLinkTable(
  con = con,
  df = asrs_df,
  sys_source = 'asrs',
  return_eid = TRUE
)
create_raw_table(
  con = con,
  df = asrs_df[,1:129], # update end point
  table_name = "asrs_raw",
  update_values = TRUE)

