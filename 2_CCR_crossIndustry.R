########### Scripts for using CCR module across public data----
########### https://github.com/Ali-Omrani/CCR
library(tidyverse)

########### Get event data ----

# rm(nar_df)
for (sys in c('asrs','nrc','rail','phmsa')) {
  table <- paste0(sys,'_raw')
  print(table)
  q <- glue::glue_sql(
    .con = con,
    "SELECT eid, event_text FROM {`table`};")
  if (!exists('nar_df')) {
    nar_df <- DBI::dbGetQuery(conn = con, q) |>
      mutate(sys_source = sys)
  } else {
    nar_df <- bind_rows(
      nar_df,
      DBI::dbGetQuery(conn = con, q) |>
        mutate(sys_source = sys)
    )
  }
}
nar_df <- nar_df |>
  distinct(event_text, .keep_all = TRUE)

########### Get survey items ----
climate_items <- readxl::read_excel(
  here::here(config$survey_item_path,config$survey_items), 
  sheet = 'items') %>%
  filter(scale != 'JHH_2015_survey') %>%
  mutate(
    num = seq_len(nrow(.)),
    sim_item = glue::glue('sim_item_{num}')) 
########### Get event loadings ----
CCR::ccr_setup()
ccr_loadings <- CCR::ccr_wrapper(nar_df,'event_text',climate_items,'item')
# write_csv(ccr_loadings, 'ccr_loadings_crossIndustry.csv')
# ccr_loadings <- read_csv('ccr_loadings_crossIndustry.csv')

########### Make subscales ----
get_sim_items <- function(var_stem, df) {
  df |> filter(str_detect(domain_abbrev, var_stem)) |> select(sim_item) |> deframe()
}

for (subscale in unlist(unique(climate_items$domain_abbrev))) {
  ccr_loadings <- ccr_loadings |>
    mutate(
      "{subscale}_sim" := rowSums(pick(all_of(get_sim_items(var_stem = subscale, df = climate_items))))
    )
}
########### Visualize ----
cor_mat <- ccr_loadings |>
  select(ends_with('_sim')) |>
  drop_na() |>
  cor()
corrplot::corrplot(cor_mat,method = 'number')

ccr_loadings |>
  select(ends_with('_sim'), sys_source) |>
  drop_na() |>
  group_by(sys_source) |>
  summarize(
    across(everything(), mean)
  ) |>
  pivot_longer(cols = ends_with('_sim'),names_to = 'var') |>
  ggplot(aes(fill = sys_source, y = value, x = var)) + 
  geom_bar(position="dodge", stat="identity")

########### ASRS ----
# get data
asrs_df <- ccr_loadings |>
  filter(sys_source == 'asrs') |>
  select(-sys_source,-starts_with("sim_item_"),-event_text) |>
  left_join(
    DBI::dbGetQuery(conn = con, "SELECT eid,event_date FROM asrs_raw;"),
    by = 'eid'
  ) |>
  mutate(
    # event_date = mdy(event_date),
    year = year(event_date),
    month = floor_date(event_date, unit = 'month')
  ) 
# trends over time
asrs_df |>
  pivot_longer(cols = ends_with("_sim"), names_to = 'var') |>
  group_by(var, month) |>
  summarise(value = mean(value)) |>
  ungroup() |>
  ggplot(aes(x = month, y = value)) +
  # geom_line(aes(color = var, linetype = var))
  geom_line() +
  facet_wrap(~var) + 
  geom_smooth(method='lm')
########### NRC ----
# get data
phmsa_df <- ccr_loadings |>
  filter(sys_source == 'phmsa') |>
  select(-sys_source,-starts_with("sim_item_"),-event_text) |>
  left_join(
    DBI::dbGetQuery(conn = con, "SELECT eid,carrier,event_date FROM phmsa_raw;"),
    by = 'eid'
  ) |>
  mutate(
    event_date = mdy(event_date),
    year = year(event_date),
    month = floor_date(event_date, unit = 'month')
  ) 
# trends over time
phmsa_df |>
  pivot_longer(cols = ends_with("_sim"), names_to = 'var') |>
  group_by(var, month) |>
  summarise(value = mean(value)) |>
  ungroup() |>
  ggplot(aes(x = month, y = value)) +
  # geom_line(aes(color = var, linetype = var))
  geom_line() +
  facet_wrap(~var) + 
  geom_smooth(method='lm')
# differences by facility
# ICC's 
org_var <- 'carrier'
for (var in Filter(function(x)grepl('*_sim',x), names(phmsa_df))) {
  f <- as.formula(paste(var,'~ ',org_var))
  ICC1 <- multilevel::ICC1(aov(f,data=phmsa_df))
  print(paste0(var,' by ',org_var,': ',ICC1))
}
# association with event severity
########### NRC ----
# get data
nrc_df <- ccr_loadings |>
  filter(sys_source == 'nrc') |>
  select(-sys_source,-starts_with("sim_item_"),-event_text) |>
  left_join(
    DBI::dbGetQuery(conn = con, "SELECT eid,facility,event_date FROM nrc_raw;"),
    by = 'eid'
  ) |>
  mutate(
    event_date = mdy(event_date),
    year = year(event_date),
    month = floor_date(event_date, unit = 'month')
  ) |>
  filter(event_date >= mdy('01-01-1999'))
# trends over time
nrc_df |>
  pivot_longer(cols = ends_with("_sim"), names_to = 'var') |>
  group_by(var, month) |>
  summarise(value = mean(value)) |>
  ungroup() |>
  ggplot(aes(x = month, y = value)) +
  # geom_line(aes(color = var, linetype = var))
  geom_line() +
  facet_wrap(~var) + 
  geom_smooth(method='lm')
# differences by facility
# ICC's 
org_var <- 'facility'
for (var in Filter(function(x)grepl('*_sim',x), names(nrc_df))) {
  f <- as.formula(paste(var,'~ ',org_var))
  ICC1 <- multilevel::ICC1(aov(f,data=nrc_df))
  print(paste0(var,' by ',org_var,': ',ICC1))
}
# association with event severity
########### RAIL ----
# get data
rail_df <- ccr_loadings |>
  filter(sys_source == 'rail') |>
  select(-sys_source,-starts_with("sim_item_"),-event_text) |>
  left_join(
    DBI::dbGetQuery(conn = con, "SELECT eid,event_date,reporting_railroad_code FROM rail_raw;"),
    by = 'eid'
  ) |>
  mutate(
    event_date = mdy(event_date),
    year = year(event_date),
    month = floor_date(event_date, unit = 'month')
  ) 
# trends over time
rail_df |>
  pivot_longer(cols = ends_with("_sim"), names_to = 'var') |>
  group_by(var, month) |>
  summarise(value = mean(value)) |>
  ungroup() |>
  ggplot(aes(x = month, y = value)) +
  geom_line(aes(color = var, linetype = var))# +
  # geom_line() +
  # facet_wrap(~var) + 
  # geom_smooth(method='lm')
# differences by facility
# ICC's 
org_var <- 'reporting_railroad_code'
for (var in Filter(function(x)grepl('*_sim',x), names(rail_df))) {
  f <- as.formula(paste(var,'~ ',org_var))
  ICC1 <- multilevel::ICC1(aov(f,data=rail_df))
  print(paste0(var,' by ',org_var,': ',ICC1))
}
# association with event severity