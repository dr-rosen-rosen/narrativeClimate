########### Scripts for using CCR module ----
########### https://github.com/Ali-Omrani/CCR
library(tidyverse)
# install.packages("devtools")
# devtools::install_github("tomzhang255/CCR")

########### Prep PSN data ----

# event             
events_df <- read.csv('/Volumes/calculon/event_reporting/psn/old_psn.csv') |>
  mutate(
    date = lubridate::mdy(FLR_SUBMIT_DATE)
  )
# survey items
climate_items <- read.csv('climateItems.csv') %>%
  mutate(
    num = seq_len(nrow(.)),
    sim_item = glue::glue('sim_item_{num}')) 
# unit-level survey data
climate_scores <- readxl::read_excel(
  '/Volumes/calculon/event_reporting/old_culture_data/culture\ Data\ Set\ From\ 05-13-13\ CROSS\ MAPPED.xlsx',
  sheet = 'FINAL')

get_sim_items <- function(var_stem, df) {
  climate_items |> filter(str_detect(var, var_stem)) |> select(sim_item) |> deframe()
}
########### Get similarities ----
CCR::ccr_setup()
ccr_loadings <- CCR::ccr_wrapper(events_df,'Narrative_merged',climate_items,'item')
# write.csv(ccr_loadings,'ccr_loadings.csv')
ccr_event_scores <- ccr_loadings |>
  select(PRIMARY_LOC_NAME, HARM_SCORE, starts_with('sim_item_')) |>
  mutate(
    TC_sim = rowSums(pick(all_of(get_sim_items(var_stem = "TC", df = climate_items)))),
    SC_sim = rowSums(pick(all_of(get_sim_items(var_stem = "SC", df = climate_items)))),
    PSM_sim = rowSums(pick(all_of(get_sim_items(var_stem = "PSM", df = climate_items)))),
    PLM_sim = rowSums(pick(all_of(get_sim_items(var_stem = "PLM", df = climate_items))))
  ) 

ccr_unit_scores <- ccr_event_scores |>
  group_by(PRIMARY_LOC_NAME) |>
  summarize(across(everything(),~mean(.x))) |>
  ungroup()
########### self-report (event-level) by climate ----
cmbd_event_df <- ccr_event_scores |>
  left_join(climate_scores, by = c("PRIMARY_LOC_NAME" = "primary_loc_name")) |>
  drop_na(TC)

unique(cmbd_event_df$PRIMARY_LOC_NAME)
names(cmbd_event_df)

skimr::skim(cmbd_event_df)
  mutate(
    PRIMARY_LOC_NAME = factor(PRIMARY_LOC_NAME),
    TC_z = datawizard::standardise(TC),
    SC_z = datawizard::standardise(SC)
  )
psych::ICC()
m.1 <- lme4::lmer(HARM_SCORE ~ 1 + 
                    TC_sim +
                    SC_sim +
                    PSM_sim +
                    PLM_sim +
                    # TC +
                    # SC +
                    # PSM +
                    # PLM +
                    (1|PRIMARY_LOC_NAME), 
                  data = cmbd_event_df,
                  control = lme4::lmerControl(optimizer = 'bobyqa'))

m.2 <- lme4::lmer(PLM_sim ~ 1 + (1|PRIMARY_LOC_NAME), 
                  data = cmbd_event_df,
                  control = lme4::lmerControl(optimizer = 'bobyqa'))
sjPlot::tab_model(m.1)
y.1 <- lm(HARM_SCORE ~ 1 + 
            TC_sim + 
            SC_sim + 
            PSM_sim +
            PLM_sim, 
          data = cmbd_event_df)
sjPlot::tab_model(y.1)
lme4::allFit(m.1)

########### self-report (unit-level) by climate ----
var_to_sim_item <- setNames(climate_items$var,climate_items$sim_item)
cmb_df <- climate_scores |>
  left_join(ccr_unit_scores, by = c('primary_loc_name' = 'PRIMARY_LOC_NAME'))

y.1 <- lm(PLM ~ 1 +
            sim_item_1 +
            sim_item_2 +
            sim_item_3 +
            sim_item_4 +
            sim_item_5 +
            sim_item_6 +
            sim_item_7 +
            sim_item_8 +
            sim_item_9 +
            sim_item_10 +
            sim_item_11 +
            sim_item_12 +
            sim_item_13 +
            sim_item_14 +
            sim_item_15 +
            sim_item_16 +
            sim_item_17 +
            sim_item_18 +
            sim_item_19 +
            sim_item_20
            , data = cmb_df)
sjPlot::tab_model(y.1)

cor_mat <- cmb_df |>
  select(
    all_of(unique(climate_items$var)),
    # starts_with("sim_item"),
    # all_of(c("TC","SC","PSM","PLM")),
    all_of(c("TC_sim","SC_sim","PSM_sim","PLM_sim"))
    ) |>
  drop_na() |>
  cor()

corrplot::corrplot(cor_mat, method = 'number')

for(name in names(var_to_sim_item)) {
  #print(name)
  #print(var_to_sim_item[name])
  cmb_df |>
    ggplot(aes(x = !!sym(name), y = !!sym(var_to_sim_item[name]))) +
    geom_point() + ggthemes::theme_base() + 
    geom_smooth(method="auto", se=TRUE, fullrange=FALSE, level=0.95)
  ggsave(glue::glue('{name}.png'))
}

gamlss::fitDist(cmb_df$SC_sim, type ="realAll")
