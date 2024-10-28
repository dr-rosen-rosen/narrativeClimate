
########### Scripts for reading and cleaning events ----
########### from asrs, nrc, rail, and phmsa sources

get_and_clean_ASRS <- function(asrs.files) {
  # merges and does minimal cleaning for the raw
  # data files downloaded from ASRS (could not
  # download all files at once)
  dfs <- NULL
  for (f_path in asrs.files) {
    l1 <- readxl::read_xlsx(asrs.files[1],
                            n_max = 0,
                            .name_repair = 'minimal') |> names()
    l2 <- readxl::read_xlsx(asrs.files[1],
                            skip = 1,
                            n_max = 0,
                            .name_repair = 'minimal') |> names()
    if (length(l1) == length(l2)) {
      header <- mapply(FUN = function(x,y) {
        paste0(x,y,sep = '.')}, x = l1,y = l2
      )
    }
    df <- readxl::read_xlsx(f_path,
                            skip = 3,
                            col_names = header,
                            range = readxl::cell_cols("A:DU")) |>
      janitor::clean_names()
    if (is.null(dfs)) {
      dfs <- df
    } else {dfs <- bind_rows(dfs,df)}
  }
  dfs <- dfs |>
    filter(!is.na(acn), acn != 'ACN') |>
    mutate(
      date = as.Date(paste0(substr(time_date,start = 1, stop = 4),'-',substr(time_date,start = 5, stop = 6),'-01'),format = "%Y-%m-%d")
    ) |>
    unite('cmbd_narrative',starts_with('report_'),sep = " ", na.rm = TRUE,remove = FALSE) |>
    mutate(tot_wc = stringr::str_count(cmbd_narrative, '\\w+')) |>
    filter(
      !is.na(cmbd_narrative), 
      cmbd_narrative != '', 
      utf8::utf8_valid(cmbd_narrative))  |>
    group_by(acn) |>
    mutate(r = row_number()) |>
    ungroup() |>
    mutate(
      acn = if_else(r == 1, acn, paste0(acn,'_',r))
    ) |> select(-r)
  
  dfs <- dfs |>
    distinct(cmbd_narrative, .keep_all = TRUE)
  return(dfs)
}

get_and_clean_NRC_events <- function(f) {
  df <- read.csv(f) |>
    mutate(
      event_date = case_match(event_date,
                              '11/20/2103' ~ '11/20/2013',
                              .default = event_date),
      event_date2 = lubridate::mdy(event_date),
      notification_date2 = lubridate::mdy(notification_date),
      event_text = tolower(event_text),
      tot_wc = stringr::str_count(event_text, '\\w+'),
      facility = case_match(tolower(facility),
                            'columbia generating statiregion:' ~ 'columbia generating station',
                            'davis-besse'~ 'davis besse',
                            'washington nuclear (wnp-2region:' ~ 'washington nuclear',
                            'vogtle 1/2' ~ 'vogtle',
                            'vogtle 3/4' ~ 'vogtle',
                            'fort calhoun' ~ 'ft calhoun',
                            'summer construction' ~ 'summer',
                            .default = tolower(facility))  
    ) |>
    select(-X)
  df <- df |>
    distinct(event_text, .keep_all = TRUE)
  return(df)
}

get_and_clean_rail_events <- function(f) {
  df <- read.csv(f) 
  df <- df |>
    mutate(
      Narrative = str_replace_all(Narrative,'NoneNone',''),
      Narrative = str_remove(Narrative,'None$'),
      tot_wc = stringr::str_count(Narrative, '\\w+')
    ) |>
    filter(
      !is.na(Narrative), 
      Narrative != '',
      Narrative != 'None',
      utf8::utf8_valid(Narrative)) |>
    group_by(Accident.Number) |>
    mutate(r = row_number()) |>
    ungroup() |>
    mutate(
      Accident.Number = if_else(r == 1, Accident.Number, paste0(Accident.Number,'_',r))
    ) |> select(-r)
  df <- df |>
    distinct(Narrative, .keep_all = TRUE)
  return(df)
}

get_and_clean_phmsa <- function(f) {
  df <- read.csv(f)
  df <- df |> 
    filter(
      !is.na(cmbd_narrative), 
      cmbd_narrative != '', 
      utf8::utf8_valid(cmbd_narrative))
  df <- df |>
    distinct(cmbd_narrative, .keep_all = TRUE) |>
    mutate(
      tot_wc = stringr::str_count(cmbd_narrative, '\\w+')
    )
  return(df)
}

harmonize_key_vars <- function(df, sys_source) {
  #creates 
  if (sys_source == 'asrs') {
    key <- c(event_date = 'date',event_num = 'acn', event_text = 'cmbd_narrative')#cmbd_narrative = 'event_text')
  } else if (sys_source == 'rail') {
    key <- c(event_date = 'Date',event_num = 'Accident.Number', event_text = 'Narrative')#Narrative = 'event_text')
  } else if (sys_source == 'nrc') {
    key <- c(event_date = 'event_date',event_num = 'event_num')
  } else if (sys_source == 'phmsa') {
    key <- c(event_date = 'date',event_num = 'report_no', event_text = 'cmbd_narrative')#cmbd_narrative = 'event_text')
  } else if (sys_source == 'psn') {
    key <- c(event_date = 'FLR_SUBMIT_DATE',event_num = 'Report_ID', event_text = 'Narrative_merged')
  }
  if (exists('key')) {
    return(
      df |> rename(all_of(key)) |> mutate(event_num = as.character(event_num))
    )
  } else {
    print('No appropriate source provided...')
    return(NULL)
  }
  
}
