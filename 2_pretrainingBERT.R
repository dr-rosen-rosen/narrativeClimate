########### Scripts for pre-training BERT ----
########### with safety event narratives

########### Pull and pre-process event text ----

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

write.csv(nar_df,'eid_eventText.csv')

nar_df <- nar_df |> 
  mutate(dupe = duplicated(event_text))