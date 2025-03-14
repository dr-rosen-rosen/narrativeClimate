
########### Scripts for creating and managing DBs ----
########### based on narrative safety events
con <- DBI::dbConnect(RPostgres::Postgres(),
                      dbname   = config$db_name,
                      host     = config$db_host,
                      port     = config$dbPort,
                      user     = config$dbUser,
                      password = config$dbPW
                      )

create_link_table <- function(con){
  q <- DBI::sqlInterpolate(
    conn = con, 
    "CREATE TABLE IF NOT EXISTS link_table (eid uuid PRIMARY KEY DEFAULT gen_random_uuid(), system_source text, event_date date, event_num text)")
  DBI::dbExecute(conn = con, q)
}
if (!DBI::dbExistsTable(con,'link_table')) {
  create_link_table(con = con)
}

create_survey_table <- function(con){
  q <- DBI::sqlInterpolate(
    conn = con, 
    "CREATE TABLE IF NOT EXISTS survey_tools (sid uuid PRIMARY KEY DEFAULT gen_random_uuid(), survey text, reference text);")
  DBI::dbExecute(conn = con, q)
}
if (!DBI::dbExistsTable(con,'survey_tools')) {
  create_survey_table(con = con)
}

# make a survey_items table
# to include link t sid, item_id, item (text), item embedding, reverse score, subdimension

insert_new_column <- function(con, table, column_name, column_type, vec_len) {
  print(glue::glue("Attempting to insert {column_name} as {column_type}"))
  column_type <- if_else(is.na(vec_len), column_type, paste0('vector(',vec_len,')'))
  stmt <- glue::glue(
    "ALTER TABLE ?table ADD COLUMN IF NOT EXISTS ?column_name {column_type};")
  q <- DBI::sqlInterpolate(
    conn = con, 
    stmt, 
    table= DBI::dbQuoteIdentifier(con, table), 
    column_name = DBI::dbQuoteIdentifier(con, column_name)
  )
  print(q)
  DBI::dbExecute(conn = con, q)
}

create_raw_table <- function(con,df, table_name, update_values) {
  q <- DBI::sqlInterpolate(
    conn = con,
    "CREATE TABLE IF NOT EXISTS ?table_name (eid uuid PRIMARY KEY, CONSTRAINT fk_eid FOREIGN KEY(eid) REFERENCES link_table(eid));",
    table_name = DBI::dbQuoteIdentifier(con, table_name)
  )
  DBI::dbExecute(conn = con, q)
  for(c in names(df)) {
    if(c != 'eid') {
      print(c)
      print(class(df[,c]))
      insert_new_column(
        con = con,
        table = table_name,
        column_name = c,
        column_type = DBI::dbDataType(con, df[,c]),
        vec_len = NA
      )
    }
  }
  if(update_values) { # need to check existing entries
    DBI::dbWriteTable(
      conn = con, 
      table_name, 
      df, 
      append = TRUE)
  }
}

updateLinkTable <- function(con, df, sys_source, return_eid){
  df <- df |> 
    mutate(
      system_source = sys_source,
      event_num = as.character(event_num))
  DBI::dbWriteTable(
    conn = con, 
    'link_table', 
    df |> select(event_num,event_date, system_source), 
    append = TRUE)
  if(return_eid){
    # pull all eid's for update events
    t <- tbl(con, 'link_table')
    eids <- t  |> collect() |> # this is lazy at it pulls entire talbe, but...
      filter(system_source == !!sys_source) |>
      filter(event_num %in% unique(df$event_num)) |>
      select(eid, event_num)
    df <- df |> 
      left_join(eids, by = 'event_num') |> 
      relocate(eid) |> 
      select(-system_source)
    return(df)
  }
}

get_eids <- function(con, df, sys_source) {
  # pull all eid's for update events
  event_nums <- unique(df$event_num)
  q <- glue::glue_sql(
    .con = con,
    "SELECT * FROM link_table as lt WHERE (lt.system_source = {sys_source}) AND (lt.event_num IN ({event_nums*}));")
  eids <- DBI::dbGetQuery(conn = con, q) |>
    select(eid, event_num)
  df <- df |> 
    left_join(eids, by = 'event_num') |> 
    relocate(eid)
  return(df)
}