print("Script is starting...")


if (!require(librarian)){
  install.packages("librarian")
  library(librarian)
}

# librarian downloads, if not already downloaded, and reads in needed packages
librarian::shelf(tidyverse, here, DBI, odbc)

### Database Connection: Connect to `ENTERPRISE` database using Snowflake
tryCatch({
  db_connection_pg <- DBI::dbConnect(odbc::odbc(),
                                     Driver = "SnowflakeDSIIDriver",
                                     Server = "hawaiianair.west-us-2.azure.snowflakecomputing.com",
                                     WAREHOUSE = "DATA_LAKE_READER",
                                     Database = "PLAYGROUND",
                                     UID = "jacob.eisaguirre@hawaiianair.com",  # Replace Sys.getenv("UID") with your email
                                     authenticator = "externalbrowser")
  print("Database Connected!")  # Success message
}, error = function(cond) {
  print("Unable to connect to Database.")  # Error handling
})

# Set the schema for the session
dbExecute(db_connection_pg, "USE SCHEMA CREW_ANALYTICS")


### Query reserve table

rlv_q <- "select * from AA_RESERVE_UTILIZATION;"

raw_rlv <- dbGetQuery(db_connection_pg, rlv_q)

clean_rlv <- raw_rlv %>% 
  mutate(PAIRING_POSITION = if_else(PAIRING_POSITION %in% c("CA", "FO"), PAIRING_POSITION, "FA"),
         BASE = if_else(BASE == "HAL", "HNL", BASE),
         TRANSACTION_CODE = if_else(TRANSACTION_CODE == "ARC", "SCR", TRANSACTION_CODE)) %>% 
  group_by(PAIRING_POSITION, PAIRING_DATE, BASE, TRANSACTION_CODE, EQUIPMENT) %>%
  summarise(DAILY_COUNT = n()) %>%  # Use summarise() instead of mutate() to avoid repeated counts
  ungroup() %>%
  pivot_wider(names_from = TRANSACTION_CODE, values_from = DAILY_COUNT) %>%
  mutate(RLV_SCR = coalesce(RLV, SCR)) %>%
  select(!c(SCR, RLV)) %>% 
  mutate(ASN = if_else(is.na(ASN), 0, ASN),
         EQUIPMENT = if_else(EQUIPMENT == "NA", NA, EQUIPMENT)) %>%
  drop_na(RLV_SCR) %>%
  mutate(PERCENT_UTILIZATION = round((ASN / RLV_SCR) * 100, 2)) %>%
  select(PAIRING_POSITION, PAIRING_DATE, BASE, EQUIPMENT, ASN, RLV_SCR, PERCENT_UTILIZATION) %>%
  #mutate(PAIRING_POSITION = "FA") %>%
  relocate(PAIRING_POSITION, .before = PAIRING_DATE) %>%
  relocate(EQUIPMENT, .before = ASN) %>% 
  mutate(CREW_TYPE = if_else(PAIRING_POSITION %in% c("CA", "FO"), "P", "FA")) %>% 
  rename(DATE=PAIRING_DATE,
         SEAT = PAIRING_POSITION,
         FLEET = EQUIPMENT) %>% 
  relocate(CREW_TYPE, .before = SEAT) %>% 
  mutate(AIRLINE = "HA", .before = CREW_TYPE)



write_csv(clean_rlv, "F:/INFLIGHT_RESERVE_UTILIZATION.csv")

print("Data Uploaded")
Sys.sleep(10)
