# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Summary: Read in raw EHR data csv files (demographics, race, encounters, pt_prob_list) and 
#           write them to the DPP database as Tables on the Analyst Schema.
# 
# NOTE: WRITING THE ENCOUNTERS DATA TO THE DB TAKES A LONG TIME!!!
# 
# Inputs:   Raw EHR data csv files stored at [path_to_ehr_dat]
# 
# Outputs:  Writes each file to the DPP database as a Table on the Analyst Schema
# 
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

library(dplyr)
library(readr)
library(tidyr)
library(stringr)
library(lubridate)
library(scales)
library(odbc)
library(DBI)
library(dbplyr)
library(bit64)
library(haven)

setwd(rstudioapi::getActiveDocumentContext()$path %>% dirname())

pre_box <- getwd() %>% str_remove("OneDrive.+")
path_to_ehr_dat <- paste0(pre_box, "Box/Medicaid DPP/EDW Pull")
path_to_outputs <- paste0(pre_box, "OneDrive - University of Utah/GitHub - Center for Hope/DPP")

# Load EHR data
demog <- read_csv(file.path(path_to_ehr_dat, "dpp_inclusion_pat_demog_20220321.csv"), show_col_types = FALSE)
race <- read_csv(file.path(path_to_ehr_dat, "dpp_inclusion_pat_race_20220321.csv"), show_col_types = FALSE) %>% slice(1:nrow(.)-1) #last row is text and not meant to be kept in
encounters <- read_csv(file.path(path_to_ehr_dat, 'dpp_inclusion_pat_enc_dx_20220321.csv'), show_col_types = FALSE)
pt_prob_list <- read_csv(file.path(path_to_ehr_dat, 'dpp_inclusion_pat_prob_list_dx_20220321.csv'), show_col_types = FALSE) %>% slice(1:nrow(.)-1)  #last row is text and not meant to be kept in 

# Connect to DB
sort(unique(odbc::odbcListDrivers()[[1]]))
#ensure "MySQL ODBC 8.0 ANSI Driver" or "MySQL ODBC 8.0 Unicode Driver" are installed -- 
#mariadb has a connector too, but MySQL installer makes the above easy to install. if you use MariaDB, then you'll need to update driver below. there's also the mariadb package.
con <- DBI::dbConnect(odbc::odbc(),
                      Driver   =  "MySQL ODBC 8.0 ANSI Driver",
                      Server   = "localhost", #standard
                      user = read_lines(file = file.path(pre_box, "Box/my_credentials/mysql_dpp_username.txt")),        #mysql_dpp_username
                      password = read_lines(file = file.path(pre_box, "Box/my_credentials/mysql_dpp_password.txt")),    #mysql_dpp_username
                      Port     = 5508 #get server (if local) or from command run to SSH to remote server
)

# Load EHR data into DB
rs <- dbSendQuery(con, 'use analyst'); dbClearResult(rs); rm(rs)

dbWriteTable(con, "raw_EHR_demographics", demog, overwrite = F)

dbWriteTable(con, "raw_EHR_race", race, overwrite = F)

dbWriteTable(con, "raw_EHR_encounters", encounters, overwrite = F)

dbWriteTable(con, "raw_EHR_pt_prob_list", pt_prob_list, overwrite = F)

