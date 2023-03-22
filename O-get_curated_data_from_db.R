library(dplyr)
library(readr)
library(tidyr)
library(stringr)
library(odbc)
library(DBI)
library(dbplyr)
library(bit64)
library(writexl)

setwd(rstudioapi::getActiveDocumentContext()$path %>% dirname())
pre_box <- getwd() %>% str_remove("OneDrive.+")
dpp_path_to_staged <- paste0(pre_box, "Box/Medicaid DPP/Data Management/Scripts for Analytical Dataset/staged")

# STEP 1: Connect to DB ----
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

# STEP 2: Get Curated Data from DB ----

wf_wide_outcomes <- dplyr::tbl(con, from = dbplyr::in_schema('analyst', 'N_wide_outcomes_4_analysis')) %>% 
  as_tibble() %>% mutate(across(where(is.integer64), ~as.character(.)))

wf_text_messages_tall <- dplyr::tbl(con, from = dbplyr::in_schema('analyst', 'I_textmessages_tall')) %>% 
  as_tibble() %>% mutate(across(where(is.integer64), ~as.character(.)))

wf_maps_calls_tall <- dplyr::tbl(con, from = dbplyr::in_schema('analyst', 'L_mapsCalls_tall')) %>% 
  as_tibble() %>% mutate(across(where(is.integer64), ~as.character(.)))

ehr_curated_race_tall <- dplyr::tbl(con, from = dbplyr::in_schema('analyst', 'D_EHR_race_tall')) %>% 
  as_tibble() %>% mutate(across(where(is.integer64), ~as.character(.)))

ehr_curated_demographics <- dplyr::tbl(con, from = dbplyr::in_schema('analyst', 'E_EHR_demographics')) %>% 
  as_tibble() %>% mutate(across(where(is.integer64), ~as.character(.)))

ehr_curated_encounters <- dplyr::tbl(con, from = dbplyr::in_schema('analyst', 'F_EHR_encounters')) %>% 
  as_tibble() %>% mutate(across(where(is.integer64), ~as.character(.)))

ehr_curated_pt_prob_list <- dplyr::tbl(con, from = dbplyr::in_schema('analyst', 'G_EHR_pt_prob_list')) %>% 
  as_tibble() %>% mutate(across(where(is.integer64), ~as.character(.)))

# STEP 3: Create WF Codebook read-in shell ----
codebook_shell <- data.frame(variables = colnames(wf_wide_outcomes),
                       variable_category = NA_character_,
                       varlab = NA_character_
)

# STEP 4: Save datasets in staged folder ----
write_xlsx(codebook_shell,
           path = file.path(dpp_path_to_staged, "codebook_4_read_in_template.xlsx"))

save(wf_wide_outcomes, wf_text_messages_tall, wf_maps_calls_tall, ehr_curated_race_tall, ehr_curated_demographics,
     ehr_curated_encounters, ehr_curated_pt_prob_list,
     file = file.path(dpp_path_to_staged, "curated_datasets.RData")
)
