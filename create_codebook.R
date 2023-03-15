library(tidyr)
library(stringr)
library(dplyr)
library(readr)
library(writexl)
library(lubridate)
library(scales)
library(odbc)
library(DBI)
library(dbplyr)
library(bit64)
library(haven)

pre_box <- rstudioapi::documentPath() %>% str_split_fixed(.,pattern = "Box", n = 2)
pre_box <- pre_box[1]
dpp_path_to_data <- paste0(pre_box, "Box/Medicaid DPP/Data Management/Scripts for Analytical Dataset/outputs")
dpp_path_to_staged <- paste0(pre_box, "Box/Medicaid DPP/Data Management/Scripts for Analytical Dataset/staged")

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Connect to database to grab some workflow data

# converts integer64 columns to a more native R numeric format. 
clean_int64 <- function(mydf){
  mutate_if(.tbl=mydf,
            .predicate=bit64::is.integer64,
            .funs=bit64::as.double.integer64)
}
### read all tables from workflow schema
my_readtable <- function(mytable){
  tbl(con, in_schema(production_schema, mytable)) %>% as_tibble %>% clean_int64
}

#mysql_dpp_username <- read_lines(file = file.path(pre_box, "Box/my_credentials/mysql_dpp_username.txt"))
#mysql_dpp_password <- read_lines(file = file.path(pre_box, "Box/my_credentials/mysql_dpp_password.txt"))
# production_schema <- 'workflow' #this varies between servers

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

wf_wide_outcomes <- dplyr::tbl(con, from = dbplyr::in_schema('analyst', 'H_wide_outcomes_4_analysis')) %>% 
  as_tibble() %>% mutate(across(where(is.integer64), ~as.character(.)))
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# one time thing, to create 'codebook_4_read_in' template
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

if(F){
  template <- data.frame(variables = colnames(wf_wide_outcomes),
                       variable_category = NA_character_,
                       varlab = NA_character_
                       )

  write_xlsx(template,
          path = file.path(dpp_path_to_staged, "codebook_4_read_in_template.xlsx"))
  }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Use the filled out codebook_4_read_in and wf_wide_outcomes to create codebook
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
manually_updated_codebook_sections <- read_csv(file = file.path(dpp_path_to_staged, "codebook_4_read_in.csv"))

codebook <- manually_updated_codebook_sections %>% mutate(values = NA)


if(F){
  i <- 2
}

for (i in 1:nrow(codebook)){
  var_i <- codebook$variables[i]
  if (codebook$categorical[i]){ # TRUE == limited range of values; will add them in the values column for var_i
    codebook$values[i] <- wf_wide_outcomes %>% mutate(var_i = as.character(var_i)) %>% select(all_of(var_i)) %>% unique() %>% na.omit() %>% unlist() %>% sort() %>% paste(collapse = "; ")
  } else {
    codebook$values[i] <- ''
  }
}
codebook <- codebook %>% select(-categorical)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Outputs
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
write.csv(codebook,
          file = file.path(dpp_path_to_data, 'codebook.csv'), row.names = F)

write.csv(wf_wide_outcomes,
          file = file.path(dpp_path_to_data, 'wide_workflow_outcome.csv'), row.names = F)




