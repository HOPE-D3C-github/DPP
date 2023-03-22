library(dplyr)
library(readr)
library(tidyr)
library(stringr)
library(odbc)
library(DBI)
library(dbplyr)

setwd(rstudioapi::getActiveDocumentContext()$path %>% dirname())
pre_box <- getwd() %>% str_remove("OneDrive.+")

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

# Test sending queries to DB ----
(rs <- dbSendQuery(con, ' /* test */ SELECT * FROM analyst.raw_EHR_race;'))

dbFetch(rs)

dbClearResult(rs); rm(rs)


# Read in .sql scripts and parse by semi-colon
# will need to treat the create function script differently because it uses semi-colon in the create 

file <- read_file('z2-EHR_demographics.sql')

file_parsed <- as.vector(str_split(file, pattern = ';', simplify = T))

for (i in 1:length(file_parsed)){
  if (file_parsed[i] != ""){
    (rs <- dbSendQuery(con, file_parsed[i]))
    if(exists(quote(rs))){
      dbClearResult(rs)
      rm(rs)
      cat('sent query #', i,'\n')
    } else {
      cat('Query ', i, 'was unsuccessful. Execution stopped.') # An error message should come up before this can run, but keeping it as a safeguard
      stop()
    }
  } 
  if (i == length(file_parsed)){
    cat('All queries sent sucessfully')
  }
}





