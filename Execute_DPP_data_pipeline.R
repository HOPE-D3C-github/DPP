library(dplyr)
library(readr)
library(tidyr)
library(stringr)
library(odbc)
library(DBI)
library(dbplyr)

setwd(rstudioapi::getActiveDocumentContext()$path %>% dirname())
pre_box <- getwd() %>% str_remove("OneDrive.+")

# STEP 1: Create ordered dataframe of files to be run ----
data_pipeline <- data.frame(name = list.files()) %>% filter(!str_detect(name, pattern = 'ignore|.docx|.md')) %>% 
  filter(!str_detect(name, pattern = 'Loading_EHR_data_into_SQL_database|Execute_DPP_data_pipeline|CREATE_analyst_convert_to_AmDenv')) %>% 
  filter(name != 'create_codebook.R') %>%  # @TB: temporary - ensure this is removed
  mutate(started_running_file = NA,
         finished_running_file = NA)

# STEP 2: Connect to DB ----
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

# STEP 3: Run the files in order, stopping on an error 
for (file_i in 1:nrow(data_pipeline)){
  data_pipeline$started_running_file[file_i] <- TRUE
  data_pipeline$finished_running_file[file_i] <- FALSE
  
  ### 
  if (str_split(data_pipeline$name[file_i], pattern = "\\.", simplify = T)[2] == "Rmd"){  # if the respective file name is a .Rmd file
    rmarkdown::render(as.character(data_pipeline$name[file_i]))
    data_pipeline$finished_running_file[file_i] <- TRUE
    cat("Executed file:", data_pipeline$name[file_i])
  } else if (str_split(data_pipeline$name[file_i], pattern = "\\.", simplify = T)[2] == "R"){ # else if the respective file name is a .R file
    source(as.character(data_pipeline$name[file_i]))
    data_pipeline$finished_running_file[file_i] <- TRUE
    cat("Executed file:", data_pipeline$name[file_i])
  } else if (str_split(data_pipeline$name[file_i], pattern = "\\.", simplify = T)[2] == "sql"){
    # below is specific to .sql files
    file <- read_file(data_pipeline$name[file_i])
    file_parsed <- as.vector(str_split(file, pattern = ';', simplify = T))
    for (query_i in 1:length(file_parsed)){
      txt <- file_parsed[query_i]
      query <- str_remove_all(txt, pattern = regex("/\\*.+?\\*/", dotall = TRUE))
      if (query != ""){ 
        (rs <- dbSendQuery(con, query))    
        if(exists(quote(rs))){
          dbClearResult(rs)
          rm(rs)
          cat('Sent query #', query_i, ", for file #", file_i,',', data_pipeline$name[file_i],"sucessfully.",'\n')
        } else {
          cat('Query ', query_i, 'was unsuccessful. Execution stopped.') # An error message should come up before this can run, but keeping it as a safeguard
          stop()
        }
      } 
      if (query_i == length(file_parsed)){
        cat('All queries sent sucessfully for file ', file_i, ',', data_pipeline$name[file_i],'\n')
        data_pipeline$finished_running_file[file_i] <- TRUE
      }
    }}
  # if (file_i == nrow(data_pipeline)){
  #   cat('All queries sent sucessfully for data pipeline \n')}
    else { # Catches non .Rmd and .R files
      message("Could not execute the file. Not .R or .Rmd")
    }
}

# STEP 4: Print Results
print(data_pipeline)
