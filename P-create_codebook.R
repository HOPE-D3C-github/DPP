library(tidyr)
library(stringr)
library(dplyr)
library(readr)
library(testthat)

setwd(rstudioapi::getActiveDocumentContext()$path %>% dirname())
pre_box <- getwd() %>% str_remove("OneDrive.+")
dpp_path_to_staged <- paste0(pre_box, "Box/Medicaid DPP/Data Management/Scripts for Analytical Dataset/staged")

# STEP 1. Load datasets ----
codebook_4_read_in <- read_csv(file = file.path(dpp_path_to_staged, "codebook_4_read_in.csv"), show_col_types = FALSE)
load(file.path(dpp_path_to_staged, "curated_datasets.RData"))

# STEP 2. Confirm variable names from codebook_4_read_in match the data

test1 <- test_that(desc = 'Confirm variable names from codebook_4_read_in match the data', code = {
  expect_equal(object = codebook_4_read_in$Variables,
               expected = colnames(wf_wide_outcomes))
})

# STEP 3. Gather categorical values (only for categorical variables)
codebook <- codebook_4_read_in %>% mutate(Values = NA)

for (i in 1:nrow(codebook)){
  var_i <- codebook$Variables[i]
  if (codebook$categorical[i]){ # TRUE == limited range of values; will add them in the values column for var_i
    codebook$Values[i] <- wf_wide_outcomes %>% mutate(var_i = as.character(var_i)) %>% select(all_of(var_i)) %>% unique() %>% na.omit() %>% unlist() %>% sort() %>% paste(collapse = "; ")
  } else {
    codebook$Values[i] <- ''
  }
}
codebook <- codebook %>% select(-categorical)

# STEP 4. Save codebook in staged folder
saveRDS(codebook, file = file.path(dpp_path_to_staged, "codebook_staged.RDS"))
