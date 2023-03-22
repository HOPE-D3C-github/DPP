library(tidyr)
library(stringr)
library(dplyr)
library(readr)
library(testthat)

setwd(rstudioapi::getActiveDocumentContext()$path %>% dirname())
pre_box <- getwd() %>% str_remove("OneDrive.+")
dpp_path_to_staged <- paste0(pre_box, "Box/Medicaid DPP/Data Management/Scripts for Analytical Dataset/staged")

# STEP 1. Load Data
load(file.path(dpp_path_to_staged, 'curated_datasets.RData'))
codebook <- readRDS(file = file.path(dpp_path_to_staged, "codebook_staged.RDS"))












