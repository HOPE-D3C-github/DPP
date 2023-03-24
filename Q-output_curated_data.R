library(tidyr)
library(stringr)
library(dplyr)
library(readr)
library(testthat)
library(haven)
library(openxlsx)

setwd(rstudioapi::getActiveDocumentContext()$path %>% dirname())
pre_box <- getwd() %>% str_remove("OneDrive.+")
dpp_path_to_staged <- paste0(pre_box, "Box/Medicaid DPP/Data Management/Scripts for Analytical Dataset/staged")
dpp_path_to_output <- paste0(pre_box, "Box/Medicaid DPP/Data Management/Scripts for Analytical Dataset/outputs")

# Load Data ----
load(file.path(dpp_path_to_staged, 'curated_datasets.RData'))
codebook <- readRDS(file = file.path(dpp_path_to_staged, "codebook_staged.RDS"))

# Save Each Dataset ----

# Wide Workflow Data
write_csv(wf_wide_outcomes, file = file.path(dpp_path_to_output, "WF_Wide_Outcomes.csv"))
saveRDS(wf_wide_outcomes, file = file.path(dpp_path_to_output, "WF_Wide_Outcomes.RDS"))
write_dta(wf_wide_outcomes %>% select(-phase1_concatenated_notes, -phase2_concatenated_notes), path = file.path(dpp_path_to_output, "WF_Wide_Outcomes.dta")) # @TB: notes field is too many characters for DTA - probably need to remove from analysis dataset anyway due to PHI potential

# Text Messages Tall
write_csv(wf_text_messages_tall, file = file.path(dpp_path_to_output, "WF_Text_Messages_Tall.csv"))
saveRDS(wf_text_messages_tall, file = file.path(dpp_path_to_output, "WF_Text_Messages_Tall.RDS"))
write_dta(wf_text_messages_tall, path = file.path(dpp_path_to_output, "WF_Text_Messages_Tall.dta"))

# MAPS Calls Tall
write_csv(wf_maps_calls_tall, file = file.path(dpp_path_to_output, "WF_MAPS_Calls_Tall.csv"))
saveRDS(wf_maps_calls_tall, file = file.path(dpp_path_to_output, "WF_MAPS_Calls_Tall.RDS"))
write_dta(wf_maps_calls_tall, path = file.path(dpp_path_to_output, "WF_MAPS_Calls_Tall.dta"))

# EHR Race Tall
write_csv(ehr_curated_race_tall, file = file.path(dpp_path_to_output, "EHR_Race_Tall.csv"))
saveRDS(ehr_curated_race_tall, file = file.path(dpp_path_to_output, "EHR_Race_Tall.RDS"))
write_dta(ehr_curated_race_tall, path = file.path(dpp_path_to_output, "EHR_Race_Tall.dta"))

# EHR Demographis
write_csv(ehr_curated_demographics, file = file.path(dpp_path_to_output, "EHR_Demographics.csv"))
saveRDS(ehr_curated_race_tall, file = file.path(dpp_path_to_output, "EHR_Demographics.RDS"))
write_dta(ehr_curated_race_tall, path = file.path(dpp_path_to_output, "EHR_Demographics.dta"))

# EHR Encounters
write_csv(ehr_curated_encounters, file = file.path(dpp_path_to_output, "EHR_Encounters.csv"))
saveRDS(ehr_curated_encounters, file = file.path(dpp_path_to_output, "EHR_Encounters.RDS"))
write_dta(ehr_curated_encounters, path = file.path(dpp_path_to_output, "EHR_Encounters.dta"))

# EHR Pt Prob List
write_csv(ehr_curated_pt_prob_list, file = file.path(dpp_path_to_output, "EHR_Pt_Prob_List.csv"))
saveRDS(ehr_curated_pt_prob_list, file = file.path(dpp_path_to_output, "EHR_Pt_Prob_List.RDS"))
write_dta(ehr_curated_pt_prob_list, path = file.path(dpp_path_to_output, "EHR_Pt_Prob_List.dta"))

# Codebook
saveRDS(codebook, file = file.path(dpp_path_to_output, "Codebook.RDS"))
write_dta(codebook %>% rename(Variable_Category = `Variable Category`, Variable_Description = `Variable Description`), path = file.path(dpp_path_to_output, "Codebook.dta"))

wb <- createWorkbook()
addWorksheet(wb, "Codebook")
writeData(wb, sheet = 1, x = codebook)
setColWidths(wb, sheet = 1, cols = c(1,2,3,4), widths = c(35,35,70,35))
freezePane(wb, sheet = 1, firstRow = TRUE, firstCol = TRUE)
header_style <- createStyle(textDecoration = "bold")
addStyle(wb, sheet = 1, header_style, rows = 1, cols = 1:4)
style_wrap <- createStyle(wrapText = TRUE)
addStyle(wb, sheet = 1, style_wrap, rows = 1:nrow(codebook), cols = 3:4, gridExpand = TRUE)
saveWorkbook(wb, file.path(dpp_path_to_output, "Codebook.xlsx"), overwrite = TRUE)
