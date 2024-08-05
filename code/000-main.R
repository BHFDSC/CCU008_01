library(DBI)
library(tidyverse)
library(lubridate)
library(dbplyr)
library(furrr)
library(mgcv)

source('00-functions.R')
source('0-import_clean.R')

dat %>% summarise(sum(n_rf), .by=rf)

# Models ----
source('1-model.R')

# Plots ----
source('2-trend.R')
source('3-%d_trend.R')

# Tables ----
source('4-d_table.R')
source('5-%d_table.R')


