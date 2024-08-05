
# Import ----

con <- dbConnect(
  odbc::odbc(),
  dsn = 'databricks',
  HTTPPath = 'sql/protocolv1/o/847064027862604/0622-162121-dts9kxvy',
  PWD = read.table('pwd.txt')$V1

)

# pop <- tbl(con, in_schema('dsa_391419_j3w9t_collab',
#                           'ccu008_popu_cnt_20240703d')) %>%
#   collect() %>%
#   rename(popu=count)

pop <- tbl(con, in_schema('dsa_391419_j3w9t_collab',
                          'ccu008_popu_cnt_20240715_Final')) %>%
  collect() %>%
  rename(popu=count)

# ccu008_rf_dailycnt_20240709     # Inc AmBP, All records
# ccu008_rf_dailycnt_SA_20240709  # Inc AmBP, Exc without values
# ccu008_rf_cnt_20240710          # Exc AmBP, All records
# ccu008_rf_cnt_20240703          # Exc AmBP, Exc without values

# rf <- bind_rows(
#   tbl(con, in_schema('dsa_391419_j3w9t_collab',
#                      'ccu008_rf_dailycnt_20240709')) %>%
#     filter(rf %in% c('Alcohol', 'Smoking')) %>%
#     collect(),
#   tbl(con, in_schema('dsa_391419_j3w9t_collab',
#                      'ccu008_rf_dailycnt_SA_20240709')) %>%
#     filter(!(rf %in% c('Alcohol', 'Smoking'))) %>%
#     collect()) %>%
#   rename(n_rf=count)

rf <- tbl(con, in_schema('dsa_391419_j3w9t_collab',
                         'ccu008_rf_cnt_20240715_Final')) %>%
  collect() %>%
  rename(n_rf=count)


dat <- expand_grid(rf=unique(rf$rf), pop) %>% 
  left_join(rf, by=c('rf', 'year', 'month', 'age_gp',  'sex', 'ethnicity', 'imd5')) %>% 
  mutate(n_rf=if_else(!is.na(n_rf), as.integer(n_rf), as.integer(0)),
         popu=as.integer(popu),
         rate=n_rf/popu)

rm(pop, rf)

# Clean ----

# dat <- dat %>% 
#   mutate(
#     ld=if_else((year==2020 & month %in% c(4:6, 11)) | 
#                  (year==2021 & month %in% c(1:3)) , 1, 0), 
#     td=if_else(year > 2020 | (year==2020 & month > 3), (year-2020) + (month-3)/12, 0)
#   ) %>% 
#   left_join(si, by=c('year', 'month')) %>% 
#   mutate(si=if_else(is.na(si), 0, si))

dat <- dat %>% 
  arrange(rf, year, month, age_gp, sex, ethnicity, imd5) %>% 
  mutate(
    date=ymd(paste(year, month, '15', sep='-')), 
    t = (year - 2020) + (month - 3)/12, # t as timeline variable with 0 at 3/2020
    rf=factor(rf, levels=c('BMI', 'Alcohol', 'Smoking', 
                           'BP', 'Fasting glucose', 'HbA1c', 
                           'Total cholesterol', 'LDL cholesterol', 'HDL cholesterol', 
                           'Triglycerides', 'LFT', 'eGFR')), 
    age_gp=factor(age_gp), 
    sex=factor(sex), 
    ethnicity=factor(ethnicity, 
                     levels=c('White', 'Asian or Asian British', 
                              'Black, Black British, Caribbean or African', 
                              'Mixed or multiple ethnic groups', 
                              'Other/Unknown'), 
                     labels=c('White', 'Asian', 'Black', 'Mixed', 'Other/Unknown')), 
    .after=year)
