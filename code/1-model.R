
plot_end_date <- '2024-05-15'

# Models ----

plan('multisession', workers=6)
tab_ma <- future_pmap_dfr(list(levels(dat$rf)          ), cf_gam)
tab_sa <- future_pmap_dfr(list(levels(dat$rf), t_pred=0), cf_gam)
plan('sequential')


# Data cleaning ----

tab_combo <- tab_ma %>% 
  select(-c(starts_with('pr'), pn)) %>% 
  bind_cols(tab_sa %>% select(pn.lci2=pn.lci, pn.uci2=pn.uci)) %>% 
  mutate(pn.lci=pmin(pn.lci, pn.lci2), 
         pn.uci=pmax(pn.uci, pn.uci2)) %>% 
  select(-c(pn.lci2, pn.uci2)) %>% 
  mutate(
    period = case_when(
      date <= ymd('2022-02-15') ~ 'P1', 
      date <= ymd('2023-02-15') ~ 'P2', 
      .default = 'P3'
    )
  )
