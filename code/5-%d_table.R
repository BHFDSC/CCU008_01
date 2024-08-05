
tab_combo %>% 
  filter(date <= ymd(plot_end_date) & date >= ymd('2020-03-15')) %>% 
  summarise(
    delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
    delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
    delta = (delta.l + delta.u)/2, .by=c(rf, period)
  ) %>% 
  transmute(rf, period, d=format_param(delta, delta.u, delta.l)) %>% 
  
  inner_join(
    tab_combo %>% 
      filter(date <= ymd(plot_end_date) & date >= ymd('2020-03-15')) %>% 
      summarise(
        delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
        delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
        delta = (delta.l + delta.u)/2, .by=c(rf, period, age_gp)
      ) %>% 
      transmute(rf, period, d=format_param(delta, delta.u, delta.l), age_gp) %>% 
      pivot_wider(names_from='age_gp', values_from='d'), 
    by=c('rf', 'period'))%>% 
  
  inner_join(
    tab_combo %>% 
      filter(date <= ymd(plot_end_date) & date >= ymd('2020-03-15')) %>% 
      summarise(
        delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
        delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
        delta = (delta.l + delta.u)/2, .by=c(rf, period, sex)
      ) %>% 
      transmute(rf, period, d=format_param(delta, delta.u, delta.l), sex) %>% 
      pivot_wider(names_from='sex', values_from='d'), 
    by=c('rf', 'period'))%>% 
  
  inner_join(
    tab_combo %>% 
      filter(date <= ymd(plot_end_date) & date >= ymd('2020-03-15')) %>% 
      summarise(
        delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
        delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
        delta = (delta.l + delta.u)/2, .by=c(rf, period, ethnicity)
      ) %>% 
      transmute(rf, period, d=format_param(delta, delta.u, delta.l), ethnicity) %>% 
      pivot_wider(names_from='ethnicity', values_from='d'), 
    by=c('rf', 'period'))%>% 
  
  inner_join(
    tab_combo %>% 
      filter(date <= ymd(plot_end_date) & date >= ymd('2020-03-15')) %>% 
      summarise(
        delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
        delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
        delta = (delta.l + delta.u)/2, .by=c(rf, period, imd5)
      ) %>% 
      transmute(rf, period, d=format_param(delta, delta.u, delta.l), imd5) %>% 
      pivot_wider(names_from='imd5', values_from='d'), 
    by=c('rf', 'period')) %>% 
  
  write_csv('output/tables/%deviations.csv')
    