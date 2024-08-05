
# Overall ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date) & date >= ymd('2020-01-15')) %>% 
  summarise(
    delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
    delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
    delta = (delta.l + delta.u)/2, 
    .by=c(rf, date)
  ) %>% 
  
  ggplot(aes(x=date, y=delta)) + 
  geom_hline(yintercept=0, linewidth=0.5, linetype='dashed') + 
  geom_line(linewidth=0.25) + 
  geom_ribbon(aes(ymin=delta.l, ymax=delta.u), alpha=0.2) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='% deviation in measurements') + 
  coord_cartesian(ylim=c(-100, 50)) +
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigD0-deviation_overall.pdf', width=9, height=10, dpi=300)


# Age ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date) & date >= ymd('2020-01-15') & age_gp %in% levels(age_gp)[c(1, 4)]) %>% 
  summarise(
    delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
    delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
    delta = (delta.l + delta.u)/2, 
    .by=c(rf, date, age_gp)
  ) %>% 
  
  ggplot(aes(x=date, y=delta, colour=age_gp, fill=age_gp)) + 
  geom_hline(yintercept=0, linewidth=0.5, linetype='dashed') + 
  geom_line(linewidth=0.25) + 
  geom_ribbon(aes(ymin=delta.l, ymax=delta.u, colour=NULL), alpha=0.2) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='% deviation in measurements', fill='Age group', colour='Age group') + 
  coord_cartesian(ylim=c(-100, 50)) +
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigD1-deviation_age.pdf', width=9, height=10, dpi=300)


# Sex ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date) & date >= ymd('2020-01-15')) %>% 
  summarise(
    delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
    delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
    delta = (delta.l + delta.u)/2, 
    .by=c(rf, date, sex)
  ) %>% 
  
  ggplot(aes(x=date, y=delta, colour=sex, fill=sex)) + 
  geom_hline(yintercept=0, linewidth=0.5, linetype='dashed') + 
  geom_line(linewidth=0.25) + 
  geom_ribbon(aes(ymin=delta.l, ymax=delta.u, colour=NULL), alpha=0.2) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='% deviation in measurements', fill='Sex', colour='Sex') + 
  coord_cartesian(ylim=c(-100, 50)) +
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigD2-deviation_sex.pdf', width=9, height=10, dpi=300)


# Ethnicity ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date) & date >= ymd('2020-01-15')) %>% 
  summarise(
    delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
    delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
    delta = (delta.l + delta.u)/2, 
    .by=c(rf, date, ethnicity)
  ) %>% 
  
  ggplot(aes(x=date, y=delta, colour=ethnicity, fill=ethnicity)) + 
  geom_hline(yintercept=0, linewidth=0.5, linetype='dashed') + 
  geom_line(linewidth=0.25) + 
  geom_ribbon(aes(ymin=delta.l, ymax=delta.u, colour=NULL), alpha=0.2) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='% deviation in measurements', fill='Ethnicity', colour='Ethnicity') + 
  coord_cartesian(ylim=c(-100, 50)) +
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigD3-deviation_eth.pdf', width=9, height=10, dpi=300)


# IMD ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date) & date >= ymd('2020-01-15') & imd5 %in% c(2, 5)) %>% 
  summarise(
    delta.l = (sum(n_rf) - sum(pn.lci)) / sum(pn.lci) *100, 
    delta.u = (sum(n_rf) - sum(pn.uci)) / sum(pn.uci) *100,
    delta = (delta.l + delta.u)/2, 
    .by=c(rf, date, imd5)
  ) %>% 
  mutate(imd5=factor(imd5)) %>% 
  
  ggplot(aes(x=date, y=delta, colour=imd5, fill=imd5)) + 
  geom_hline(yintercept=0, linewidth=0.5, linetype='dashed') + 
  geom_line(linewidth=0.25) + 
  geom_ribbon(aes(ymin=delta.l, ymax=delta.u, colour=NULL), alpha=0.2) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='% deviation in measurements', fill='IMD quintile', colour='IMD quintile') + 
  coord_cartesian(ylim=c(-100, 50)) +
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigD4-deviation_imd.pdf', width=9, height=10, dpi=300)

