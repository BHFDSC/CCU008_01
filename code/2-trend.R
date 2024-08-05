
# Plots Combo ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date)) %>% 
  
  ggplot(aes(x=date)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/Fig0-trend_overall.pdf', width=9, height=10, dpi=300)

## Age group ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date) & age_gp %in% levels(age_gp)[c(1, 4)]) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, age_gp)) %>% 
  
  ggplot(aes(x=date, colour=age_gp, fill=age_gp)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='Age group', fill='Age group') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/Fig1-trend_by_age.pdf', width=10, height=10, dpi=300)


## Sex ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, sex)) %>% 
  
  ggplot(aes(x=date, colour=sex, fill=sex)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='Sex', fill='Sex') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/Fig2-trend_by_sex.pdf', width=10, height=10, dpi=300)


## Ethnicity ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, ethnicity)) %>% 
  
  ggplot(aes(x=date, colour=ethnicity, fill=ethnicity)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='Ethnicity', fill='Ethnicity') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/Fig3-trend_by_eth.pdf', width=10, height=10, dpi=300)


## IMD ----

tab_combo %>% 
  filter(date <= ymd(plot_end_date) & imd5 %in% c(2, 5)) %>% 
  mutate(imd5=factor(imd5)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, imd5)) %>% 
  
  ggplot(aes(x=date, colour=imd5, fill=imd5)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='IMD quintile', fill='IMD quintile') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/Fig4-trend_by_imd.pdf', width=10, height=10, dpi=300)



# Plots Main ----

tab_ma %>% 
  filter(date <= ymd(plot_end_date)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date)) %>% 
  
  ggplot(aes(x=date)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEA0_trend_overall.pdf', width=9, height=10, dpi=300)

## Age group ----

tab_ma %>% 
  filter(date <= ymd(plot_end_date) & age_gp %in% levels(age_gp)[c(1, 4)]) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, age_gp)) %>% 
  
  ggplot(aes(x=date, colour=age_gp, fill=age_gp)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='Age group', fill='Age group') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEA1_trend_by_age.pdf', width=10, height=10, dpi=300)


## Sex ----

tab_ma %>% 
  filter(date <= ymd(plot_end_date)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, sex)) %>% 
  
  ggplot(aes(x=date, colour=sex, fill=sex)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='Sex', fill='Sex') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEA2_trend_by_sex.pdf', width=10, height=10, dpi=300)


## Ethnicity ----

tab_ma %>% 
  filter(date <= ymd(plot_end_date)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, ethnicity)) %>% 
  
  ggplot(aes(x=date, colour=ethnicity, fill=ethnicity)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='Ethnicity', fill='Ethnicity') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEA3_trend_by_eth.pdf', width=10, height=10, dpi=300)


## IMD ----

tab_ma %>% 
  filter(date <= ymd(plot_end_date) & imd5 %in% c(2, 5)) %>% 
  mutate(imd5=factor(imd5)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, imd5)) %>% 
  
  ggplot(aes(x=date, colour=imd5, fill=imd5)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='IMD quintile', fill='IMD quintile') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEA4_trend_by_imd.pdf', width=10, height=10, dpi=300)


# Plots SA ----

tab_sa %>% 
  filter(date <= ymd(plot_end_date)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date)) %>% 
  
  ggplot(aes(x=date)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEB0_trend_overall.pdf', width=9, height=10, dpi=300)

### Age group ----

tab_sa %>% 
  filter(date <= ymd(plot_end_date) & age_gp %in% levels(age_gp)[c(1, 4)]) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, age_gp)) %>% 
  
  ggplot(aes(x=date, colour=age_gp, fill=age_gp)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='Age group', fill='Age group') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEB1_trend_by_age.pdf', width=10, height=10, dpi=300)


## Sex ----

tab_sa %>% 
  filter(date <= ymd(plot_end_date)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, sex)) %>% 
  
  ggplot(aes(x=date, colour=sex, fill=sex)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='Sex', fill='Sex') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEB2_trend_by_sex.pdf', width=10, height=10, dpi=300)


## Ethnicity ----

tab_sa %>% 
  filter(date <= ymd(plot_end_date)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, ethnicity)) %>% 
  
  ggplot(aes(x=date, colour=ethnicity, fill=ethnicity)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='Ethnicity', fill='Ethnicity') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEB3_trend_by_eth.pdf', width=10, height=10, dpi=300)


## IMD ----

tab_sa %>% 
  filter(date <= ymd(plot_end_date) & imd5 %in% c(2, 5)) %>% 
  mutate(imd5=factor(imd5)) %>% 
  summarise(
    r     =sum(n_rf  )/sum(popu)*1000, 
    pr    =sum(pn    )/sum(popu)*1000, 
    pr.lci=sum(pn.lci)/sum(popu)*1000, 
    pr.uci=sum(pn.uci)/sum(popu)*1000, 
    .by=c(rf, date, imd5)) %>% 
  
  ggplot(aes(x=date, colour=imd5, fill=imd5)) + 
  geom_ribbon(aes(ymin=pr.lci, ymax=pr.uci), colour=NA, alpha=0.1) + 
  geom_line(aes(y=pr), lty=2, alpha=0.5) + 
  geom_line(aes(y=r)) + 
  scale_x_continuous(breaks=(ymd('2019-1-15') + (365.25)*(0:5)), labels=c(2019:2024)) + 
  labs(x='Year', y='Measurements per 1,000 persons', colour='IMD quintile', fill='IMD quintile') + 
  theme_bw() + 
  facet_wrap(vars(rf), nrow=4, scale='free_y')
ggsave('output/FigEB4_trend_by_imd.pdf', width=10, height=10, dpi=300)
