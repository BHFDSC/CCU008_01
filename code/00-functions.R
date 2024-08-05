
cf_gam <- function(rf_sel, start_ymd='2018-11-15', t_pred=NULL){
  suppressPackageStartupMessages(require(tidyverse))
  require(mgcv)
  
  datm  <- dat %>% 
    filter(rf==rf_sel & date >= ymd(start_ymd)) %>% 
    mutate(lt=log(t+3))
  
  m <- gam(n_rf ~ lt + lt:age_gp + lt:sex + lt:ethnicity + lt:imd5 + 
             s(month, bs='cp', k=4) + age_gp + sex + ethnicity + imd5, 
           offset=log(popu), family='quasipoisson', 
           data=datm %>% filter(date <= ymd('2020-02-15')))
  # summary(m); plot(m)
  
  tab <- data.frame(rbind(summary(m)$p.table, summary(m)$s.table))
  tab <- tab[, -3]
  names(tab) <- c('coef', 'se', 'p')
  tab$rf <- rf_sel
  tab$var <- row.names(tab)
  
  write_csv(tab, paste0('output/GAM/', rf_sel, '.csv'))
  
  if (!is.null(t_pred)){
    datm <- datm %>% mutate(t=if_else(t <= t_pred, t, t_pred))
  }
  datm  <- datm %>% mutate(lt=log(t+3))
  
  datm <- datm %>% 
    mutate(as.data.frame(predict(m, newdata=datm, se.fit=T))) %>% 
    transmute(
      rf=factor(rf_sel, levels=rf_sel), 
      year, month, date, age_gp, sex, ethnicity, imd5, popu, rate, n_rf, 
      pr    =exp(fit)              , 
      pr.lci=exp(fit - 1.96*se.fit), 
      pr.uci=exp(fit + 1.96*se.fit), 
      pn    =pr    *popu, 
      pn.lci=pr.lci*popu, 
      pn.uci=pr.uci*popu)
  
  return(datm)
}


format_param <- function(coef, lci, uci, digit=1){
  paste0(format(round(coef, digit), nsmall=digit), ' (', 
         format(round(lci , digit), nsmall=digit), ', ', 
         format(round(uci , digit), nsmall=digit), ')')
}
