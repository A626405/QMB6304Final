source("code/R/functions.r")
create_db()
reticulate::py_run_file("code/Python/functions.py")
gc(F,F,T)
options(scipen=4,"digits.sec"=4)

selectedcols<-c("datetime", "host", "proto", "spt", "dpt", "srcstr", "cc", "country", "latitude", "longitude")
raw_data <- data.table::fread("data/internal/AWS_Honeypot_marx-geo.csv",quote="\"",strip.white=T,fill=T,blank.lines.skip=T,header=T,na.strings=c("NA",NULL,"",",,"),keepLeadingZeros=T,select=c("datetime", "host", "proto", "spt", "dpt", "srcstr", "cc", "country", "longitude", "latitude"), colClasses=list(character=c(1:5,8:13),integer=6:7,double=14:15,NULL=16),data.table=T,nThread= (parallel::detectCores()-1),nrows=451581)
rm(selectedcols)
gc(F,F,T)

#options("digits.secs"=0,"max.print"=1000,datatable.optimize=T)

datetimes <- as.POSIXlt(raw_data$datetime, format="%d/%m/%y %M:%OS",tz="")
datetimes <- format(datetimes,format= "%Y/%m/%d %I:%M:%OS:%Os%H",tz="UTC", usetz=F,digits=10)
datetimes <- as.character.POSIXt(datetimes)
raw_data$datetimes <- datetimes
#working_data$seconds <- C(as.character.POSIXt(working_data$datetimes))

#raw_data$datetimes <- C(round.POSIXt(datetimes,units="secs"))
#datetimes <- round.POSIXt(datetimes)
rm(datetimes)
gc(F,T,T)

"
<- datetimes2 / 1e10

lubridate::am(datetimes1)

lubridate::decimal_date(datetimes1)
raw_data$datetimes <- c(datetimes)
format()
gc(F,F,T)
"
working_data <- raw_data |>
  dplyr::mutate("host"=gsub("-", "_", raw_data$host),"dpt"= c(tidyr::replace_na(raw_data$dpt,99999)),"spt"= c(tidyr::replace_na(raw_data$spt,99999)))|>
  dplyr::select(datetime,datetimes,srcstr,proto,host,spt,dpt,longitude,latitude,cc,country) |>
  dplyr::group_by() |>
  dplyr::arrange(.by_group=T) |>
  dplyr::rename("region"="country","long"="longitude","lat"="latitude") 
rm(raw_data)
gc(F,F,T)

USI<-which(working_data$cc == "US")
working_data$region<-replace(working_data$region,USI,"United States")
rm(USI)
gc(F,F,T)

HKI<-which(working_data$cc == "HK")
working_data$region<-replace(working_data$region,HKI,"Hong Kong")
rm(HKI)
working_data <- working_data |> dplyr::mutate("cc"=NULL)
gc(F,F,T)


na_indices<-which(is.na(working_data$region))
ips_to_check<-data.frame('x'=working_data$srcstr[na_indices])
write.csv2(ips_to_check,"data/tempdata/ips_file.csv",row.names=F,col.names=T)
rm(na_indices,ips_to_check)
gc(F,F,T)

require(reticulate,include.only=T)
reticulate::import("sqlite3",delay_load=T)
reticulate::import("pandas",as="pd",delay_load=T)
reticulate::import("geoip2",delay_load=T)

reticulate::py$process_ip_file1('data/tempdata/ips_file.csv', 'data/tempdata/ips_with_country.csv')

file.remove('data/tempdata/ips_file.csv')
gc()
resolvedcountries <- data.table::fread(input="data/tempdata/ips_with_country.csv",header=T,col.names=c("srcstr", "region"),nThread=(parallel::detectCores()-1))
working_data <- dplyr::left_join(working_data,resolvedcountries)
rm(resolvedcountries)
file.remove('data/tempdata/ips_with_country.csv')
gc(F,F,T)

"
na_indices2 <- which(is.na(working_data$region))
ipmissingreg <- unique(working_data$srcstr[na_indices2])
write.csv2(ipmissingreg,'data/tempdata/ips_file.csv',row.names=F)
rm(na_indices2,ipmissingreg)
gc(F,F,T)

reticulate::py$process_ip_file('data/tempdata/ips_file.csv', 'data/tempdata/ips_with_country.csv')
file.remove('data/tempdata/ips_file.csv')
resolvedcountries <- data.table::fread(input='data/tempdata/ips_with_country.csv',header=T,col.names=c('srcstr', 'region'),nThread=(parallel::detectCores()-1))
working_data <- dplyr::left_join(working_data,resolvedcountries)
rm(resolvedcountries)
file.remove('data/tempdata/ips_with_country.csv')
gc(F,F,T)
"
resolvedcountries <- data.table::fread(input='data/tempdata/ips_with_country2.csv',header=T,col.names=c('srcstr', 'region'),nThread=(parallel::detectCores()-1))
working_data <- dplyr::left_join(working_data,resolvedcountries)
rm(resolvedcountries)
gc(F,F,T)


working_data <- working_data |>
  dplyr::rename("protocol"="proto") |>
  dplyr::arrange(datetime) 
gc(F,F,T)

working_data$ccrating <- working_data$region
L5C<-c("United States","France","Germany","Canada","United Kingdom","Belgium","Switzerland","Netherlands","Ireland","Finland","Sweden","Norway","Denmark","Luxembourg","Puerto Rico","New Zealand","Austrailia","Iceland","Spain","Portugal")
goodccindex <- which(working_data$ccrating %in% L5C)
rm(L5C)
gc(F,F,T)

working_data$ccrating<- replace(working_data$ccrating,goodccindex,c(rep(0,length(goodccindex))))
working_data$ccrating<- replace(working_data$ccrating,-goodccindex,c(rep(1,length(-goodccindex))))
rm(goodccindex)
gc(F,F,T)


dpt<-c("1433","445","3389","3306","135","53","5060","5900","123","389","5432","1434","1900","5353","11211","19","137","138","161","162","500","1900","3702","5683","20800","3283","7547","11211","7100","33434","1080")
service<-c("MSSQL","SMB","RDP","MSSQL","RPC","DNS","SIP","VNC","NTP","LDAP","MSSQL","MSSQL","SSDP","mDNS","Memcached","Chargen","NetBIOS_NS","NetBIOS_DGM","SNMP","SNMP_Trap","ISAKMP","SSDP","WS_Discovery","CoAP","CompuServe","NetAssistant","TR_069","Memcached","X11","Linux_tracert","socks")
protocol<-c("UDP","TCP","TCP","UDP","TCP","UDP","UDP","TCP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","TCP","TCP")

ddosport <- c(rep(1,length(dpt)))
servdict<-data.frame(cbind(ddosport,dpt,service))
rm(dpt,service,protocol,ddosport)
gc(F,T,T)

working_data$dpt <- as.character(working_data$dpt)
working_data <- dplyr::left_join(working_data,servdict,relationship="many-to-many")
rm(servdict)
gc()
working_data$service <- C(tidyr::replace_na(working_data$service,replace="NonDDoS"))
working_data$ddosport <- C(tidyr::replace_na(working_data$ddosport,replace="0"))

saveRDS(working_data,"data/tempdata/working_data.RDS",compress="gzip",refhook=NULL)
working_data$datetimes1 <- as.double.POSIXlt(working_data$datetimes)
working_data$mu10s <- as.POSIXct(working_data$datetimes1 * 10)
working_data$mus <- as.POSIXct(working_data$datetimes1 / 10)
gc(F,T,T)


F1 <- working_data |>
  dplyr::mutate("V1"=NULL) |>
  dplyr::add_count(mus,datetimes1,datetime,protocol,service,ccrating,ddosport,name="PPmus",wt=NULL,.drop=T) 

  F1$mus <- as.double.difftime(F1$mus)
 F1$ms <-  F1$PPmus  *1000 / F1$mus
 
F2 <- F1 |>
  dplyr::add_count(ms,mus,service,ccrating,ddosport,name="PPms",wt=NULL,.drop=T) 


F1$PPms <- F1$PPS*100
exp(1)

dplyr::consecutive_id(F1$seconds,F1$datetime)

F2 <- F1 |>
  dplyr::count(,name="PPms",wt=NULL) 

F1

t(F1)
ts(F1)
gc()



F2 <- working_data |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::add_count(seconds,name="IP_PS",wt=NULL)
gc()

F3 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(seconds,region,cc) |>
  dplyr::group_by(seconds,region,cc) |>
  dplyr::add_count(region,cc,seconds,name="RPS",wt=NULL)
gc()

F4 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(seconds,host) |>
  dplyr::group_by(seconds,host) |>
  dplyr::add_count(host,name="DPS",wt=NULL)
gc()

F5 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(seconds,protocol,port,ddosport,service) |>
  dplyr::group_by(seconds,ddosport,port,service) |>
  dplyr::count(seconds,ddosport,port,service,name="ProtPS",wt=NULL,.drop=T)

labeled_data <- data.frame(cbind(F1$IP_PS,F2$PPS,F3$RPS,F4$DPS,F5$ProtPS, working_data$ddosport,as.numeric(factor(working_data$host)),as.numeric(factor(working_data$protocol))))

log(F9$ProtPS+1)
exp(F9$ProtPS+1)
labeled_data$X1


levels(X1)

unique(working_data$host)
as.factor(working_data$host)
unique.matrix(labeled_data,)
as.data.frame.factor(t(labeled_data))
transform.data.frame(labeled_data)
format.data.frame(labeled_data, na.encode = TRUE)
tt<-format.AsIs(t(labeled_data))

as.data.frame.raw(labeled_data,optional=T)
as.data.frame(labeled_data,optional=T)
as.data.frame.ts(labeled_data)

as.data.frame.model.matrix(labeled_data,make.names=T,optional=T)
test <- as.data.frame(labeled_data,fix.empty.names = TRUE)
test <- as.matrix(labeled_data,rownames.force=T)
t(test) %*% test

rm(F1,F2,F3,F4,F5)
gc(F,T,T)
rm(F6,F7,F8,F9)
gc(F,T,T)



labeled_data <- readRDS("labled_data.RDS")

ts
(stats::
    
    
    labeled_data <- working_data1[,15:24]
  as.fa
  labeled_data <- t(labeled_data)
  stats::
    
    as.factor(ea)
  eapply(as.factor(),env=labeled_data[,18],all.names=T,USE.NAMES=T)
  gc(F,T,T)
  save(working_data,file="data/internal/temp/working_data.RDA",compress="gzip")
  
  labeled_data <- working_data[,c(3:15)]
  
  rm(working_data)
  gc(F,T,T)
  
  save(labeled_data,file="data/internal/temp/labled_data.RDA",compress="gzip")
  rm(labeled_data)
  
  
  workdata_savedb = list(rda_path="data/internal/temp/working_data.RDA",rda_name="working_data.RDA",db_path="data/internal/datasets.db",db_name="main_datasets",file_name="file_name")
  save_db(workdata_savedb$rda_path,workdata_savedb$rda_name,workdata_savedb$db_path,workdata_savedb$db_name,workdata_savedb$file_name)
  rm(workdata_savedb)
  gc(F,T,T)
  
  lableddata_savedb = list(rda_path="data/internal/temp/labled_data.RDA",rda_name="labled_data.RDA",db_path="data/internal/datasets.db",db_name="main_datasets",file_name="file_name")
  save_db(lableddata_savedb$rda_path,lableddata_savedb$rda_name,lableddata_savedb$db_path,lableddata_savedb$db_name,lableddata_savedb$file_name)
  rm(lableddata_savedb)
  gc(F,T,T)
  
  