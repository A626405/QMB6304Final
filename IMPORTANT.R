source("code/R/functions.r")
create_db()
reticulate::py_run_file("code/Python/functions.py")
gc(F,F,T)

selectedcols<-c("datetime", "host", "proto", "spt", "dpt", "srcstr", "cc", "country", "latitude", "longitude")
raw_data <- data.table::fread("data/internal/AWS_Honeypot_marx-geo.csv",quote="\"",strip.white=T,fill=T,blank.lines.skip=T,header=T,na.strings=c("NA",NULL,"",",,"),keepLeadingZeros=T,select=c("datetime", "host", "proto", "spt", "dpt", "srcstr", "cc", "country", "longitude", "latitude"), colClasses=list(character=c(1:5,8:13),integer=6:7,double=14:15,NULL=16),data.table=T,nThread= (parallel::detectCores()-1),nrows=451581)
rm(selectedcols)
gc(F,F,T)

#options("digits.secs"=0,"max.print"=1000,datatable.optimize=T)

datetimes <- as.POSIXlt(raw_data$datetime, format="%m/%d/%y %M:%OS",tz="")
datetimes <- lubridate::as_datetime(datetimes,tz="UTC")
#formatted_datetime  <- as.POSIXlt(format(datetimes,format= "%Y-%m-%d %H:%M:%S.%Os", tz="", usetz=F))
raw_data$datetimes <- as.double(datetimes)
rm(datetimes)
gc(F,F,T)

working_data <- raw_data |>
  dplyr::mutate("host"=gsub("-", "_", raw_data$host),"dpt"= c(tidyr::replace_na(raw_data$spt,99999)),"spt"= c(tidyr::replace_na(raw_data$dpt,99999)))|>
  dplyr::select(datetime,datetimes,srcstr,proto,host,spt,dpt,longitude,latitude,cc,country) |>
  dplyr::group_by() |>
  dplyr::arrange(.by_group=T) |>
  dplyr::rename("region"="country","long"="longitude","lat"="latitude") 
rm(raw_data)
gc(F,F,T)

cc_countrydf <- data.table::fread("data/external/all.csv",sep=",",quote="\"",select=c(1,2),col.names=c("region", "cc"),keepLeadingZeros=F,na.strings=c("NA","",",,"),data.table=T,nThread= (parallel::detectCores()-1))

region <- stringi::stri_replace_first(working_data$region,fixed=c("country\n0 "),replacement="")
region <- stringi::stri_replace_first(region,fixed=c("0 "),replacement="")
working_data<-working_data[region %in% working_data$region, ]
rm(region)
gc(F,F,T)
working_data <- dplyr::left_join(working_data,cc_countrydf,copy=F,keep=F) 
rm(cc_countrydf)
gc(F,T,T)

na_indices<-which(is.na(working_data$region))
ips_to_check<-data.frame(cbind(working_data$datetime[na_indices],working_data$region[na_indices],working_data$srcstr[na_indices],working_data$spt[na_indices],working_data$proto[na_indices],working_data$host[na_indices]))
rm(na_indices)
gc()

require(reticulate,quietly=T,include.only=T)
reticulate::import("sqlite3",convert=T,delay_load=T)
reticulate::import("pandas",as="pd",convert=T,delay_load=T)
reticulate::import("geoip2",convert=T,delay_load=T)

ips_to_check1<- sapply(ips_to_check[,3], reticulate::py$get_country)
rm(ips_to_check)
gc(F,F,T)

ips_to_check2 <- stringi::stri_list2matrix(ips_to_check1)
ips <-unlist(c(attributes(ips_to_check1)))
ips <- stringi::stri_replace_last(ips,replacement = c(""),fixed = ".country")
rm(ips_to_check1)
gc(F,F,T)

ips_to_check2<-t(ips_to_check2)
country_ips<- data.frame("srcstr"=c(ips),"region"=c(ips_to_check2))
rm(ips_to_check2,ips)
gc(F,F,T)

matched_index <- match(country_ips$srcstr,working_data$srcstr)
working_data<- dplyr::left_join(working_data,country_ips)
working_data$region <- replace(working_data$region,matched_index,country_ips$region)
rm(matched_index,country_ips)
gc(F,F,T)

cc_countrydf <- data.table::fread(input= "data/external/GeoLite2-Country-CSV_20250103/GeoLite2-Country-Locations-en.csv",select=c(1,5,6),col.names=c("geoname_id", "cc", "region"), sep=",",quote="\"",header=T,strip.white=T,keepLeadingZeros=F,data.table=T,colClasses=c(character(7)),nThread=(parallel::detectCores() - 1))
working_data <- dplyr::left_join(working_data,cc_countrydf)
rm(cc_countrydf)
gc(F,F,T)

working_data <- working_data |>
  dplyr::rename("port"="spt","protocol"="proto") |>
  dplyr::mutate("dpt"=NULL,"src"=NULL,"dates"=NULL,"geoname_id"=NULL) |>
  dplyr::arrange(datetimes) 
gc(F,F,T)

port<-c("1433","445","3389","3306","135","53","5060","5900","123","389","5432","1434","1900","5353","11211","19","137","138","161","162","500","1900","3702","5683","20800","3283","7547","11211","7100")
service<-c("MSSQL","SMB","RDP","MSSQL","RPC","DNS","SIP","VNC","NTP","LDAP","MSSQL","MSSQL","SSDP","mDNS","Memcached","Chargen","NetBIOS_NS","NetBIOS_DGM","SNMP","SNMP_Trap","ISAKMP","SSDP","WS_Discovery","CoAP","CompuServe","NetAssistant","TR_069","Memcached","X11")
protocol<-c("UDP","TCP","TCP","UDP","TCP","UDP","UDP","TCP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP")

ddosport <- c(rep(1,length(port)))
servdict<-data.frame(cbind(ddosport,port,service))
rm(port,service,protocol,ddosport)
gc(F,T,T)

working_data$port <- as.character(working_data$port)
working_data <- dplyr::left_join(working_data,servdict,relationship="many-to-many")
rm(servdict)
gc()

working_data$service <- c(tidyr::replace_na(working_data$service,replace="NonDDoS"))
working_data$ddosport <- c(tidyr::replace_na(working_data$ddosport,replace="0"))
working_data$microseconds <- as.character.POSIXt(working_data$datetimes)
gc(F,T,T)

F1  <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(microseconds,srcstr) |>
  dplyr::group_by(microseconds,srcstr) |>
  dplyr::add_count(srcstr,microseconds,name="IP_PKms",wt=NULL)
gc()

F2 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(microseconds,port) |>
  dplyr::group_by(microseconds,port) |>
  dplyr::add_count(port,microseconds,name="PPKms",wt=NULL)
gc()

working_data$milliseconds <- as.double.POSIXlt(working_data$datetimes / 1e3)
F3 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(milliseconds,srcstr) |>
  dplyr::group_by(milliseconds,srcstr) |>
  dplyr::add_count(srcstr,milliseconds,name="IP_Pms",wt=NULL)
gc()

F4 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(milliseconds,port) |>
  dplyr::group_by(milliseconds,port) |>
  dplyr::add_count(port,milliseconds,name="PPms",wt=NULL)
gc(F,F,T)

working_data$seconds <- as.double.POSIXlt(working_data$datetimes / 1e6)

F5 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(seconds,srcstr) |>
  dplyr::group_by(seconds,srcstr) |>
  dplyr::add_count(srcstr,seconds,name="IP_PS",wt=NULL)
gc()

F6 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(seconds,port) |>
  dplyr::group_by(seconds,port) |>
  dplyr::add_count(port,seconds,name="PPS",wt=NULL)
gc(F,F,T)

F7 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(seconds,region,cc) |>
  dplyr::group_by(seconds,region,cc) |>
  dplyr::add_count(region,cc,seconds,name="RPS",wt=NULL)
gc()

F8 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(seconds,host,protocol) |>
  dplyr::group_by(seconds,host,protocol) |>
  dplyr::add_count(host,protocol,seconds,name="DPS",wt=NULL)
gc()

F9 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL) |>
  dplyr::select(seconds,protocol) |>
  dplyr::group_by(seconds,protocol) |>
  dplyr::add_count(protocol,seconds,name="ProtPS",wt=NULL)

labeled_data <- data.frame(cbind(F1$IP_PKms,F2$PPKms,F3$IP_Pms,F4$PPms,F5$IP_PS,F6$PPS,F7$RPS,F8$DPS,F9$ProtPS, working_data$ddosport,as.numeric(factor(working_data$host)),as.numeric(factor(working_data$protocol))))
log(as.numeric(labeled_data$X1))
log(as.numeric(labeled_data$X2))
log(as.numeric(labeled_data$X3))
log(as.numeric(labeled_data$X4))
log(as.numeric(labeled_data$X5))
log(as.numeric(labeled_data$X6))
log(as.numeric(labeled_data$X7))
log(as.numeric(labeled_data$X8))
log(as.numeric(labeled_data$X9))
cor(as.numeric(labeled_data$X1),as.numeric(labeled_data$X2))
working_data$host


transform.data.frame(labeled_data,)
format.data.frame(labeled_data, na.encode = TRUE)
tt<-format.AsIs(t(labeled_data))

as.data.frame.raw(na.omit(labeled_data),optional=T)
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

