require(DBI)
require(RSQLite)
require(reticulate)
require(dplyr)
require(tidyr)
require(tibble)
mem.maxVSize(vsize=636)


source("code/R/functions.r")
reticulate::py_run_file("code/Python/functions.py")
#reg.finalizer(e, f, onexit = FALSE)

selectedcols<-c("datetime", "host", "proto", "spt", "dpt", "srcstr", "cc", "country", "latitude", "longitude")
raw_data <- data.table::fread("data/internal/AWS_Honeypot_marx-geo.csv",sep=",",quote="\"",strip.white=T,fill=T,blank.lines.skip=T,header=T,na.strings=c("NA",NULL,"",",,"),keepLeadingZeros=F,select=c("datetime", "host", "proto", "spt", "dpt", "srcstr", "cc", "country", "longitude", "latitude"), colClasses=list(character=c(1:5,8:13),integer=6:7,double=14:15,NULL=16),data.table=T,nThread= (parallel::detectCores()-1),nrows=451581)
rm(selectedcols)
gc()

raw_data <- raw_data |> 
  dplyr::mutate("datetimes"= c(raw_data$datetime),"dates"= c(strptime(as.character(raw_data$datetime), format = "%m/%d/%y"))) |>
  tidyr::separate(datetimes, into = c("date", "time"), sep = " ") |>
  dplyr::mutate("host"=gsub("-", "_", raw_data$host),"dpt"= c(tidyr::replace_na(raw_data$spt,99999)),"spt"= c(tidyr::replace_na(raw_data$dpt,99999)))

gc(F,T,T)

working_data <- raw_data |>
  dplyr::select(datetime,date,time,host,srcstr,proto,spt,dpt,longitude,latitude,cc,country,dates) |>
  dplyr::group_by() |>
  dplyr::arrange(.by_group=T) |>
  dplyr::rename("region"="country","long"="longitude","lat"="latitude") 

rm(raw_data)
clrmem(2)


cc_countrydf <- data.table::fread("data/external/all.csv",sep=",",quote="\"",select=c(1,2),col.names=c("region", "cc"),keepLeadingZeros=T,na.strings=c("NA","",",,"),encoding="UTF-8",data.table=T,nThread= (parallel::detectCores()-1))

region<-working_data$region
region <- stringi::stri_replace_first(working_data$region,fixed=c("country\n0 "),replacement="")
region <- stringi::stri_replace_first(region,fixed=c("0 "),replacement="")
working_data<-working_data[region %in% working_data$region, ]
rm(region)
gc(F,T,T)

working_data <- dplyr::left_join(working_data,cc_countrydf,copy=F,keep=F) 
rm(cc_countrydf)
gc(F,F,T)

na_indices<-which(is.na(working_data$region))
ips_to_check<-data.frame(cbind(working_data$datetime[na_indices],working_data$region[na_indices],working_data$srcstr[na_indices],working_data$date[na_indices],working_data$time[na_indices],working_data$spt[na_indices],working_data$proto[na_indices],working_data$host[na_indices]))

reticulate::import("sqlite3",convert=T,delay_load=T)
reticulate::import("pandas",as="pd",convert=T,delay_load=T)
reticulate::import("geoip2",convert=T,delay_load=T)

ips_to_check1<- sapply(ips_to_check[,3], reticulate::py$get_country)
rm(ips_to_check)
gc(F,T,T)

ips_to_check2 <- stringi::stri_list2matrix(ips_to_check1)
ips <-unlist(c(attributes(ips_to_check1)))
ips <- stringi::stri_replace_last(ips,replacement = c(""),fixed = ".country")

rm(ips_to_check1)
clrmem(3)

ips_to_check2<-t(ips_to_check2)
country_ips<-data.frame("ips"=c(ips),"cnames"=c(ips_to_check2))

rm(ips_to_check2,na_indices)
gc(F,T,T)

matched_index <- match(country_ips[,1],working_data$srcstr)
working_data$region <- replace(working_data$region,matched_index,country_ips[,2])
rm(ips,matched_index,country_ips)
gc(F,T,T)


cc_countrydf <- data.table::fread(input= "data/external/GeoLite2-Country-CSV_20250103/GeoLite2-Country-Locations-en.csv",select=c(1,5,6),col.names=c("geoname_id", "cc", "region"), sep=",",quote="\"",header=T,keepLeadingZeros=T,data.table=T,colClasses=c(character(7)),nThread=(parallel::detectCores() - 1))
working_data <- dplyr::left_join(working_data,cc_countrydf)
rm(cc_countrydf)
gc(F,T,T)

working_data <- working_data |> dplyr::mutate(geoname_id=NULL)
gc(F,F,T)

day<- reorder( stringi::stri_sub(as.character(working_data$dates),from=9L,length=2,ignore_negative_length=F),working_data$datetime)

working_data <- working_data |>
  dplyr::rename("port"="spt","protocol"="proto") |>
  dplyr::mutate("dpt"=NULL,"src"=NULL,"month"=c(months.Date(working_data$dates,abbreviate=T)),"day"=day,"dates"=NULL, "year" = c(rep("2013",nrow(working_data)))) |>
  dplyr::arrange(datetime) 

rm(day)
gc(F,F,T)

port<-c("1433","99999","445","3389","80","56338","8080","22","3306","2193","135","53","23","5060","6666","443","3128","5900","19","1469","21","25","110","143","7","389","123","68","5432","1434","1900","5353","161","179","139","137","111","0","13","9","3","2","1","37","33","26","38","42")
service<-c("MSSQL_Server","ICMP","SMB","RDP","HTTP","UDP_Flood1","HTTPAlt","SSH","MySQL","UDP_Flood2","RPC","DNS","Telnet","SIP","IRC","HTTPS","Squid_Proxy","VNC","CHARGEN","AAL_LM","FTP","SMTP","POP3","IMAP","Echo","LDAP","NTP","DHCPClient","PostgreSQL","MSSQL_Monitor","SSDP","MDNS","SNMP","BGP","NETBIOS_ssh","NETBIOS_ns","RCPBind","Reserved","Daytime","Discard","compressnet","compressnet","tcpnux","time","dsp","unassigned","rap","nameserver_WIN")

servdict<-data.frame(cbind(c(1:48),port,service),portsnum=as.numeric(port))

working_data$port <- as.character(working_data$port)
working_data<- dplyr::left_join(working_data,servdict)

rm(port,service,servdict)
gc(F,F,T)

portmatchdata<- data.table::fread("data/external/service-names-port-numbers.csv",nThread= (parallel::detectCores(logical=T)-1),quote="\"",skip=1,sep=c(","),select=c(1,2,3),col.names=c("service","port","protocol"),colClasses=list(character=c(1,2,3),NULL=4:12),blank.lines.skip=T,na.strings=c("",",,,","NA"),header=T,data.table=T)
portmatchdata<-na.omit(portmatchdata)

portmatchdata$protocol <- toupper(portmatchdata$protocol)
portmatchdata$port <- as.character(portmatchdata$port)

working_data<- dplyr::left_join(working_data,portmatchdata)
rm(portmatchdata)
gc(F,T,T)

dates<- c(strptime(as.character(working_data$datetime), format = "%m/%d/%y"))
dates<- lubridate::floor_date(dates,unit="1 minute")

working_data1 <- working_data |>
  dplyr::ungroup() |>
  dplyr::mutate("time_interval"=c(dates),"V1"=NULL,"portsnum"=NULL) |>
  dplyr::group_by(date,month,day,time,host,time_interval,port,protocol,srcstr,service,.drop=F) |>
  dplyr::add_count(srcstr,port,date,month,day,time,host,name="countip_bymin",.drop=F) |>
  dplyr::distinct(.keep_all=F)

rm(working_data,dates)
gc(F,T,T)

#working_data2 <- which(working_data1[working_data1$countip_bymin,]==T)


working_data1$seqports <- as.character(factor(dplyr::consecutive_id(working_data1$date,working_data1$month,working_data1$day,working_data1$protocol,working_data1$port,working_data1$host,working_data1$countip_bymin,working_data1$region,working_data1$cc,working_data1$service)))

working_data1$time_interval = NULL

working_data3 <- working_data1 |>
  dplyr::group_by(date,time,month,day,host,port,protocol,srcstr,countip_bymin,seqports,service) |>
  dplyr::count(seqports,port,protocol,host,time,date,month,day,name="countipsperport",.drop=T) 

working_data1$seqportip <- as.character.factor(factor(dplyr::consecutive_id(working_data1$date,working_data1$month,working_data1$day,working_data1$protocol,working_data1$port,working_data1$srcstr,working_data1$countip_bymin)))

rm(working_data1)
gc(F,T,T)


save(working_data,file="data/internal/temp/working_data.RDA",compress="gzip")
rm(working_data)
gc(F,T,T)

workdata_savedb = list(rda_path="data/internal/temp/working_data.RDA",rda_name="working_data.RDA",db_path="data/internal/datasets.db",db_name="main_datasets",file_name="file_name")
save_db(workdata_savedb$rda_path,workdata_savedb$rda_name,workdata_savedb$db_path,workdata_savedb$db_name,workdata_savedb$file_name)

rm(workdata_savedb)
gc(F,F,T)

