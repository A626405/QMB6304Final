source("code/R/functions.r")
create_db()
reticulate::py_run_file("code/Python/functions.py")


selectedcols<-c("datetime", "host", "proto", "spt", "dpt", "srcstr", "cc", "country", "latitude", "longitude")
raw_data <- data.table::fread("data/internal/AWS_Honeypot_marx-geo.csv",sep=",",quote="\"",strip.white=T,fill=T,blank.lines.skip=T,header=T,na.strings=c("NA",NULL,"",",,"),keepLeadingZeros=F,select=c("datetime", "host", "proto", "spt", "dpt", "srcstr", "cc", "country", "longitude", "latitude"), colClasses=list(character=c(1:5,8:13),integer=6:7,double=14:15,NULL=16),data.table=T,nThread= (parallel::detectCores()-1),nrows=451581)
rm(selectedcols)

#options(nsize = 18.8e6,vsize=8e7)  # ~512MB for ncells
gc(F,T,T)

working_data <- raw_data |>
  dplyr::mutate("host"=gsub("-", "_", raw_data$host),"dpt"= c(tidyr::replace_na(raw_data$spt,99999)),"spt"= c(tidyr::replace_na(raw_data$dpt,99999)))|>
  dplyr::select(datetime,srcstr,proto,spt,dpt,longitude,latitude,cc,country) |>
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
ips_to_check<-data.frame(cbind(working_data$datetime[na_indices],working_data$region[na_indices],working_data$srcstr[na_indices],working_data$spt[na_indices],working_data$proto[na_indices],working_data$host[na_indices]))

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

working_data <- working_data |> dplyr::mutate("geoname_id"=NULL)
gc(F,F,T)

working_data <- working_data |>
  dplyr::rename("port"="spt","protocol"="proto") |>
  dplyr::mutate("dpt"=NULL,"src"=NULL,"dates"=NULL) |>
  dplyr::arrange(datetime) 

gc(F,F,T)

port<-c("1433","445","3389","3306","135","53","5060","5900","123","389","5432","1434","1900","5353","11211",
        "19","137","138","161","162","500","1900","3702","5683","20800","3283","7547","11211","7100")
service<-c("MSSQL","SMB","RDP","MSSQL","RPC","DNS","SIP","VNC","NTP","LDAP","MSSQL","MSSQL","SSDP","mDNS","Memcached",
           "Chargen","NetBIOS_NS","NetBIOS_DGM","SNMP","SNMP_Trap","ISAKMP","SSDP","WS_Discovery","CoAP","CompuServe",
           "NetAssistant","TR_069","Memcached","X11")
protocol<-c("UDP","TCP","TCP","UDP","TCP","UDP","UDP","TCP","UDP","UDP","UDP","UDP","UDP","UDP","UDP",
            "UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP","UDP")

ddosport <- c(rep(1,length(port)))
servdict<-data.frame(cbind(ddosport,port,service))

working_data$port <- as.character(working_data$port)
working_data <- dplyr::left_join(working_data,servdict,relationship="many-to-many")
working_data$service <- c(tidyr::replace_na(working_data$service,replace="NonDDoS"))
working_data$ddosport <- c(tidyr::replace_na(working_data$ddosport,replace="0"))


rm(port,service,servdict)
gc(F,F,T)

datetimes <- as.POSIXct(strptime(working_data$datetime, "%m/%d/%y %H:%M"))
working_data$datetime1 <- lubridate::floor_date(datetimes,unit="1 minute")
working_data$datetime5 <- lubridate::floor_date(datetimes,unit="5 minute")
rm(datetimes)
gc(F,T,T)

#working_data <- as.ts(working_data)

working_data <- working_data |>
  dplyr::ungroup() |>
  dplyr::select(datetime5,datetime1,service,protocol,port,ddosport,srcstr,region,region,cc,long,lat) |>
  dplyr::mutate("V1"=NULL,"portsnum"=NULL,"datetime"=NULL) |>
  dplyr::group_by(datetime5,datetime1,service,protocol,ddosport) |>
  dplyr::add_count(ddosport,service,protocol,datetime1,name="countservice_1min",wt=NULL)

gc(F,T,T)

working_data <- working_data |>
  dplyr::select(datetime5,datetime1,service,protocol,port,ddosport,srcstr,region,region,cc,long,lat,countservice_1min) |>
  dplyr::group_by(datetime5,service,protocol,ddosport) |>
  dplyr::add_count(ddosport,service,protocol,datetime5,name="countservice_5min",wt=NULL) 

working_data <- working_data |>
  dplyr::select(datetime5,datetime1,service,protocol,port,ddosport,srcstr,region,region,cc,long,lat,countservice_1min,countservice_5min,) |>
  dplyr::group_by(datetime5,service,protocol,ddosport) |>
  dplyr::add_count(region,cc,protocol,datetime5,name="countregion_5min",wt=NULL)

working_data <- working_data |> 
  dplyr::select(datetime5,datetime1,service,protocol,port,ddosport,srcstr,region,region,cc,long,lat,countservice_1min,countservice_5min,countregion_5min) |>
  dplyr::add_count(srcstr,protocol,datetime5,name="countip_5min",wt=NULL)

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

