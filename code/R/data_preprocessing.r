require("reticulate")
require("dplyr")
require("tidyr")
require("data.table")
clrmem(3)

selectedcols<-c("datetime,host,proto,spt,dpt,srcstr,country,latitude,longitude")
raw_data <- data.table::fread("data/internal/AWS_Honeypot_marx-geo.csv",sep=",",quote="\"",header=T,select=c("datetime", "host", "proto", "spt", "dpt", "srcstr", "country", "locale", "localeabbr", "postalcode", "longitude", "latitude"), colClasses=list(character=c(1:5,8:13),integer=6:7,double=14:15,NULL=16),encoding="UTF-8",key=selectedcols,index=selectedcols,data.table=T,nThread= (parallel::detectCores()-1),nrows=451581)
rm(selectedcols)
gc()

raw_data <- raw_data |> 
  dplyr::mutate("datetimes"= c(raw_data$datetime),"dates"= c(strptime(as.character(raw_data$datetime), format = "%m/%d/%y"))) |>
  tidyr::separate(datetimes, into = c("date", "time"), sep = " ") |>
  dplyr::mutate("host"=gsub("-", "_", raw_data$host),"dpt"= c(tidyr::replace_na(raw_data$spt,99999)),"spt"= c(tidyr::replace_na(raw_data$dpt,99999)))

gc(F,T,T)

working_data <- raw_data |>
  dplyr::select(datetime,date,time,host,srcstr,proto,spt,dpt,longitude,latitude,country,dates) |>
  dplyr::group_by() |>
  dplyr::arrange(.by_group=T) |>
  dplyr::rename("region"="country","long"="longitude","lat"="latitude") 

rm(raw_data)
clrmem(2)

na_indices<-which(is.na(working_data$region))
ips_to_check<-data.frame(cbind(working_data$datetime[na_indices],working_data$region[na_indices],working_data$srcstr[na_indices],working_data$date[na_indices],working_data$time[na_indices],working_data$spt[na_indices],working_data$proto[na_indices],working_data$host[na_indices]))

reticulate::import("sqlite3",convert=T,delay_load=T)
reticulate::import("pandas",as="pd",convert=T,delay_load=T)
reticulate::import("geoip2",convert=T,delay_load=T)

ips_to_check1<- sapply(ips_to_check[,3], reticulate::py$get_country)
rm(ips_to_check)
clrmem(3)

ips_to_check2<- stringi::stri_list2matrix(ips_to_check1)
ips<-unlist(c(attributes(ips_to_check1)))
ips<- stringi::stri_replace_last(ips,replacement = c(""),fixed = ".country")

rm(ips_to_check1)
clrmem(3)

ips_to_check2<-t(ips_to_check2)
country_ips<-data.frame("ips"=c(ips),"cnames"=c(ips_to_check2))

rm(ips_to_check2,na_indices)
clrmem(3)

matched_index <- match(country_ips[,1],working_data$srcstr)
working_data$region <- replace(working_data$region,matched_index,country_ips[,2])
rm(ips,matched_index,country_ips)
clrmem(3)

day<-reorder( stringi::stri_sub(as.character(working_data$dates),from=9L,length=2,ignore_negative_length=F),working_data$datetime)

working_data <- working_data |>
  dplyr::rename("port"="spt","protocol"="proto") |>
  dplyr::mutate("dpt"=NULL,"src"=NULL,"month"=c(months.Date(working_data$dates,abbreviate=T)),"day"=day) |>
  dplyr::arrange(datetime) |>
  dplyr::mutate("dates"=NULL)

rm(day)
clrmem(3)

Ports<-c("1433","99999","445","3389","80","56338","8080","22","3306","2193","135","53","23","5060","6666","443","3128","5900","19","1469","21","25","110","143","7","389","123","68","5432","1434","1900","5353","161","179","139","137","111","0","13","9","3","2","1","37","33","26","38","42")
Services<-c("MSSQL_Server","ICMP","SMB","RDP","HTTP","UDP_Flood1","HTTPAlt","SSH","MySQL","UDP_Flood2","RPC","DNS","Telnet","SIP","IRC","HTTPS","Squid_Proxy","VNC","CHARGEN","AAL_LM","FTP","SMTP","POP3","IMAP","Echo","LDAP","NTP","DHCPClient","PostgreSQL","MSSQL_Monitor","SSDP","MDNS","SNMP","BGP","NETBIOS_ssh","NETBIOS_ns","RCPBind","Reserved","Daytime","Discard","compressnet","compressnet","tcpnux","time","dsp","unassigned","rap","nameserver_WIN")

servdict<-data.frame(cbind(c(1:48),Ports,Services),portsnum=as.numeric(Ports))

working_data$servindex <- c(match(working_data$port,servdict$portsnum,nomatch = NA))
working_data<-merge(working_data,servdict,no.dups=F,incomparables="NA",by.x="servindex",by.y="V1",all=F)
working_data<-working_data |> dplyr::mutate("ports"=NULL,"portsnum"=NULL,"servindex"=NULL) 

rm(Ports,Services,servdict)
clrmem(3)

portmatchdata<- data.table::fread("data/external/service-names-port-numbers.csv",nThread= (parallel::detectCores(logical=T)-1),quote="\"",skip=1,sep=c(","),select=c(1,2,3),col.names=c("service","port","protocol"),colClasses=list(character=c(1,2,3),NULL=4:12),blank.lines.skip=T,na.strings=c("",",,,","NA"),header=T,data.table=T)
portmatchdata<-na.omit(portmatchdata)

portmatchdata$protocol <- toupper(portmatchdata$protocol)
portmatchdata$port <- as.character(portmatchdata$port)

working_data<- dplyr::left_join(working_data,portmatchdata,by = c("port","protocol"))

rm(portmatchdata)
clrmem(3)

working_data<-working_data |> dplyr::mutate("service"=NULL,"Ports"=NULL,"year"=NULL) |> dplyr::rename("service"="Services") |> dplyr::group_by(datetime) |> dplyr::arrange(.by_group=T)

save(working_data,file="data/internal/temp/working_data.RDA",compress="gzip")
rm(working_data)
clrmem(3)

workdata_savedb = list(rda_path="data/internal/temp/working_data.RDA",rda_name="working_data.RDA",db_path="data/internal/datasets.db",db_name="main_datasets",file_name="file_name")
save_db(workdata_savedb$rda_path,workdata_savedb$rda_name,workdata_savedb$db_path,workdata_savedb$db_name,workdata_savedb$file_name)

rm(workdata_savedb)
clrmem(3)

reticulate::py_run_file("code/Python/functions.py")
source("code/R/functions.r")
reticulate::py_run_file("code/Python/extract_multithreaded.py")

load("data/internal/temp/workingdata_restored.RDA")
gc()

hostarray <- working_data |>
  dplyr::select(datetime,date,time,host,srcstr,service,protocol,port,long,lat,region,month,day) |>
  dplyr::ungroup() |>
  tidyr::nest(.by=host,.key=NULL)

save(hostarray,file="data/internal/temp/hostarray.RDA",compress="gzip")
rm(working_data,hostarray)
clrmem(3)

hostaray_savedb = list(rda_path="data/internal/temp/hostarray.RDA",rda_name="hostarray.RDA",db_path="data/internal/datasets.db",db_name="main_datasets",file_name="file_name")
save_db(hostaray_savedb$rda_path,hostaray_savedb$rda_name,hostaray_savedb$db_path,hostaray_savedb$db_name,hostaray_savedb$file_name)
rm(hostaray_savedb)
clrmem(3)

#file.remove("data/internal/temp/HostArray.RDA")
#file.remove("data/internal/temp/working_data.RDA")
#clrmem(1)