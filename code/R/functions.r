options(verbose=F,echo=F,renv.consent=T,PCRE_use_JIT=T,prompt="y ",repos="https://cloud.r-project.org")

load_lb<-function(LibList){
  Lib <- as.list(LibList)
  for(i in 1:length(Lib)){
    require(Lib[[i]],character.only=T)}}


clrmem <- function(select_123){
  require(reticulate,include.only=T)
  
  if(select_123==1){
  gc(F,T,T)
  cat("\014")
  gc()
  
  objs_to_remove <- ls(all.names = TRUE, envir = .GlobalEnv)
  objs_to_remove <- objs_to_remove[!grepl("^renv", objs_to_remove)]
  rm(list = objs_to_remove, envir = .GlobalEnv)
  gc(F,F,T)
  
  options(verbose=F,echo=F,renv.consent=T,PCRE_use_JIT=T,prompt="y ",repos="https://cloud.r-project.org")
  reticulate::py_run_file("code/Python/functions.py")
  source("code/R/functions.r")
  load_lb(c("dplyr","tidyr"))
  gc(F,T,T)
  cat("\014")
  
  }else if(select_123==2){
    gc()
    cat("\014")
    gc(F,T,T)
    
  }else if(select_123==3){
    gc()
    cat("\014")
    gc()
  }else{print("Incorrect Selection")}}



create_db <- function(dbpath){
  require(reticulate,include.only=T)
  dbpath<-file.path(dbpath)
  if(!file.exists("data/internal/datasets.db")){
    reticulate::py$create_db(dbpath)
    gc(F,T,T)
  }else{
    print("The Database Already Exists.")
  }
}

save_db <- function(rda_path,rda_name,db_path,tbl_name,col_name){
  require(DBI,include.only=T)
  require(RSQLite,include.only=T)
  require(reticulate,include.only=T)
  conn <- DBI::dbConnect(RSQLite::SQLite(),db_path)
  current_dbs <- c(RSQLite::sqliteQuickColumn(conn,tbl_name,col_name))
  DBI::dbDisconnect(conn)
  gc(F,T,T)
  if(!(rda_name %in% current_dbs)){
    reticulate::py$write_db(rda_path,rda_name,db_path,tbl_name)
    gc(F,T,T)
  }else{print("Error! Dataframe Already Exists In Database.")}}
#
#
#getlibs <- function(pkgs_charlist) {
#  Lib <- as.list(pkgs_charlist)
#  require(doParallel)
#  require(parallel)
#  num_cores <- parallel::detectCores() - 1
#  cl <- parallel::makeCluster(num_cores)
#  doParallel::registerDoParallel(cl)
#  parallel::mclapply(Lib, function(pkg){
#          require(pkg,character.only=T)
#  },mc.cores = num_cores,mc.preschedule=T)  
#  parallel::stopCluster(cl)
#}
#
#load_db <- function(path_to_db_char,file_name){
#  data=list(path_to_db_char,file_name)
#  py$read_db(data$path_to_db_char,data$file_name) 
#}