install.packages("installr")
install.packages("stringr")
install.packages("stringi")



# 数据环境================================================================ 
library(DBI)
library(RMySQL)

# 数据库环境设置
conn1<-dbConnect(MySQL(),dbname="Channe_sales_analysis",host="8888888888",username="88888",password="8888888")
dbSendQuery(conn1,"SET NAMES gbk")

# 设置工作目录
dir<-"E:/2018年碧生源工作文档/流向数据—医药渠道分析"
setwd(dir)
getwd()


# 目标数据导入
fetch_sql<-dbSendQuery(conn1,"select * from 医药渠道分析原始表")
dataset_term<-fetch(fetch_sql,n=-1)

# 数据清洗
for(i in 0:nrow(dataset_term))
{
  dataset_term$终端标准名称[i]<-trimws(gsub("[0-9]|' '|(|)|[a-zA-Z]","",dataset_term$终端名称[i]))
}


# 数据写入
dbWriteTable(conn1,"医药渠道分析表",dataset_term,append = TRUE, row.names = FALSE)
dbDisconnect(conn1)
