install.packages("installr")
install.packages("stringr")
install.packages("stringi")



# 数据环境================================================================ 
library(DBI)
library(RMySQL)

# 数据库环境设置


# 设置工作目录
dir<-"E:/2018年碧生源工作文档/流向数据—医药渠道分析"
setwd(dir)
getwd()


# 目标数据导入
conn1<-dbConnect(MySQL(),dbname="Channe_sales_analysis",host="192.168.111.251",username="***",password="*******")
dbSendQuery(conn1,"SET NAMES gbk")
fetch_sql<-dbSendQuery(conn1,"select * from 医药渠道分析原始表 where 识别标签='人工识别'")
dataset_base<-fetch(fetch_sql,n=-1)

# 数据清洗

# 使用stringr进行文本数据清洗
# 门店名称特殊符号 
library(stringr)
dataset_base$终端辅助名称<-dataset_base$终端名称%>%
  str_trim()%>%
  str_replace_all(c("[~!@$%&?/:()-★_^]"="","[{};（）:>,?。.\\\n]"=""," " =""))
row_n<-nrow(dataset_base)
for(j in c(1:row_n))
{
  dataset_base$term_length[j]<-nchar(dataset_base$终端辅助名称[j])
}
  

# 门店识别正则表达式
term_type_a<-"[分,加盟,直营,门,广场,路,连锁,一,二,三,四,五,六,七,八,九,街,园,村,城,山,分店].$"
term_seq_a<-c("分","加盟","直营","门","广场","连锁","路","店")
term_type_b<-"^分"
term_type_c<-"公司$"
term_type_d<-"[药房,药店,库,铺,行,栈,库,商场,零售点,门市,经营部,副食品店,超市,商店,商场,商城]$"
term_type_f<-"[经销公司,生产公司,配送公司,营销公司,分公司,有限公司,有限责任公司]$"
term_type_g<-"[卫生所,诊所,医院,门诊部,干休所,部队,防治所,医务室]$"

# 门店识别标志--方法一
n<-nrow(dataset_base)
for(a in c(1:n))
{ if(dataset_base$客户类型[a]=="连锁总部"&(str_detect(dataset_base$终端辅助名称[a],term_type_a)|str_detect(dataset_base$终端辅助名称[a],term_type_b))==TRUE)
{ dataset_base$终端类型辅助[a]<-"终端"
  dataset_base$终端类别辅助[a]<-"连锁门店"}
  else if(dataset_base$客户类型[a]=="连锁总部"&dataset_base$term_length[a]==0)
  { dataset_base$终端类型辅助[a]<-"终端"
  dataset_base$终端类别辅助[a]<-"连锁门店"}
  else if(dataset_base$客户类型[a]=="连锁总部"&str_detect(dataset_base$终端辅助名称[a],term_type_c)==TRUE)
  {dataset_base$终端类型辅助[a]<-"商业"
   dataset_base$终端类别辅助[a]<-"连锁总部"}
  else if(dataset_base$客户类型[a]=="T1"&str_detect(dataset_base$终端辅助名称[a],term_type_d)==TRUE)
  {dataset_base$终端类型辅助[a]<-"终端"
   dataset_base$终端类别辅助[a]<-"单体药店"}
  else if(dataset_base$客户类型[a]=="T1"&str_detect(dataset_base$终端辅助名称[a],term_type_g)==TRUE)
  {dataset_base$终端类型辅助[a]<-"终端"
  dataset_base$终端类别辅助[a]<-"诊疗"}
  else if(dataset_base$客户类型[a]=="T1"&str_detect(dataset_base$终端辅助名称[a],term_type_f)==TRUE)
  {dataset_base$终端类型辅助[a]<-"商业"
  dataset_base$终端类别辅助[a]<-"经分销商"}
  else if(dataset_base$客户类型[a]=="T2"&str_detect(dataset_base$终端辅助名称[a],term_type_d)==TRUE)
  {#dataset_base$终端类型辅助[a]<-"终端"
  dataset_base$终端类别辅助[a]<-"单体药店"}
  else if(dataset_base$客户类型[a]=="T2"&str_detect(dataset_base$终端辅助名称[a],term_type_g))
  {dataset_base$终端类型辅助[a]<-"终端"
  dataset_base$终端类别辅助[a]<-"诊疗"}
  else if(dataset_base$客户类型[a]=="T2"&str_detect(dataset_base$终端辅助名称[a],term_type_f))
  {dataset_base$终端类型辅助[a]<-"商业"
  dataset_base$终端类别辅助[a]<-"经分销商"}
  else{dataset_base$终端类型辅助[a]<-NA
  dataset_base$终端类别辅助[a]<-NA}
} 

# 数据写入
conn1<-dbConnect(MySQL(),dbname="Channe_sales_analysis",host="192.168.111.251",username="root",password="P#y20bsy17")
dbSendQuery(conn1,"SET NAMES gbk")
dbRemoveTable(conn1,"医药渠道分类表")   # 删除目标表
dbWriteTable(conn1,"医药渠道分类表",dataset_base,append = TRUE, row.names = FALSE)
dbDisconnect(conn1)


# 第二次识别

# 目标数据导入
conn1<-dbConnect(MySQL(),dbname="Channe_sales_analysis",host="192.168.111.251",username="****",password="*****")
dbSendQuery(conn1,"SET NAMES gbk")
fetch_sql<-dbSendQuery(conn1,"select * from 医药渠道分析原始表二 where 识别标签='人工识别'")
dataset_twice<-fetch(fetch_sql,n=-1)


# 数据二轮清洗
library(stringr)
dataset_twice$终端辅助名称<-dataset_twice$终端名称%>%
  str_trim()%>%
  str_replace_all(c("[~!@$%&?/:()-★_^]"="","[{};（）:>,?。#.\\\n]"=""," " ="","[非月]"="","全部发货"="","浒关"="","南京仓"="","图"=""))
for(j in c(1:nrow(dataset_twice)))
{
  dataset_twice$term_length[j]<-nchar(dataset_twice$终端标准名称[j])
}

term_ao<-"连锁"
term_at<-"[店房堂馆部]$"
term_bo<-"^连锁"
term_b<-"[超商市终零][场市坊堂局端售馆]$"
term_d<-"[合终零][伙端售]$"
term_c<-"公司$"
term_g<-"[卫综][生合][服]?[务]?[室站]$"
term_h<-"[仓配][库送]"
other_word<-"药[店房堂馆部]$"

# 门店识别标志--方法二
for(b in c(1:nrow(dataset_twice)))
{ if(dataset_twice$客户类型[b]=="连锁总部"&str_detect(dataset_twice$终端辅助名称[b],term_b))
  { dataset_twice$终端类型辅助[b]<-"终端"
  dataset_twice$终端类别辅助[b]<-"单体药店"}
  else if(dataset_twice$客户类型[b]=="连锁总部"&str_detect(dataset_twice$终端辅助名称[b],term_g))
  { dataset_twice$终端类型辅助[b]<-"终端"
  dataset_twice$终端类别辅助[b]<-"诊疗"}
  else if(dataset_twice$客户类型[b]=="T1"&str_detect(dataset_twice$终端辅助名称[b],term_b))
  {dataset_twice$终端类型辅助[b]<-"渠道"
  dataset_twice$终端类别辅助[b]<-"单体药店"}
  if(dataset_twice$客户类型[b]=="T1"&(str_detect(dataset_twice$终端辅助名称[b],term_ao)&str_detect(dataset_twice$终端辅助名称[b],term_at)))
  { dataset_twice$终端类型辅助[b]<-"终端"
  dataset_twice$终端类别辅助[b]<-"连锁门店"}
  else if(dataset_twice$客户类型[b]=="T1"&str_detect(dataset_twice$终端辅助名称[b],term_d))
  {dataset_twice$终端类型辅助[b]<-"终端"
  dataset_twice$终端类别辅助[b]<-"单体药店"}
  else if(dataset_twice$客户类型[b]=="T1"&str_detect(dataset_twice$终端辅助名称[b],term_g))
  {dataset_twice$终端类型辅助[b]<-"终端"
  dataset_twice$终端类别辅助[b]<-"诊疗"}
  else if(dataset_twice$客户类型[b]=="T1"&str_detect(dataset_twice$终端辅助名称[b],term_c))
  {dataset_twice$终端类型辅助[b]<-"渠道"
  dataset_twice$终端类别辅助[b]<-"经分销商"}
  else if(dataset_twice$客户类型[b]=="T2"&str_detect(dataset_twice$终端辅助名称[b],term_b))
  {dataset_twice$终端类型辅助[b]<-"渠道"
  dataset_twice$终端类别辅助[b]<-"单体药店"}
  else if(dataset_twice$客户类型[b]=="T2"&str_detect(dataset_twice$终端辅助名称[b],term_d))
  {dataset_twice$终端类型辅助[b]<-"终端"
  dataset_twice$终端类别辅助[b]<-"单体药店"}
  else if(dataset_twice$客户类型[b]=="T2"&str_detect(dataset_twice$终端辅助名称[b],term_g))
  {dataset_twice$终端类型辅助[b]<-"终端"
  dataset_twice$终端类别辅助[b]<-"诊疗"}
  else if(dataset_twice$客户类型[b]=="T2"&str_detect(dataset_twice$终端辅助名称[b],term_c))
  {dataset_twice$终端类型辅助[b]<-"渠道"
  dataset_twice$终端类别辅助[b]<-"经分销商"}
  else{dataset_twice$终端类型辅助[b]<-NA
  dataset_twice$终端类别辅助[b]<-NA}
} 


# 连锁总部补充
for(k in c(1:nrow(dataset_twice))){
  if(dataset_twice$客户类型[k]=="连锁总部"&(str_detect(dataset_twice$终端辅助名称[k],term_ao)&str_detect(dataset_twice$终端辅助名称[k],term_at)))
  { dataset_twice$终端类型辅助[k]<-"终端"
  dataset_twice$终端类别辅助[k]<-"连锁门店"}
  else if(dataset_twice$客户类型[k]=="连锁总部"&str_detect(dataset_twice$终端辅助名称[k],term_at))
  { dataset_twice$终端类型辅助[k]<-"终端"
  dataset_twice$终端类别辅助[k]<-"连锁门店"}
  else{}
}

for(g in c(1:nrow(dataset_twice))){
  if(is.null(dataset_twice$终端类型辅助[g])&dataset_twice$客户类型[g]=="连锁总部"&(str_detect(dataset_twice$终端辅助名称[g],term_ao)&str_detect(dataset_twice$终端辅助名称[g],term_at)))
  { dataset_twice$终端类型辅助[g]<-"终端"
  dataset_twice$终端类别辅助[g]<-"连锁门店"}
  else{}
}

# 门店补充
for(d in c(1:nrow(dataset_twice))){
  if(is.null(dataset_twice$终端类型辅助[d])&dataset_twice$客户类型[d]=="T1"&str_detect(dataset_twice$终端辅助名称[d],other_word))
  {dataset_twice$终端类型辅助[d]<-"终端"
  dataset_twice$终端类别辅助[d]<-"单体药店"}
  else if(is.null(dataset_twice$终端类型辅助[d])&dataset_twice$客户类型[d]=="T2"&str_detect(dataset_twice$终端辅助名称[d],other_word))
  {dataset_twice$终端类型辅助[d]<-"终端"
  dataset_twice$终端类别辅助[d]<-"单体药店"}
    else{}
   }

# 数据写入
conn1<-dbConnect(MySQL(),dbname="Channe_sales_analysis",host="192.168.111.251",username="root",password="P#y20bsy17")
dbSendQuery(conn1,"SET NAMES gbk")
dbRemoveTable(conn1,"医药渠道分类表二")   # 删除目标表
dbWriteTable(conn1,"医药渠道分类表二",dataset_twice,append = TRUE, row.names = FALSE)
dbDisconnect(conn1)



## 原始库存数据批号清洗===========================
## 目的：对原始库存数据中的库存批号进行清洗，提高数据使用量
library(DBI)
library(RMySQL)

# 目标数据导入
conn1<-dbConnect(MySQL(),dbname="Channe_sales_analysis",host="192.168.111.251",username="root",password="P#y20bsy17")
dbSendQuery(conn1,"SET NAMES gbk")
stock_sql<-dbSendQuery(conn1,"select * from 医药库存原始表")
dataset_stock<-fetch(stock_sql,n=-1)


# 使用stringr进行文本数据清洗
# 门店名称特殊符号 
library(stringr)
library(dplyr)
dataset_stock$产品批号[is.na(dataset_stock$产品批号)]<-0
dataset_stock$产品批号长度<-nchar(dataset_stock$产品批号)
dataset_stock<-filter(dataset_stock,产品批号长度>3&grepl("[0-9]{6,15}",dataset_stock$产品批号))
dataset_stock$产品标准批号<-dataset_stock$产品批号%>%
  str_trim()%>%
  str_extract_all("[0-9]{6,14}",simplify = FALSE)

# 将提取出来的数据字符list转换成字符串

for(i in 1:(nrow(dataset_stock)))
{
 dataset_stock$产品批号辅助[i]<-unlist(dataset_stock$产品标准批号[i],recursive = FALSE,use.names =FALSE)
  
}

dataset_stock_tempale<-subset(dataset_stock,select = c( "序号","客户所属组织", "客户编码","客户名称","客户所在省","客户所在城市",
                                                        "客户类型","上传周期","终端名称","终端所属组织","主数据类型", 
                                                        "终端类型","终端标准名称","行政省","行政市","产品批号",   
                                                        "产品数量","产品品类","产品规格","库存时间","计算金额","产品批号辅助"))

# 数据写入
conn1<-dbConnect(MySQL(),dbname="Channe_sales_analysis",host="192.168.111.251",username="root",password="P#y20bsy17")
dbSendQuery(conn1,"SET NAMES gbk")
dbRemoveTable(conn1,"stock_dataset_template")   # 删除目标表
dbWriteTable(conn1,"stock_dataset_template",dataset_stock_tempale,append = TRUE, row.names = FALSE)
dbDisconnect(conn1)

