-- 数据选择框
SET @up_cycle="2018-06";

-- 数据形成部分
DROP TABLE
IF EXISTS 医药渠道分析原始表;
CREATE TABLE 医药渠道分析原始表 AS
SELECT
 tt.`客户名`,
 tt.`客户所在省`,
 ss.省会城市,
 ss.省会经度,
 ss.省会纬度,
 tt.`客户类型`,
 tt.`终端名称`,
 tt.`销售时间`,
 tt.`产品名`,
 tt.`产品规格`,
 tt.`产品批号`,
 tt.`标准名称`,
 tt.`标准规格类型`,
 tt.`产品销量`,
 tt.`销量单位`
FROM
(SELECT
 tt.cust_id AS 客户编号,
 cust.cust_name AS 客户名,
 cust.dist_type AS 客户类型, 
 cust.area_state AS 客户所在省,
 cust.area_city  AS 客户所在城市,		
 tt.term_name AS 终端名称,
 tt.prod_name AS 产品名,
 tt.prod_spec AS 产品规格,
 tt.prod_batch AS 产品批号 ,
 tt.prod_count AS 产品销量, 
 tt.prod_unit AS 销量单位,
 tt.sale_date AS 销售时间,
 zz.`标准名称`,
 zz.`标准规格类型`
FROM
(SELECT
 tt.cust_id,
 tt.term_name,
 tt.prod_name,
 tt.prod_spec,
 tt.prod_count,
 tt.prod_batch,
 tt.prod_unit,
 tt.sale_date
FROM
 Channe_sales_analysis.销售数据主表 tt
WHERE
 tt.up_cycle = @up_cycle
)tt
LEFT JOIN
 Channe_sales_analysis.`客户信息主表` cust
ON
 tt.cust_id=cust.id
LEFT JOIN
 Channe_sales_analysis.`渠道产品标准化` zz
ON
 CONCAT(tt.prod_name,tt.prod_spec)=CONCAT(zz.产品名,规格类型)
)tt
LEFT JOIN
(SELECT
 tt.`省份`,
 CASE WHEN tt.`地市`=tt.`区县` THEN tt.`地市` ELSE NULL END AS 省会城市,
 CASE WHEN tt.`地市`=tt.`区县` THEN tt.`经度` ELSE NULL END AS 省会经度,
 CASE WHEN tt.`地市`=tt.`区县` THEN tt.`纬度` ELSE NULL END AS 省会纬度
FROM
 agilesc_report.`全国省市经纬度` tt
GROUP BY
 tt.`省份`)ss
ON tt.`客户所在省`=ss.省份;
