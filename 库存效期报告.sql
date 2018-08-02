-- 流向库存表数据理解
SET @max_stock_date = "2018-06";
SET @last_date = DATE_FORMAT(now(),"%Y-%m-%d");
SET @rownum = 0;
SET @up_cycle = "2018-06";
-- 最近一次库存天数：2018-06-30 00:00:00


DROP TABLE
IF EXISTS 库存效期初始表;
CREATE TABLE 库存效期初始表 AS
SELECT
 @rownum := @rownum+1 AS 序列号,
 tt.*,
 CASE WHEN tt.生产效期<0 THEN "00-过期品" 
      WHEN tt.生产效期 BETWEEN 0 AND 6 THEN "01-06个月"
      WHEN tt.生产效期 BETWEEN 7 AND 9 THEN "07-09个月"  
      WHEN tt.生产效期 BETWEEN 10 AND 12 THEN "10-12个月"   
      ELSE "13个月及以上" END AS 产品效期 
FROM
(SELECT
 tt.`客户名称`,
 tt.`客户编号`,
 tt.客户类型,
 tt.`客户所在省`,
 t1.省会城市,
 t1.省会经度,
 t1.省会纬度,
 tt.`客户所在城市`,
 tt.`产品名`,
 tt.`产品规格`,
 tt.`产品标准名`,
 tt.`产品标准规格`,
 tt.`产品批号`,
 tt.生产日期,
 CASE WHEN tt.`产品标准名`="常润茶" THEN ROUND(18-ABS(ROUND(DATEDIFF(tt.生产日期,@last_date)/30,0)),0) 
     WHEN tt.`产品标准名`="常菁茶" THEN ROUND(20-ABS(ROUND(DATEDIFF(tt.生产日期,@last_date)/30,0)),0)
     WHEN tt.`产品标准名`="纤纤茶" THEN ROUND(24-ABS(ROUND(DATEDIFF(tt.生产日期,@last_date)/30,0)),0)  
     WHEN tt.`产品标准名`="开塞露" THEN ROUND(36-ABS(ROUND(DATEDIFF(tt.生产日期,@last_date)/30,0)),0)  
     WHEN tt.`产品标准名`="来利奥利司他" THEN ROUND(36-ABS(ROUND(DATEDIFF(tt.生产日期,@last_date)/30,0)),0)  
     ELSE NULL END AS 生产效期,
 tt.`库存编号`,
 tt.`库存类型`,
 tt.`库存日期`,
 tt.`产品单位`,
 tt.`产品销量`,
--  tt.`产品价格`,
--  tt.`产品金额`
 zz.`标准价`,
 tt.`产品销量`*zz.`标准价`AS 产品金额
FROM
(
SELECT
 tt.`客户名称`,
 tt.`客户编号`,
 CASE WHEN tt.`客户类型` = 1 THEN "T1" 
      WHEN tt.`客户类型` = 2 THEN "T2"
      WHEN tt.`客户类型` = 3 THEN "KA"
      ELSE "未知" END AS 客户类型,
 tt.`客户所在省`,
 tt.`客户所在城市`,
 tt.`产品名`,
 tt.`产品规格`,
 tt.`产品标准名`,
 tt.`产品标准规格`,
 tt.`产品批号`,
CASE WHEN tt.`产品标准名`="常润茶" THEN DATE_FORMAT(CONCAT(MID(tt.`产品批号`,2,2),"-",MID(tt.`产品批号`,4,2),"-01"),"%Y-%m-%d")  
     WHEN tt.`产品标准名`="常菁茶" THEN DATE_FORMAT(CONCAT(MID(tt.`产品批号`,2,2),"-",MID(tt.`产品批号`,4,2),"-01"),"%Y-%m-%d") 
     WHEN tt.`产品标准名`="纤纤茶" THEN DATE_FORMAT(CONCAT(MID(tt.`产品批号`,3,2),"-",MID(tt.`产品批号`,5,2),"-01"),"%Y-%m-%d") 
     WHEN tt.`产品标准名`="开塞露" THEN DATE_FORMAT(CONCAT(MID(tt.`产品批号`,1,2),"-",MID(tt.`产品批号`,3,2),"-01"),"%Y-%m-%d") 
     WHEN tt.`产品标准名`="来利奥利司他" AND LEFT(tt.`产品批号`,2)=21 THEN DATE_FORMAT(CONCAT(MID(tt.`产品批号`,2,2),"-",MID(tt.`产品批号`,4,2),"-01"),"%Y-%m-%d")
     WHEN tt.`产品标准名`="来利奥利司他" AND LEFT(tt.`产品批号`,2)=11 THEN DATE_FORMAT(CONCAT(MID(tt.`产品批号`,2,2),"-",MID(tt.`产品批号`,4,2),"-01"),"%Y-%m-%d")
     WHEN tt.`产品标准名`="来利奥利司他" AND LEFT(tt.`产品批号`,2)=20 THEN DATE_FORMAT(CONCAT(MID(tt.`产品批号`,3,2),"-",MID(tt.`产品批号`,5,2),"-01"),"%Y-%m-%d") 
     WHEN tt.`产品标准名`="来利奥利司他" AND LEFT(tt.`产品批号`,2)=15 THEN DATE_FORMAT(CONCAT(MID(tt.`产品批号`,1,2),"-",MID(tt.`产品批号`,3,2),"-01"),"%Y-%m-%d")
     ELSE NULL END AS 生产日期,
 tt.`库存编号`,
 tt.`库存类型`,
 tt.`库存日期`,
 tt.`产品单位`,
 tt.`产品销量`
--  tt.`产品价格`,
--  tt.`产品金额`
FROM
( 
SELECT
 tt.stock_date AS 库存日期,
 tt.cust_id AS 客户编号,
 zz.dist_name AS 客户名称,
 zz.dist_type AS 客户类型,
 zz.area_state AS 客户所在省,
 zz.area_city AS 客户所在城市,
 tt.id AS 库存编号,
 tt.stock_type AS 库存类型,
 tt.prod_name AS 产品名,
 tt.prod_spec AS 产品规格,
CONVERT(tt.prod_batch,DECIMAL(10,0))AS 产品批号,
--  tt.prod_batch AS 产品批号,
 tt.prod_unit AS 产品单位,
 tt.prod_count AS 产品销量,
--  tt.prod_price AS 产品价格,
--  tt.prod_money AS 产品金额,
 jj.产品标准名,
 jj.`产品标准规格`
FROM
(SELECT
tt.*
FROM
agilesc_report.`库存主表信息` tt
WHERE
 tt.prod_batch NOT REGEXP '[A|C|B|#|_|!|@|$|%|"批号:"]'
AND
 LENGTH(tt.prod_batch) BETWEEN 6 AND 12
AND
 tt.up_cycle = "2018-06")tt
LEFT JOIN
agilesc_report.`客户信息主表` zz
ON  tt.cust_id = zz.id
LEFT JOIN
agilesc_report.`产品标准表` jj
ON  CONCAT(tt.prod_name,tt.prod_spec)=CONCAT(jj.产品名,jj.规格类型)
ORDER BY
 jj.`产品标准名` DESC
)tt
WHERE
 LENGTH(tt.`产品批号`)>1
AND
 tt.产品标准名 IS NOT NULL
ORDER BY
 生产日期 DESC
) tt
LEFT JOIN
(SELECT
 tt.`省份`,
 CASE WHEN tt.`地市`=tt.`区县` THEN tt.`地市` ELSE NULL END AS 省会城市,
 CASE WHEN tt.`地市`=tt.`区县` THEN tt.`经度` ELSE NULL END AS 省会经度,
 CASE WHEN tt.`地市`=tt.`区县` THEN tt.`纬度` ELSE NULL END AS 省会纬度
FROM
 agilesc_report.`全国省市经纬度` tt
GROUP BY
 tt.`省份`)t1
ON tt.`客户所在省`=t1.省份
LEFT JOIN
 agilesc_report.`产品价格表` zz
ON
 CONCAT(tt.`产品标准名`,tt.`产品标准规格`)=CONCAT(zz.产品名称,zz.产品标准规格)
WHERE
 tt.生产日期 IS NOT NULL
ORDER BY
 tt.`产品标准名`,
 tt.`客户名称`,
 tt.产品批号 DESC)tt
ORDER BY
 生产效期;



