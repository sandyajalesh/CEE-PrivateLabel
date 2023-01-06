#importing libraries
import pyodbc
import numpy
import pandas
import sharepy
import datetime
from datetime import datetime,timedelta

#establishing connection 
conn= pyodbc.connect('DSN=NDC') 

#BG

bgquery = """
(
SELECT
  Table__1."COUNTRY_CODE",
  Table__1."CALENDAR_YEAR_C"  ,
  Table__1."MONTH_NAME",
  Table__1."UNIFIED_PRODUCT_CD",
  Table__1."UNIFIED_PRODUCT_NM",
  Table__1."SUB_CATEGORY_NAME",
  Table__1."CATEGORY_NAME",
  SUM(Table__1."CM_NET_SALES_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM( Table__1."CM_NUMBER_OF_TRANS_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."COUNTRY_CODE"  IN  ('BG')
   AND
   Table__1."UNIFIED_PRODUCT_CD"  IN  ('21940','21941','3918','5819','3919','3148','3147','4775','11492','1699','11397','11399','11539','11395','24127','24126','11526','30451','11527','16957','16957','11159','11158','11160','9944','9942','9945','9943','11577','11556','11545','11546','11535','11522','11623','11515','11480','11601','11599','9635','11500','11537','11521','11519','11504','11510','11516','11504','11505','11561','11503','11394','17000','11520','11540','11523','11571','11501','11568','11569','389','16034','11528','30467','11579','11530','11544','11550','11553')
   AND
   Table__1."CALMONTH"  IN  ( '202212','202112'  )
  )
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."CALENDAR_YEAR_C"  , 
  Table__1."MONTH_NAME", 
  Table__1."UNIFIED_PRODUCT_CD", 
  Table__1."UNIFIED_PRODUCT_NM", 
  Table__1."SUB_CATEGORY_NAME", 
  Table__1."CATEGORY_NAME"
)
"""
bg = pandas.read_sql(bgquery,conn)
#writing data to csv
bg.to_csv("C:\\Users\\Sandya.JaleshKumar\\OneDrive - Shell\\CEE Private Lable\\New folder\\dec BG pp products.csv")
print("bg completed")

#HU

huquery = """
(
SELECT
  Table__1."COUNTRY_CODE",
  Table__1."CALENDAR_YEAR_C"  ,
  Table__1."MONTH_NAME",
  Table__1."UNIFIED_PRODUCT_CD",
  Table__1."UNIFIED_PRODUCT_NM",
  Table__1."SUB_CATEGORY_NAME",
  Table__1."CATEGORY_NAME",
  SUM(Table__1."CM_NET_SALES_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM( Table__1."CM_NUMBER_OF_TRANS_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."COUNTRY_CODE"  IN  ('HU')
   AND
   Table__1."UNIFIED_PRODUCT_CD"  IN  ('52535','52536','52538','16356','16647','19756','19764','19801','19810','19852','19861','19879','19908','19916','48954','48955','49346','49350','49351','49428','49429','52537','52559','55202','74354','74777','74778','74779','74780','74781','74782','74783','13533','13538','13534','13536','13535','13537','13564','16634','16635','16636','16630','16631','16632','16633')
   AND
   Table__1."CALMONTH"  IN  ( '202212','202112'  )
  )
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."CALENDAR_YEAR_C"  , 
  Table__1."MONTH_NAME", 
  Table__1."UNIFIED_PRODUCT_CD", 
  Table__1."UNIFIED_PRODUCT_NM", 
  Table__1."SUB_CATEGORY_NAME", 
  Table__1."CATEGORY_NAME"
)
"""
hu = pandas.read_sql(huquery,conn)
#writing data to csv
hu.to_csv("C:\\Users\\Sandya.JaleshKumar\\OneDrive - Shell\\CEE Private Lable\\New folder\\dec HU pp products.csv")
print("hu completed")

#PL

plquery = """
(
SELECT
  Table__1."COUNTRY_CODE",
  Table__1."CALENDAR_YEAR_C"  ,
  Table__1."MONTH_NAME",
  Table__1."UNIFIED_PRODUCT_CD",
  Table__1."UNIFIED_PRODUCT_NM",
  Table__1."SUB_CATEGORY_NAME",
  Table__1."CATEGORY_NAME",
  SUM(Table__1."CM_NET_SALES_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM( Table__1."CM_NUMBER_OF_TRANS_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."COUNTRY_CODE"  IN  ('PL')
   AND
   Table__1."UNIFIED_PRODUCT_CD"  IN  ('1852','1851','12386','853','6414','3665','3666','3667','3668','3669','17824','6404','2933','6406','6405','6435','6438','24897','13804','8768','6441','6440','1491','10423','6449','6417','15532','29917','6428','6427','6429','671','1091','2932','6419','6432','6421','6422','6446','6445','6443','6442','2769','2930','859','20343','20342','6434','6431','15531','6420','6426','6433','1074','6425','1327','18810','1165','29110','3229','19627','1087','29860','840','149','150','151','152','4330','103','109','4331','4329','110','120','4328','6439','28415','8798','20933','4761','4762')
   AND
   Table__1."CALMONTH"  IN  ( '202212','202112'  )
  )
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."CALENDAR_YEAR_C"  , 
  Table__1."MONTH_NAME", 
  Table__1."UNIFIED_PRODUCT_CD", 
  Table__1."UNIFIED_PRODUCT_NM", 
  Table__1."SUB_CATEGORY_NAME", 
  Table__1."CATEGORY_NAME"
)
"""
pl = pandas.read_sql(plquery,conn)
#writing data to csv
pl.to_csv("C:\\Users\\Sandya.JaleshKumar\\OneDrive - Shell\\CEE Private Lable\\New folder\\dec PL pp products.csv")
print("pl completed")


#CZ
czquery = """
(
SELECT
  Table__1."COUNTRY_CODE",
  Table__1."CALENDAR_YEAR_C"  ,
  Table__1."MONTH_NAME",
  Table__1."UNIFIED_PRODUCT_CD",
  Table__1."UNIFIED_PRODUCT_NM",
  Table__1."SUB_CATEGORY_NAME",
  Table__1."CATEGORY_NAME",
  SUM(Table__1."CM_NET_SALES_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM( Table__1."CM_NUMBER_OF_TRANS_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."COUNTRY_CODE"  IN  ('CZ')
   AND
   Table__1."UNIFIED_PRODUCT_CD"  IN  ('11006','11007','11014','11003','11070','11071','11107','11010','11011','11117','11008','11119','11009','11118','11114','11086','11115','11088','11090','11005','11116','28156','27906','11113','27907','28960','23180','11000','11108','11110','11109','11044','11068','11111','11112','23065','27418','22936','30543','27365','22935','22937','22934','11019','23528','11028','11082','11081','11018','11075','26769','11076','11074','26465','28145','11078','27909','27910','28146','11079','11080','11073','11085','23287','11084','11022','11021','23289','27911','23286','27482', '27386','28954','10001','28774','28715','31008','27150','27013','21011','27014','31515','31008','31528','28214','31008','31009','31007','29924','27013','28932','28589','28588','28151','25540','27014','27150','28402','28402','25540','31007','31441','31009','39793','39792','27704','27705','28398','28398','27966','27005','27010','27009','28898','28769','28660','28661','27930','28908','27129','28189','28718','27002','31514','27000','27002','31514','27000','27016','27017','27018','27019','26065','26066','29912','27482','26642','26643','27696','27697','30324','30325','30326','30391','30390')
   AND
   Table__1."CALMONTH"  IN  ( '202212','202112'  )
  )
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."CALENDAR_YEAR_C"  , 
  Table__1."MONTH_NAME", 
  Table__1."UNIFIED_PRODUCT_CD", 
  Table__1."UNIFIED_PRODUCT_NM", 
  Table__1."SUB_CATEGORY_NAME", 
  Table__1."CATEGORY_NAME"
)
"""
cz = pandas.read_sql(czquery,conn)
#writing data to csv
cz.to_csv("C:\\Users\\Sandya.JaleshKumar\\OneDrive - Shell\\CEE Private Lable\\New folder\\dec CZ pp products.csv")
print("cz completed")

#SK
skquery = """
(
SELECT
  Table__1."COUNTRY_CODE",
  Table__1."CALENDAR_YEAR_C"  ,
  Table__1."MONTH_NAME",
  Table__1."UNIFIED_PRODUCT_CD",
  Table__1."UNIFIED_PRODUCT_NM",
  Table__1."SUB_CATEGORY_NAME",
  Table__1."CATEGORY_NAME",
  SUM(Table__1."CM_NET_SALES_001"),
  SUM(Table__1."SALES_QUANTITY"),
  SUM( Table__1."CM_NUMBER_OF_TRANS_001")
FROM
  "_SYS_BIC"."shell.app.retail/CA_GENERAL_SALES_GRA_001"  Table__1
WHERE
  (
   Table__1."COUNTRY_CODE"  IN  ('SK')
   AND
   Table__1."UNIFIED_PRODUCT_CD"  IN  ('29297','44629','48067','11016','11017','11014','11015','11010','11011','11008','45923','11007','45924','11002','11004','11005','42021','41993','41954','48053','41953','48052','41952','46006','11012','11006','11128','47153','11009','11101','11001','11114','29446','11113','11115','41955','11000','11134','11050','11090','11051','11053','46006','43835','46163','45926','45927','46164','11120','45925','11119','11123','11136','11122','11124','11118','11033','11133','11028','11029','42219','11132','42070','45383','47809','47811','47980','47810','47804','47805','47806','47807','28415','43273','43271','43274','43272','29023','31307','44264','10004','45244','29023','46231','46178','46363','46363','46362','46362','40401','40400','45624','45625','28430','29024','29025','28427','28427','46808','45383','44156','44155','45674','45675','47294','47295','47296','31303','31305','31304','47408','47407','29568','29337','29032','28931')
   AND
   Table__1."CALMONTH"  IN  ('202212','202112')
  )
GROUP BY
  Table__1."COUNTRY_CODE", 
  Table__1."CALENDAR_YEAR_C"  , 
  Table__1."MONTH_NAME", 
  Table__1."UNIFIED_PRODUCT_CD", 
  Table__1."UNIFIED_PRODUCT_NM", 
  Table__1."SUB_CATEGORY_NAME", 
  Table__1."CATEGORY_NAME"
)
"""
sk = pandas.read_sql(skquery,conn)
#writing data to csv
sk.to_csv("C:\\Users\\Sandya.JaleshKumar\\OneDrive - Shell\\CEE Private Lable\\New folder\\dec SK pp products.csv")
print("sk completed")






