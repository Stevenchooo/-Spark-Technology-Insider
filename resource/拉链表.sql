# --------------------------------------------------------------------------------------------------------
#  @FileName: dwd_pty_enterp_dev_up_realname_ds_his.sql
#  @CopyRight: copyright(c)huawei technologies co.,ltd.1998-2017.all rights reserved.
#  @Purpose: 企业开发者实名历史
#  @Describe:企业开发者实名历史信息
#  @Input: ODS_UP_CORP_DEVELOPER_DM
#  @Output: dwd_pty_enterp_dev_up_realname_ds_his
#  @Author: jiaojinpeng/jwx433697
#  @Version: DataOne 2.0.2
#  @Create:2017.01.17
#  @Modify:
# ---------------------------------------------------------------------------------------------------------

beeline -e "
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
use bicoredata;
# @TableName dwd_pty_enterp_dev_up_realname_ds_his
# @TableDesc 企业开发者实名历史
CREATE EXTERNAL TABLE IF NOT EXISTS bicoredata.dwd_pty_enterp_dev_up_realname_ds_his (
      up_id              VARCHAR(128)  COMMENT '华为帐号编号'
     ,start_date         VARCHAR(8)    COMMENT '开始日期'
     ,org_name           VARCHAR(128)  COMMENT '机构名称'
     ,province           VARCHAR(128)  COMMENT '省份'
     ,city               VARCHAR(128)  COMMENT '城市'
     ,postal_code        VARCHAR(128)  COMMENT '邮政编码'
     ,addr               VARCHAR(5000) COMMENT '地址'
     ,lp_name            VARCHAR(256)  COMMENT '法人名称'
     ,lp_iden_type_cd    VARCHAR(16)   COMMENT '法人证件类型代码'
     ,lp_iden_id         VARCHAR(128)  COMMENT '法人证件编号'
     ,natural_person     VARCHAR(256)  COMMENT '自然人'
     ,accessory1         STRING        COMMENT  '附件1'
     ,accessory2         STRING        COMMENT  '附件2'
     ,accessory3         STRING        COMMENT  '附件3'
     ,base_id            VARCHAR(128)  COMMENT '基础编号'
     ,etl_time           VARCHAR(30)   COMMENT 'ETL时间'
)
COMMENT '企业开发者实名历史'
PARTITIONED BY (end_date VARCHAR(8) COMMENT '结束日期')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/AppData/BIProd/DWD/PTY/dwd_pty_enterp_dev_up_realname_ds_his'
TBLPROPERTIES('orc.compress'='ZLIB')
;


# 创建临时表，取当天最新的数据
DROP TABLE IF EXISTS temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_td;
CREATE TABLE IF NOT EXISTS temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_td
AS
SELECT
      tt1.up_id           
     ,tt1.start_date                                           AS start_date     
     ,IF(t2.up_id IS NULL,tt1.org_name,'')                     AS org_name       
     ,IF(t2.up_id IS NULL,tt1.province,'')                     AS province       
     ,IF(t2.up_id IS NULL,tt1.city,'')                         AS city           
     ,IF(t2.up_id IS NULL,tt1.postal_code,'')                  AS postal_code    
     ,IF(t2.up_id IS NULL,tt1.addr,'')                         AS addr           
     ,IF(t2.up_id IS NULL,tt1.lp_name,'')                      AS lp_name        
     ,IF(t2.up_id IS NULL,tt1.lp_iden_type_cd,'')              AS lp_iden_type_cd
     ,IF(t2.up_id IS NULL,tt1.lp_iden_id,'')                   AS lp_iden_id     
     ,IF(t2.up_id IS NULL,tt1.natural_person,'')               AS natural_person
     ,IF(t2.up_id IS NULL,tt1.accessory1,'')                   AS accessory1
     ,IF(t2.up_id IS NULL,tt1.accessory2,'')                   AS accessory2
     ,IF(t2.up_id IS NULL,tt1.accessory3,'')                   AS accessory3
     ,IF(t2.up_id IS NULL,tt1.base_id,'')                      AS base_id
     ,FROM_UNIXTIME(UNIX_TIMESTAMP(),'yyyy-MM-dd HH:mm:ss')    AS etl_time
     ,''                                                       AS end_date     
FROM
    (
    SELECT
         sha256(user_id)                                                AS up_id
        ,'$date'                                                           AS start_date
        ,name                                                           AS org_name
        #对开发者所在省份进行解密处理->AesCBCUpDecry(province,'up')
        ,AesCBCUpDecry(province,'up')        AS province
        #对开发者所在城市进行解密处理->AesCBCUpDecry(city,'up')
        ,AesCBCUpDecry(city,'up')            AS city     
        ,postcode                                                       AS postal_code
        ,address                                                        AS addr
        ,legal_man                                                      AS lp_name
        ,CAST(legal_man_ctftype AS VARCHAR(16))                         AS lp_iden_type_cd
        ,legal_man_ctfcode                                              AS lp_iden_id
        ,CAST(nature AS VARCHAR(16))                                    AS natural_person
        ,attachment1                                                    AS accessory1
        ,attachment2                                                    AS accessory2
        ,attachment3                                                    AS accessory3
        ,bslic_id                                                       AS base_id
    FROM biods.ods_up_corp_developer_dm
    WHERE pt_d='$date'
    ) tt1
LEFT OUTER JOIN dwd_pty_up_cancel_ds t2 
ON tt1.up_id=t2.up_id
;

# 创建临时表，取有效的数据
DROP TABLE IF EXISTS temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_ty;
CREATE TABLE IF NOT EXISTS temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_ty
AS
SELECT
     up_id            AS up_id
    ,start_date       AS start_date
    ,org_name         AS org_name
    ,province         AS province
    ,city             AS city
    ,postal_code        AS postal_code
    ,addr             AS addr
    ,lp_name          AS lp_name
    ,lp_iden_type_cd  AS lp_iden_type_cd
    ,lp_iden_id       AS lp_iden_id
    ,natural_person   AS natural_person
    ,accessory1       AS accessory1
    ,accessory2       AS accessory2
    ,accessory3       AS accessory3
    ,base_id          AS base_id
    ,etl_time         AS etl_time
    ,'99991231'       AS end_date
FROM bicoredata.dwd_pty_enterp_dev_up_realname_ds_his
WHERE start_date<='$last_date'
AND end_date>'$last_date'
;

# 创建临时表，将今天的数据和历史有效数据进行对比
DROP TABLE IF EXISTS temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_all;
CREATE TABLE IF NOT EXISTS temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_all
AS 
SELECT
     CASE WHEN td.up_id IS NULL THEN 'del'
          WHEN ty.up_id IS NULL THEN 'add'
          WHEN CONCAT(NVL(td.org_name,'|'),NVL(td.province,'|'),NVL(td.city,'|'),NVL(td.postal_code,'|'),NVL(td.addr,'|'),NVL(td.lp_name,'|'),NVL(td.lp_iden_type_cd,'|'),NVL(td.lp_iden_id,'|'),NVL(td.natural_person,'|'),NVL(td.accessory1,'|'),NVL(td.accessory2,'|'),NVL(td.accessory3,'|'),NVL(td.base_id,'|'))<>CONCAT(NVL(ty.org_name,'|'),NVL(ty.province,'|'),NVL(ty.city,'|'),NVL(ty.postal_code,'|'),NVL(ty.addr,'|'),NVL(ty.lp_name,'|'),NVL(ty.lp_iden_type_cd,'|'),NVL(ty.lp_iden_id,'|'),NVL(ty.natural_person,'|'),NVL(ty.accessory1,'|'),NVL(ty.accessory2,'|'),NVL(ty.accessory3,'|'),NVL(ty.base_id,'|')) THEN 'mod'
          ELSE 'non'
     END                         AS c_flag
    ,COALESCE(td.up_id,ty.up_id) AS up_id
    ,ty.start_date               AS ty_start_date
    ,td.org_name                 AS td_org_name
    ,ty.org_name                 AS ty_org_name
    ,td.province                 AS td_province
    ,ty.province                 AS ty_province
    ,td.city                     AS td_city
    ,ty.city                     AS ty_city
    ,td.postal_code                AS td_postal_id
    ,ty.postal_code                AS ty_postal_id
    ,td.addr                     AS td_addr
    ,ty.addr                     AS ty_addr
    ,td.lp_name                  AS td_lp_name
    ,ty.lp_name                  AS ty_lp_name
    ,td.lp_iden_type_cd          AS td_lp_iden_type_cd
    ,ty.lp_iden_type_cd          AS ty_lp_iden_type_cd
    ,td.lp_iden_id               AS td_lp_iden_id
    ,ty.lp_iden_id               AS ty_lp_iden_id
    ,td.natural_person           AS td_natural_person
    ,ty.natural_person           AS ty_natural_person
    ,td.accessory1               AS td_accessory1
    ,ty.accessory1               AS ty_accessory1
    ,td.accessory2               AS td_accessory2
    ,ty.accessory2               AS ty_accessory2
    ,td.accessory3               AS td_accessory3
    ,ty.accessory3               AS ty_accessory3
    ,td.base_id                  AS td_base_id
    ,ty.base_id                  AS ty_base_id
    ,td.etl_time                 AS td_etl_time
    ,ty.etl_time                 AS ty_etl_time
    ,td.end_date                 AS td_end_date
    ,ty.end_date                 AS ty_end_date
FROM temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_td td
FULL OUTER JOIN
     temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_ty ty
ON td.up_id = ty.up_id
;

# @DESC 将今天修改的数据加入开链，并将开链中修改的数据置为闭链
INSERT OVERWRITE TABLE bicoredata.dwd_pty_enterp_dev_up_realname_ds_his
PARTITION (end_date)
SELECT
     up_id                   AS up_id
    ,'$date'                 AS start_date
    ,td_org_name             AS org_name
    ,td_province             AS province
    ,td_city                 AS city
    ,td_postal_id            AS postal_code
    ,td_addr                 AS addr
    ,td_lp_name              AS lp_name
    ,td_lp_iden_type_cd      AS lp_iden_type_cd
    ,td_lp_iden_id           AS lp_iden_id
    ,td_natural_person       AS natural_person
    ,td_accessory1           AS accessory1
    ,td_accessory2           AS accessory2
    ,td_accessory3           AS accessory3
    ,td_base_id              AS base_id
    ,td_etl_time             AS etl_time
    ,'99991231'              AS end_date
FROM temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_all
WHERE c_flag = 'mod'
UNION ALL
SELECT
     up_id AS up_id
    ,CASE WHEN c_flag='mod' THEN ty_start_date
          WHEN c_flag='add' THEN '$date'
          WHEN c_flag='del' THEN ty_start_date
          WHEN c_flag='non' THEN ty_start_date
          ELSE ty_start_date END AS start_date
    ,CASE WHEN c_flag='mod' THEN ty_org_name
          WHEN c_flag='add' THEN td_org_name
          WHEN c_flag='del' THEN ty_org_name
          WHEN c_flag='non' THEN td_org_name
          ELSE td_org_name END AS org_name
    ,CASE WHEN c_flag='mod' THEN ty_province
          WHEN c_flag='add' THEN td_province
          WHEN c_flag='del' THEN ty_province
          WHEN c_flag='non' THEN td_province
          ELSE td_province END AS province
    ,CASE WHEN c_flag='mod' THEN ty_city
          WHEN c_flag='add' THEN td_city
          WHEN c_flag='del' THEN ty_city
          WHEN c_flag='non' THEN td_city
          ELSE td_city END AS city
    ,CASE WHEN c_flag='mod' THEN ty_postal_id
          WHEN c_flag='add' THEN td_postal_id
          WHEN c_flag='del' THEN ty_postal_id
          WHEN c_flag='non' THEN td_postal_id
          ELSE td_postal_id END AS postal_code
    ,CASE WHEN c_flag='mod' THEN ty_addr
          WHEN c_flag='add' THEN td_addr
          WHEN c_flag='del' THEN ty_addr
          WHEN c_flag='non' THEN td_addr
          ELSE td_addr END AS addr
    ,CASE WHEN c_flag='mod' THEN ty_lp_name
          WHEN c_flag='add' THEN td_lp_name
          WHEN c_flag='del' THEN ty_lp_name
          WHEN c_flag='non' THEN td_lp_name
          ELSE td_lp_name END AS lp_name
    ,CASE WHEN c_flag='mod' THEN ty_lp_iden_type_cd
          WHEN c_flag='add' THEN td_lp_iden_type_cd
          WHEN c_flag='del' THEN ty_lp_iden_type_cd
          WHEN c_flag='non' THEN td_lp_iden_type_cd
          ELSE td_lp_iden_type_cd END AS lp_iden_type_cd
    ,CASE WHEN c_flag='mod' THEN ty_lp_iden_id
          WHEN c_flag='add' THEN td_lp_iden_id
          WHEN c_flag='del' THEN ty_lp_iden_id
          WHEN c_flag='non' THEN td_lp_iden_id
          ELSE td_lp_iden_id END AS lp_iden_id
    ,CASE WHEN c_flag='mod' THEN ty_natural_person
          WHEN c_flag='add' THEN td_natural_person
          WHEN c_flag='del' THEN ty_natural_person
          WHEN c_flag='non' THEN td_natural_person
          ELSE td_natural_person END AS natural_person
    ,CASE WHEN c_flag='mod' THEN ty_accessory1
          WHEN c_flag='add' THEN td_accessory1
          WHEN c_flag='del' THEN ty_accessory1
          WHEN c_flag='non' THEN td_accessory1
          ELSE td_accessory1 END AS accessory1
    ,CASE WHEN c_flag='mod' THEN ty_accessory2
          WHEN c_flag='add' THEN td_accessory2
          WHEN c_flag='del' THEN ty_accessory2
          WHEN c_flag='non' THEN td_accessory2
          ELSE td_accessory2 END AS accessory2
    ,CASE WHEN c_flag='mod' THEN ty_accessory3
          WHEN c_flag='add' THEN td_accessory3
          WHEN c_flag='del' THEN ty_accessory3
          WHEN c_flag='non' THEN td_accessory3
          ELSE td_accessory3 END AS accessory3 
    ,CASE WHEN c_flag='mod' THEN ty_base_id
          WHEN c_flag='add' THEN td_base_id
          WHEN c_flag='del' THEN ty_base_id
          WHEN c_flag='non' THEN td_base_id
          ELSE td_base_id END AS base_id 
    ,CASE WHEN c_flag='mod' THEN td_etl_time
          WHEN c_flag='add' THEN td_etl_time
          WHEN c_flag='del' THEN ty_etl_time
          WHEN c_flag='non' THEN ty_etl_time
          ELSE td_etl_time END AS etl_time
    ,CASE WHEN c_flag='mod' THEN '$date'
          WHEN c_flag='add' THEN '99991231'
          WHEN c_flag='del' THEN '99991231'
          WHEN c_flag='non' THEN '99991231'
          ELSE '99991231' END AS end_date
FROM temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_all
;


#  备份表dwd_pty_enterp_dev_up_realname_ds_his开链的数据
CREATE EXTERNAL TABLE IF NOT EXISTS bicoredata.dwd_pty_enterp_dev_up_realname_ds_his_bak (
      up_id              VARCHAR(128)  COMMENT '华为帐号编号'
     ,start_date         VARCHAR(8)    COMMENT '开始日期'
     ,org_name           VARCHAR(128)  COMMENT '机构名称'
     ,province           VARCHAR(128)  COMMENT '省份'
     ,city               VARCHAR(128)  COMMENT '城市'
     ,postal_code        VARCHAR(128)  COMMENT '邮政编码'
     ,addr               VARCHAR(5000) COMMENT '地址'
     ,lp_name            VARCHAR(256)  COMMENT '法人名称'
     ,lp_iden_type_cd    VARCHAR(16)   COMMENT '法人证件类型代码'
     ,lp_iden_id         VARCHAR(128)  COMMENT '法人证件编号'
     ,natural_person     VARCHAR(256)  COMMENT '自然人'
     ,accessory1         STRING        COMMENT  '附件1'
     ,accessory2         STRING        COMMENT  '附件2'
     ,accessory3         STRING        COMMENT  '附件3'
     ,base_id            VARCHAR(128)  COMMENT '基础编号'
     ,etl_time           VARCHAR(30)   COMMENT 'ETL时间'
     ,end_date           VARCHAR(8)   COMMENT '结束日期'
)
COMMENT '企业开发者实名历史'
PARTITIONED BY (pt_d VARCHAR(8) COMMENT '天分区')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
LINES TERMINATED BY '\n'
STORED AS ORC
LOCATION '/AppData/BIProd/DWD/Bak/dwd_pty_enterp_dev_up_realname_ds_his_bak'
TBLPROPERTIES('orc.compress'='ZLIB')
;
DROP TABLE IF EXISTS temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_td;
DROP TABLE IF EXISTS temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_ty;
DROP TABLE IF EXISTS temp.tmp_dwd_pty_enterp_dev_up_realname_ds_his_all;
ALTER TABLE bicoredata.dwd_pty_enterp_dev_up_realname_ds_his_bak ADD IF NOT EXISTS PARTITION (pt_d ='$date');
ALTER TABLE bicoredata.dwd_pty_enterp_dev_up_realname_ds_his_bak DROP IF EXISTS PARTITION (pt_d ='${start_time,-10,yyyyMMdd}');
"$END

hadoop fs -test -e /AppData/BIProd/DWD/PTY/dwd_pty_enterp_dev_up_realname_ds_his/
if [ $? -eq 0 ];then
hadoop fs -rm -r /AppData/BIProd/DWD/Bak/dwd_pty_enterp_dev_up_realname_ds_his_bak/pt_d=$date
hadoop fs -cp /AppData/BIProd/DWD/PTY/dwd_pty_enterp_dev_up_realname_ds_his/end_date=99991231/ /AppData/BIProd/DWD/Bak/dwd_pty_enterp_dev_up_realname_ds_his_bak/pt_d=$date
else
echo "The online partition file does not exist"
fi

hadoop fs -test -e /AppData/BIProd/DWD/Bak/dwd_pty_enterp_dev_up_realname_ds_his_bak/pt_d=${start_time,-10,yyyyMMdd}
if [ $? -eq 0 ];then
hadoop fs -rm -r /AppData/BIProd/DWD/Bak/dwd_pty_enterp_dev_up_realname_ds_his_bak/pt_d=${start_time,-10,yyyyMMdd}
else
echo "The bak partition file does not exist"
fi
