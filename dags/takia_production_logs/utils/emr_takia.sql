SET
  hive.exec.dynamic.partition = true;
SET
  hive.exec.dynamic.partition.mode = nonstrict;
CREATE TABLE dw_stage.backfill_processed6 (
    `url` varchar(255) COMMENT '',
    `method` varchar(255) COMMENT '',
    `user_id` varchar(255) COMMENT '',
    `partner_id` varchar(255) COMMENT '',
    `request_id` varchar(255) COMMENT '',
    `user_ip` varchar(255) COMMENT '',
    `catalog_force_https` varchar(255) COMMENT '',
    `catalog_asset_id` varchar(255) COMMENT '',
    `unix_timestamp` double COMMENT '',
    `event_timestamp` timestamp COMMENT '',
    `catalog_drm` varchar(255) COMMENT '',
    `is_raw` varchar(255) COMMENT '',
    `device_type` varchar(255) COMMENT '',
    `msg` varchar(255) COMMENT '',
    `user_country_of_login` varchar(255) COMMENT '',
    `catalog_preroll_url` varchar(1000) COMMENT '',
    `catalog_region` varchar(255) COMMENT '',
    `content_provider` varchar(255) COMMENT '',
    `application_id` varchar(255) COMMENT ''
  ) PARTITIONED BY (`pk_eventdate` date COMMENT '') ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' LOCATION 's3://hooq-takia-production-logs/temp/backfill_processed6';
INSERT
  OVERWRITE TABLE dw_stage.backfill_processed6 PARTITION(pk_eventdate)
SELECT
  cast(url as varchar(255)) as url,
  cast(method as varchar(255)) as method,
  cast(user_id as varchar(255)) AS user_id,
  cast(partner_id as varchar(255)) AS partner_id,
  cast(request_id as varchar(255)) AS request_id,
  cast(user_ip as varchar(255)) AS user_ip,
  cast(catalog_force_https as varchar(255)) AS catalog_force_https,
  cast(catalog_asset_id as varchar(255)) AS catalog_asset_id,
  cast(unix_timestamp AS double) as unix_timestamp,
  cast(event_timestamp as timestamp) AS event_timestamp,
  cast(catalog_drm as varchar(255)) AS catalog_drm,
  cast(israw as varchar(255)) AS is_raw,
  cast(device_type as varchar(255)) AS device_type,
  cast(msg as varchar(255)) as msg,
  cast(user_country_of_login as varchar(255)) AS user_country_of_login,
  cast(catalog_preroll_url as varchar(1000)) AS catalog_preroll_url,
  cast(catalog_region as varchar(255)) AS catalog_region,
  cast(content_provider as varchar(255)) AS content_provider,
  cast(application_id as varchar(255)) AS application_id,
  date(pk_eventdate) AS pk_eventdate
from
  dw_stage.backfill_processed4