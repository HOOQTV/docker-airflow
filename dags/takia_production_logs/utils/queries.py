DROP_STAGE = """
DROP TABLE IF EXISTS dw_stage.takia_production_logs_{dt};
"""

CREATE_STAGE= """
CREATE TABLE IF NOT EXISTS dw_stage.takia_production_logs_{dt} WITH (
  format = 'PARQUET',
  external_location = 's3://hooq-takia-production-logs/stage/{dt}',
  partitioned_by = ARRAY ['pk_eventdate']
) AS
SELECT
  cast(url as varchar(255)) as url,
  cast(method as varchar(255)) as method,
  cast(userid as varchar(255)) AS user_id,
  cast(partnerid as varchar(255)) AS partner_id,
  cast(requestid as varchar(255)) AS request_id,
  cast(userip as varchar(255)) AS user_ip,
  cast(catalogforcehttps as varchar(255)) AS catalog_force_https,
  cast(catalogassetid as varchar(255)) AS catalog_asset_id,
  cast(ts AS double) unix_timestamp,
  cast(from_unixtime(ts) as timestamp) AS event_timestamp,
  cast(catalogdrm as varchar(255)) AS catalog_drm,
  cast(israw as varchar(255)) AS is_raw,
  cast(devicetype as varchar(255)) AS device_type,
  cast(msg as varchar(255)) as msg,
  cast(usercountryoflogin as varchar(255)) AS user_country_of_login,
  cast(catalogprerollurl as varchar(1000)) AS catalog_preroll_url,
  cast(catalogregion as varchar(255)) AS catalog_region,
  cast(contentprovider as varchar(255))  AS content_provider,
  cast(applicationid as varchar(255) ) AS application_id,
  date(from_unixtime(ts)) AS pk_eventdate
FROM
  dw_stage.takia_production_logs_raw
WHERE
  partition_0 = '{year}'
  AND partition_1 = '{month}'
  AND partition_2 = '{day}';
"""

ADD_PARTITION = """
ALTER TABLE dw_prod.takia_production_logs ADD
  PARTITION (pk_eventdate = '{event_date}')
"""
