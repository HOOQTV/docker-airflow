CREATE TABLE IF NOT EXISTS dw_prod.takia_production_logs_test WITH (
  format = 'PARQUET',
  external_location = 's3://hooq-takia-production-logs/temp/test',
  partitioned_by = ARRAY ['pk_eventdate']
) AS
SELECT
  cast(url as varchar(255)),
  cast(method as varchar(255)),
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
  cast(msg as varchar(255)),
  cast(usercountryoflogin as varchar(255)) AS user_country_of_login,
  cast(catalogprerollurl as varchar(1000)) AS catalog_preroll_url,
  cast(catalogregion as varchar(255)) AS catalog_region,
  cast(contentprovider as varchar(255))  AS content_provider,
  cast(applicationid as varchar(255) ) AS application_id,
  cast(date(from_unixtime(ts) as date) AS pk_eventdate
FROM dw_stage.takia_production_logs_raw
