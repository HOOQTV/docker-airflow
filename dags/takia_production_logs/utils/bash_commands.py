# backup prod partition to temp folder by moving
S3_BACKUP = """
aws s3 mv --recursive s3://hooq-takia-production-logs/prod/pk_eventdate={event_date}/ s3://hooq-takia-production-logs/temp/pk_eventdate={event_date}/
"""

# insert into prod from stage
S3_INSERT = """
aws s3 cp --recursive s3://hooq-takia-production-logs/stage/{dt}/pk_eventdate={event_date}/ s3://hooq-takia-production-logs/prod/pk_eventdate={event_date}/
"""

# delete from backup if success
S3_DELETE = """
aws s3 rm --recursive s3://hooq-takia-production-logs/temp/pk_eventdate={event_date}/
"""

# restore backup from temp folder in case of failures
S3_RESTORE = """
aws s3 mv --recursive s3://hooq-takia-production-logs/temp/pk_eventdate={event_date}/ s3://hooq-takia-production-logs/pk_eventdate={event_date}/
"""
