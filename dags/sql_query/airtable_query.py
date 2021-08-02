# https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json
COPY_SQL = """
BEGIN;
SET search_path TO hung; 
COPY {}
FROM '{}'
ACCESS_KEY_ID '{{}}'
SECRET_ACCESS_KEY '{{}}'
json 'auto';
COMMIT;
"""

create_schema = '''
BEGIN;
CREATE SCHEMA IF NOT EXISTS hung;
COMMIT;
'''

create_airtable_web_events = '''
BEGIN;
SET search_path TO hung; 
DROP TABLE IF EXISTS hung.web_events;
CREATE TABLE IF NOT EXISTS hung.web_events (
        id varchar,
        created_at varchar, 
        device_id varchar NULL,
        ip_address varchar NULL,
        user_email varchar NULL,
        user_id varchar NULL,
        uuid varchar NULL,
        event_type varchar NULL,
        metadata super NULL,
        PRIMARY KEY (id)
    );
COMMIT;
'''

copy_all_web_sql = COPY_SQL.format(
    "web_events",
    "s3://luko-kenhung/web_events"
)


create_airtable_app_events = '''
BEGIN;
SET search_path TO hung; 
DROP TABLE IF EXISTS app_events;
CREATE TABLE IF NOT EXISTS hung.app_events (
        id varchar,
        created_at varchar,
        device_id varchar NULL,
        ip_address varchar NULL,
        user_id varchar NULL,
        uuid varchar NULL,
        event_type varchar NULL,
        platform varchar NULL,
        device_type varchar NULL,
        event_properties super NULL,
        PRIMARY KEY (id)
    );
COMMIT;
'''

copy_all_app_sql = COPY_SQL.format(
    "app_events",
    "s3://luko-kenhung/app_events"
)


'''
1. A raw event table, gathering all events from web & apps, and their metadata
'''
raw_event_table_value = '''
BEGIN;
SET search_path TO hung; 
DROP TABLE IF EXISTS raw_events;
CREATE TABLE raw_events AS (
    SELECT 
        id,
        created_at::date,
        device_id,
        ip_address,
        user_email,
        user_id,
        uuid,
        event_type,
        metadata,
        'web' as source_event,
        JSON_PARSE('{}') as event_properties,
        '' as platform,
        '' as device_type
    FROM hung.web_events
    UNION
    SELECT 
        id,
        created_at::date,
        device_id,
        ip_address,
        '' as user_email,
        user_id,
        uuid,
        event_type,
        JSON_PARSE('{}') as metadata,
        'app' as source_type,
        event_properties,
        platform,
        device_type
    FROM app_events );
COMMIT;
'''

'''
2. An event_sequence table gathering, for any event, the id of the previous event, and
the id of the next event for the same user
'''
event_sequence = '''
BEGIN;
SET search_path TO hung; 
DROP TABLE IF EXISTS event_sequence;
CREATE TABLE event_sequence AS (
    SELECT user_id,
            created_at,
            device_id,
            ip_address,
            event_type,
           ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY created_at) as seq_event
    FROM raw_events
    where user_id is not null);
COMMIT;
'''



'''
3. An event_metrics table, gathering some top level simple metrics (you will not be
evaluated on the quality of the metrics) at the day level
    1. Most app event type
    2. Most web event type
    3. Most app app platform
    4. Most_visit IP
    5. Most daily visit
'''

event_metrics = '''
BEGIN;
SET search_path TO hung; 
DROP TABLE IF EXISTS event_metrics;
CREATE TABLE event_metrics AS (
    SELECT
        (
        SELECT event_type FROM (
        SELECT event_type, COUNT(event_type) 
        FROM web_events  GROUP BY event_type 
        HAVING COUNT (event_type)=
        (
        SELECT MAX(mycount) 
        FROM ( 
        SELECT event_type, COUNT(event_type) as mycount 
        FROM web_events GROUP BY event_type) foo)) loo) most_web_event,

        (
        SELECT event_type FROM (
        SELECT event_type, COUNT(event_type) 
        FROM app_events  GROUP BY event_type 
        HAVING COUNT (event_type)=
        (
        SELECT MAX(mycount) 
        FROM ( 
        SELECT event_type, COUNT(event_type) as mycount 
        FROM app_events GROUP BY event_type) foo)) loo) most_app_event,

        (
        SELECT platform FROM (
        SELECT platform, COUNT(platform) 
        FROM app_events  GROUP BY platform 
        HAVING COUNT (platform)=
        (
        SELECT MAX(mycount) 
        FROM ( 
        SELECT platform, COUNT(platform) as mycount 
        FROM app_events GROUP BY platform) foo)) loo) most_app_platform,

        (
        SELECT ip_address FROM (
        SELECT ip_address, COUNT(ip_address) 
        FROM raw_events  GROUP BY ip_address 
        HAVING COUNT (ip_address)=
        (
        SELECT MAX(mycount) 
        FROM ( 
        SELECT ip_address, COUNT(ip_address) as mycount 
        FROM raw_events GROUP BY ip_address) foo)) loo) most_ip_address,

        (
        SELECT most_daily FROM (
        SELECT created_at::date, COUNT(created_at::date) as most_daily
        FROM raw_events  GROUP BY created_at::date 
        HAVING COUNT (created_at::date)=
        (
        SELECT MAX(mycount) 
        FROM ( 
        SELECT created_at::date, COUNT(created_at::date) as mycount 
        FROM raw_events GROUP BY created_at::date) foo)) loo) most_daily_vist
    );
COMMIT;
'''

'''
4. An attribution table, gathering for each user, the utm tags of his first visit
'''

attribution_table = '''
BEGIN;
SET search_path TO hung;
    DROP TABLE IF EXISTS attribution;
    CREATE TABLE attribution AS ( 
        SELECT  
            m.id ,
            m.created_at ,
            m.ip_address ,
            m.user_email ,
            m.user_id ,
            m.uuid ,
            m.event_type ,
            m.metadata,
      		s.total_visit
        FROM web_events as m
        JOIN 
        (SELECT user_id, MIN(created_at) as created_at, count(*) as total_visit
        FROM web_events WHERE metadata.page_search is not null
        GROUP BY user_id) as s
        ON m.user_id = s.user_id and m.created_at = s.created_at
    );
COMMIT;
'''