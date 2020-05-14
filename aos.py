from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark import SQLContext
import boto3
import datetime
import pytz
from pytz import timezone
import sys
from awsglue.utils import getResolvedOptions
import json
import pg8000


args = getResolvedOptions(sys.argv, ['STAGE_NAME'])
stage_name = args['STAGE_NAME']
aws_region = 'us-east-1'
ssm = boto3.client('ssm', region_name=aws_region)
glue = boto3.client('glue', region_name=aws_region)
secretsmanager = boto3.client('secretsmanager', region_name=aws_region)


target_schema = 'geagp-ca'
target_table = "geagp_ca.aos_project_master_vw_new"
target_file = target_table.replace('.', '_')


ca_tablename_list = ['geagp_ca.edip_actionitems', 'geagp_ca.av_hr_pub_person_contact_info_t','geagp_ca.dna_jobboard', 'geagp_ca.aos_eng_projects_io',
                     'geagp_ca.aos_eng_project_owners_io', 'geagp_ca.av_sc_pub_epex_project_details_t', 'geagp_ca.aos_qep','geagp_ca.aos_form_allothers', 'geagp_ca.aos_form_practitioner', 'geagp_ca.aos_repair_projects',
                     'geagp_ca.aos_form_foundational']

# Get parameters
ca_s3_db_name = ssm.get_parameter(Name="/ca/" + stage_name + "/gluejobparameters/gluedbname")['Parameter']['Value']
shoreline_db_names = json.loads(ssm.get_parameter(Name="/ca/" + stage_name + "/gluejobparameters/shoreline_db_names")['Parameter']['Value'])
s3_target = "s3://" + ssm.get_parameter(Name="/ca/" + stage_name + "/gluejobparameters/s3_bucket_name")['Parameter']['Value'] + "/ca-data/" + stage_name + "/" + target_schema + "/"

db_connection_name = json.loads(ssm.get_parameter(Name="/ca/" + stage_name + "/db_connections")['Parameter']['Value'])['aurora']
db_connection_parameters = glue.get_connection(Name=db_connection_name)
port = int(db_connection_parameters['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'].split('//')[1].split(':')[1].split('/')[0])
host = db_connection_parameters['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'].split('//')[1].split(':')[0]
db_user = db_connection_parameters['Connection']['ConnectionProperties']['USERNAME']
db_name = db_connection_parameters['Connection']['ConnectionProperties']['JDBC_CONNECTION_URL'].split('//')[1].split(':')[1].split('/')[1]
password = json.loads(secretsmanager.get_secret_value(SecretId='/ca/' + stage_name + '/auroracredentials')['SecretString'])["password"]


# Current Eastern time
now = datetime.datetime.now(pytz.timezone('US/Eastern'))
dt = now.strftime("%Y") + "_" + now.strftime("%m") + "_" + now.strftime("%d") + "_" + now.strftime(
    "%H") + "_" + now.strftime("%M") + "_" + now.strftime("%S")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
logger = glueContext.get_logger()
job = Job(glueContext)
sqlContext = SQLContext(sparkContext=sc)

# Load source tables
for table in ca_tablename_list:
    partitions_info = glue.get_partitions(
        DatabaseName=ca_s3_db_name,
        TableName=table.replace('.', '_'))
    location = partitions_info['Partitions'][0]['StorageDescriptor']['Location']
    df = sqlContext.read.parquet(location)
    df.createOrReplaceTempView(table.replace('.', '_'))

df = spark.sql("""SELECT DISTINCT 'eDIP' AS source, edip_actionitems.row_id AS project_id, edip_actionitems.owner, cast(left(hr.person_full_nm, 100) as string) AS owner_name,
date_format(cast(edip_actionitems.action_date as timestamp), 'MM/dd/yyyy') AS open_date, date_format(cast(edip_actionitems.end_date as timestamp), 'MM/dd/yyyy') AS closed_date,
edip_actionitems.part_number, edip_actionitems.action_summary AS project_name, edip_actionitems.detailed_action_plan AS description,
edip_actionitems.actionlifecycle AS status, edip_actionitems.site_code, edip_actionitems.engine_program, edip_actionitems.actionstatus AS action_lifecycle, edip_actionitems.action_summary,
CAST(NULL as string)AS engagement_type, CAST(NULL as string) AS engagement_leader_multiple, CAST(NULL as string)AS bpo, CAST(NULL as string) AS bporev, CAST(NULL as string) AS tags, CAST(NULL as string) AS focus, CAST(NULL as string) AS engine_family,CAST(NULL as string) AS comment,CAST(NULL as string) AS target_end_date,
CAST(NULL as string) AS updated, CAST(NULL as string) AS project_division, CAST(NULL as string) AS project_dept, CAST(NULL as string) AS aos_content, CAST(NULL as string) AS project_tags, CAST(NULL as string) AS desired_outcome, CAST(NULL as string) AS project_due_date,
CAST(NULL as string) AS project_start_date, CAST(NULL as string) AS cert_level, CAST(NULL as string) AS project_status, CAST(NULL as string) AS project_updated_date, CAST(NULL as string) AS progress_updated_date,
'https://dna.av.ge.com/page/shopdashboard' AS project_link, CAST(NULL as string) AS kpi, CAST(NULL as string) AS valuestream, CAST(NULL as string) AS business, CAST(NULL as string) AS project_type, CAST(NULL as string) AS value_stream,CAST(NULL as string) AS engine_module,
CAST(NULL as string) AS updated_date, CAST(NULL as string) AS business_process FROM geagp_ca_edip_actionitems edip_actionitems
left join
geagp_ca_av_hr_pub_person_contact_info_t hr ON hr.person_sso_id = edip_actionitems.owner
UNION ALL
SELECT DISTINCT 'Tiger team' AS source, dna_jobboard.id AS project_id, dna_jobboard.engagementleader AS owner, cast(left(hr.person_full_nm, 100) as string) AS owner_name,
date_format(cast(dna_jobboard.activateddate as timestamp), 'MM/dd/yyyy') AS open_date, date_format(cast(dna_jobboard.completeddate as timestamp), 'MM/dd/yyyy') AS closed_date,
dna_jobboard.partnumbers AS part_number, dna_jobboard.engagementtitle AS project_name, dna_jobboard.focus AS description, dna_jobboard.engagementstatus AS status,
CAST(NULL as string) AS site_code, CAST(NULL as string) AS engine_program, CAST(NULL as string) AS action_lifecycle, CAST(NULL as string) AS action_summary, dna_jobboard.engagementtype, dna_jobboard.engagementleadermultiple,
dna_jobboard.bpo, dna_jobboard.bporev, dna_jobboard.tags, dna_jobboard.focus, dna_jobboard.enginefamily, dna_jobboard.comment,
date_format(cast(dna_jobboard.targetenddate as timestamp), 'MM/dd/yyyy') AS target_end_date, dna_jobboard.updated AS updated,
CAST(NULL as string) AS project_division, CAST(NULL as string) AS project_dept, CAST(NULL as string) AS aos_content, CAST(NULL as string) AS project_tags, CAST(NULL as string) AS desired_outcome, CAST(NULL as string) AS project_due_date, CAST(NULL as string) AS project_start_date,
        CASE
            WHEN length(dna_jobboard.certification) > 2 THEN substr(dna_jobboard.certification, 3, length(dna_jobboard.certification) - 3)
            WHEN length(dna_jobboard.certification) <= 3 THEN dna_jobboard.certification
            ELSE NULL
        END AS cert_level,
CAST(NULL as string) AS project_status, CAST(NULL as string) AS project_updated_date, CAST(NULL as string) AS progress_updated_date, 'https://dna.av.ge.com/page/jobboard' AS project_link,
CAST(NULL as string) AS kpi, CAST(NULL as string) AS valuestream, CAST(NULL as string) AS business, CAST(NULL as string) AS project_type, CAST(NULL as string) AS value_stream, CAST(NULL as string) AS engine_module, CAST(NULL as string) AS updated_date,
CAST(NULL as string) AS business_process FROM geagp_ca_dna_jobboard dna_jobboard
LEFT JOIN
geagp_ca_av_hr_pub_person_contact_info_t hr ON hr.person_sso_id = dna_jobboard.engagementleader
union all
SELECT DISTINCT 'AOS Eng Project' AS source, a.project_id AS project_id, b.owner_sso AS owner, cast(left(hr.person_full_nm, 100) as string) AS owner_name,
date_format(cast(a.project_create_date as timestamp), 'MM/dd/yyyy') AS open_date, date_format(cast(a.project_close_date as timestamp), 'MM/dd/yyyy') AS closed_date, CAST(NULL as string) AS part_number,
a.project_name, a.project_statement AS description, a.project_status AS status, CAST(NULL as string) AS site_code, CAST(NULL as string) AS engine_program, CAST(NULL as string) AS action_lifecycle, CAST(NULL as string) AS action_summary,
CAST(NULL as string) AS engagement_type, CAST(NULL as string) AS engagement_leader_multiple, a.bpo AS bpo, CAST(NULL as string) AS bporev, CAST(NULL as string) AS tags, CAST(NULL as string) AS focus, CAST(NULL as string) AS engine_family, CAST(NULL as string) AS comment,
CAST(NULL as string) AS target_end_date, CAST(NULL as string) AS updated, a.project_division AS project_division, a.project_dept AS project_dept, a.aos_content AS aos_content, a.project_tags AS project_tags,
a.desired_outcome AS desired_outcome, date_format(cast(a.project_due_date as timestamp), 'MM/dd/yyyy') AS project_due_date, a.project_start_date AS project_start_date,
a.cert_level AS cert_level, a.project_status AS project_status, a.project_updated_date AS project_updated_date, a.progress_updated_date AS progress_updated_date,
concat('http://supportcentral.ge.com/caseforms/form_print.asp?form_doc_id=', a.project_id) AS project_link, CAST(NULL as string) AS kpi, CAST(NULL as string) AS valuestream, CAST(NULL as string) AS business,
CAST(NULL as string) AS project_type, CAST(NULL as string) AS value_stream, CAST(NULL as string) AS engine_module, CAST(NULL as string) AS updated_date, CAST(NULL as string) AS business_process FROM geagp_ca_aos_eng_projects_io a
left join
geagp_ca_aos_eng_project_owners_io b ON a.project_id = b.project_id
left join
geagp_ca_av_hr_pub_person_contact_info_t hr ON hr.person_sso_id = b.owner_sso
union all
SELECT DISTINCT 'EPEX' AS source, av_sc_pub_epex_project_details_t.proj_id AS project_id, av_sc_pub_epex_project_details_t.sso_id AS owner,
cast(left(hr.person_full_nm, 100) as string) AS owner_name, CAST(NULL as string) AS open_date, CAST(NULL as string) AS closed_date, av_sc_pub_epex_project_details_t.part_nbr AS part_number,
av_sc_pub_epex_project_details_t.proj_ttl AS project_name, av_sc_pub_epex_project_details_t.tg0_notes AS description, av_sc_pub_epex_project_details_t.proj_stts_cd AS status,
CAST(NULL as string) AS site_code, CAST(NULL as string) AS engine_program, CAST(NULL as string) AS action_lifecycle, CAST(NULL as string) AS action_summary, CAST(NULL as string) AS engagement_type, CAST(NULL as string) AS engagement_leader_multiple,
CAST(NULL as string) AS bpo, CAST(NULL as string) AS bporev, CAST(NULL as string) AS tags, CAST(NULL as string) AS focus, av_sc_pub_epex_project_details_t.engine_family, CAST(NULL as string) AS comment, CAST(NULL as string) AS target_end_date, CAST(NULL as string) AS updated,
CAST(NULL as string) AS project_division, CAST(NULL as string) AS project_dept, CAST(NULL as string) AS aos_content, CAST(NULL as string) AS project_tags, CAST(NULL as string) AS desired_outcome, CAST(NULL as string) AS project_due_date, CAST(NULL as string) AS project_start_date,CAST(NULL as string) AS cert_level,
CAST(NULL as string) AS project_status, CAST(NULL as string) AS project_updated_date, CAST(NULL as string) AS progress_updated_date,
concat('https://epex.aviation.ge.com/edstrack/public/index.html#/lighterVersionOfLite/', av_sc_pub_epex_project_details_t.proj_id) AS project_link,
CAST(NULL as string) AS kpi, av_sc_pub_epex_project_details_t.valuestream AS valuestream, CAST(NULL as string) AS business, CAST(NULL as string) AS project_type, av_sc_pub_epex_project_details_t.business AS value_stream,
CAST(NULL as string) AS engine_module, CAST(NULL as string) AS updated_date, CAST(NULL as string) AS business_process FROM geagp_ca_av_sc_pub_epex_project_details_t av_sc_pub_epex_project_details_t
LEFT JOIN geagp_ca_av_hr_pub_person_contact_info_t hr ON hr.person_sso_id = av_sc_pub_epex_project_details_t.sso_id
UNION ALL
SELECT DISTINCT 'QEP' AS source, aos_qep.data_form_entry_number AS project_id, aos_qep.sso AS owner, hr.person_full_nm AS owner_name,
date_format(cast(aos_qep.date_created as timestamp), 'MM/dd/yyyy') AS open_date, date_format(cast(aos_qep.project_presentation_date as timestamp), 'MM/dd/yyyy') AS closed_date,
cast(NULL as string) AS part_number, aos_qep.project_title AS project_name, aos_qep.project_summary AS description, cast(NULL as string) AS status, cast(NULL as string) AS site_code, cast(NULL as string) AS engine_program, cast(NULL as string) AS action_lifecycle, cast(NULL as string) AS action_summary,
cast(NULL as string) AS engagement_type, cast(NULL as string) AS engagement_leader_multiple, cast(NULL as string) AS bpo, cast(NULL as string) AS bporev, cast(NULL as string) AS tags, cast(NULL as string) AS focus, cast(NULL as string) AS engine_family, cast(NULL as string) AS comment, cast(NULL as string) AS target_end_date,
cast(NULL as string) AS updated, cast(NULL as string) AS project_division, cast(NULL as string) AS project_dept, cast(NULL as string) AS aos_content, cast(NULL as string) AS project_tags, cast(NULL as string) AS desired_outcome, cast(NULL as string) AS project_due_date,
cast(NULL as string) AS project_start_date, cast(NULL as string) AS cert_level, cast(NULL as string) AS project_status, cast(NULL as string) AS project_updated_date, cast(NULL as string) AS progress_updated_date,
concat('http://supportcentral.ge.com/dataforms/sup_dataform_display_beta.asp?dataform_id=1319608&dataform_doc_id=', aos_qep.data_form_entry_number) AS project_link,
cast(NULL as string) AS kpi, cast(NULL as string) AS valuestream, cast(NULL as string) AS business, aos_qep.project_type, cast(NULL as string) AS value_stream, cast(NULL as string) AS engine_module, cast(NULL as string) AS updated_date, cast(NULL as string) AS business_process
FROM 
(SELECT aos_qep_1.data_form_entry_number, aos_qep_1.sso, aos_qep_1.date_created, aos_qep_1.project_presentation_date, aos_qep_1.project_title, aos_qep_1.project_summary,
aos_qep_1.name, aos_qep_1.attachment, aos_qep_1.project_type, max(aos_qep_1.date_modified) AS date_modified, aos_qep_1.sso_id_of_record_owner,
aos_qep_1.last_updated_by, aos_qep_1.business_practice FROM geagp_ca_aos_qep aos_qep_1
GROUP BY aos_qep_1.data_form_entry_number, aos_qep_1.sso, aos_qep_1.date_created, aos_qep_1.project_presentation_date,
aos_qep_1.project_title, aos_qep_1.project_summary, aos_qep_1.name, aos_qep_1.attachment, aos_qep_1.project_type, aos_qep_1.sso_id_of_record_owner, aos_qep_1.last_updated_by, aos_qep_1.business_practice) aos_qep
LEFT JOIN geagp_ca_av_hr_pub_person_contact_info_t hr ON hr.person_sso_id = aos_qep.sso
UNION ALL 
SELECT DISTINCT 'AOS Other Projects' AS source, aos_form_allothers.item_id AS project_id, aos_form_allothers.project_leader AS owner,cast(left(hr.person_full_nm, 100) as string) AS owner_name,
date_format(cast(aos_form_allothers.start_date as timestamp), 'MM/dd/yyyy') AS open_date, date_format(cast(aos_form_allothers.end_date as timestamp), 'MM/dd/yyyy') AS closed_date,
aos_form_allothers.part_number, aos_form_allothers.project_title AS project_name, aos_form_allothers.impact AS description, aos_form_allothers.status,
aos_form_allothers.source AS site_code, aos_form_allothers.engine_pgm AS engine_program, cast(NULL as string) AS action_lifecycle, cast(NULL as string) AS action_summary,
cast(NULL as string) AS engagement_type, cast(NULL as string) AS engagement_leader_multiple, aos_form_allothers.bpo, cast(NULL as string) AS bporev, cast(NULL as string) AS tags, cast(NULL as string) AS focus, cast(NULL as string) AS engine_family,
cast(NULL as string) AS comment, cast(NULL as string) AS target_end_date, cast(NULL as string) AS updated, cast(NULL as string) AS project_division, cast(NULL as string) AS project_dept, cast(NULL as string) AS aos_content, cast(NULL as string) AS project_tags,
cast(NULL as string) AS desired_outcome, cast(NULL as string) AS project_due_date, cast(NULL as string) AS project_start_date, 'Other' AS cert_level, cast(NULL as string) AS project_status,cast(NULL as string) AS project_updated_date,
cast(NULL as string) AS progress_updated_date, concat('http://forms.ge.com/record/', aos_form_allothers.item_id) AS project_link, cast(NULL as string) AS kpi,cast(NULL as string) AS valuestream, cast(NULL as string) AS business,
aos_form_allothers.project_type, aos_form_allothers.value_stream, aos_form_allothers.engine_module,
date_format(cast(aos_form_allothers.updated_date as timestamp), 'MM/dd/yyyy') AS updated_date, NULL AS business_process
FROM geagp_ca_aos_form_allothers aos_form_allothers
LEFT JOIN 
geagp_ca_av_hr_pub_person_contact_info_t hr ON hr.person_sso_id = aos_form_allothers.project_leader
union all
SELECT DISTINCT 'AOS Practitioner' AS source, aos_form_practitioner.item_id AS project_id, aos_form_practitioner.project_leader AS owner, cast(left(hr.person_full_nm, 100) as string) AS owner_name,
date_format(cast(aos_form_practitioner.start_date as timestamp), 'MM/dd/yyyy') AS open_date,date_format(cast(aos_form_practitioner.end_date as timestamp), 'MM/dd/yyyy') AS closed_date,
aos_form_practitioner.part_number, aos_form_practitioner.project_title AS project_name, aos_form_practitioner.impact AS description, aos_form_practitioner.status,
aos_form_practitioner.source AS site_code, aos_form_practitioner.engine_pgm AS engine_program, cast(NULL as string) AS action_lifecycle,cast(NULL as string)AS action_summary,
cast(NULL as string) AS engagement_type, cast(NULL as string) AS engagement_leader_multiple, aos_form_practitioner.bpo,cast(NULL as string) AS bporev, cast(NULL as string) AS tags, cast(NULL as string) AS focus, cast(NULL as string) AS engine_family,
cast(NULL as string) AS comment, cast(NULL as string) AS target_end_date, cast(NULL as string) AS updated, cast(NULL as string) AS project_division, cast(NULL as string) AS project_dept, cast(NULL as string) AS aos_content, cast(NULL as string) AS project_tags,
cast(NULL as string) AS desired_outcome, NULL AS project_due_date, NULL AS project_start_date, 'Practitioner' AS cert_level,cast(NULL as string) AS project_status, cast(NULL as string) AS project_updated_date,
cast(NULL as string) AS progress_updated_date, concat('http://forms.ge.com/record/', aos_form_practitioner.item_id) AS project_link, cast(NULL as string) AS kpi, cast(NULL as string) AS valuestream,
cast(NULL as string) AS business, aos_form_practitioner.project_type, aos_form_practitioner.value_stream, aos_form_practitioner.engine_module,
date_format(cast(aos_form_practitioner.updated_date as timestamp), 'MM/dd/yyyy') AS updated_date, cast(NULL as string) AS business_process
FROM geagp_ca_aos_form_practitioner aos_form_practitioner
LEFT JOIN geagp_ca_av_hr_pub_person_contact_info_t hr ON hr.person_sso_id = aos_form_practitioner.project_leader
union all
SELECT DISTINCT 'AOS Foundational' AS source, aos_form_foundational.item_id AS project_id, aos_form_foundational.project_leader AS owner, cast(left(hr.person_full_nm, 100) as string) AS owner_name,
date_format(to_timestamp(project_date, 'dd-MMM-yy'), 'MM/dd/yyyy') as open_date, date_format(to_timestamp(project_date, 'dd-MMM-yy'), 'MM/dd/yyyy') as closed_date,
cast(NULL as string) AS part_number, aos_form_foundational.project_title AS project_name, aos_form_foundational.project_summary AS description, 'Completed' AS status,
cast(NULL as string) AS site_code, cast(NULL as string) AS engine_program, cast(NULL as string) AS action_lifecycle, cast(NULL as string) AS action_summary, cast(NULL as string) AS engagement_type, cast(NULL as string) AS engagement_leader_multiple, cast(NULL as string) AS bpo,
cast(NULL as string) AS bporev, cast(NULL as string) AS tags, cast(NULL as string) AS focus,cast(NULL as string) AS engine_family,cast(NULL as string) AS comment,cast(NULL as string) AS target_end_date,cast(NULL as string) AS updated,cast(NULL as string) AS project_division,
cast(NULL as string) AS project_dept, cast(NULL as string) AS aos_content, cast(NULL as string) AS project_tags, NULL AS desired_outcome, cast(NULL as string) AS project_due_date, cast(NULL as string) AS project_start_date, 'Foundation' AS cert_level,
cast(NULL as string) AS project_status, cast(NULL as string) AS project_updated_date, cast(NULL as string) AS progress_updated_date, concat('http://forms.ge.com/record/', aos_form_foundational.item_id) AS project_link,
cast(NULL as string) AS kpi, cast(NULL as string) AS valuestream, cast(NULL as string) AS business, cast(NULL as string) AS project_type, cast(NULL as string) AS value_stream, cast(NULL as string) AS engine_module, cast(NULL as string) AS updated_date,
aos_form_foundational.business_process FROM geagp_ca_aos_form_foundational aos_form_foundational
LEFT JOIN geagp_ca_av_hr_pub_person_contact_info_t hr ON hr.person_sso_id = aos_form_foundational.project_leader
UNION ALL
SELECT DISTINCT 'Repair' AS source, aos_repair_projects.project_id, cast(NULL as string) AS owner, cast(left(aos_repair_projects.employee, 100) as string) AS owner_name,NULL AS open_date,
cast(NULL as string) AS closed_date, aos_repair_projects.product AS part_number, aos_repair_projects.project_title AS project_name, aos_repair_projects.project_description AS description,
aos_repair_projects.status, aos_repair_projects.plant AS site_code, aos_repair_projects.platform AS engine_program, cast(NULL as string) AS action_lifecycle,
cast(NULL as string) AS action_summary, cast(NULL as string) AS engagement_type,cast(NULL as string) AS engagement_leader_multiple, cast(NULL as string) AS bpo,cast(NULL as string) AS bporev,cast(NULL as string) AS tags,
cast(NULL as string) AS focus,cast(NULL as string) AS engine_family, cast(NULL as string) AS comment, cast(NULL as string) AS target_end_date, cast(NULL as string) AS updated, cast(NULL as string) AS project_division, cast(NULL as string) AS project_dept, cast(NULL as string) AS aos_content,
aos_repair_projects.category AS project_tags, aos_repair_projects.performance_target AS desired_outcome,
date_format(cast(aos_repair_projects.target_completed_date as timestamp), 'MM/dd/yyyy') AS project_due_date,
date_format(cast(aos_repair_projects.project_start as timestamp), 'MM/dd/yyyy') AS project_start_date, cast(NULL as string) AS cert_level,
cast(NULL as string) AS project_status, date_format(cast(aos_repair_projects.update_date as timestamp), 'MM/dd/yy') AS project_updated_date,
cast(NULL as string) AS progress_updated_date, concat('http://forms.ge.com/record/', aos_repair_projects.project_id) AS project_link,
cast(NULL as string) AS kpi, cast(NULL as string) AS valuestream, cast(NULL as string) AS business, cast(NULL as string) AS project_type, cast(NULL as string) AS value_stream, cast(NULL as string) AS engine_module, cast(NULL as string) AS updated_date,
cast(NULL as string) AS business_process FROM geagp_ca_aos_repair_projects aos_repair_projects""")


df.createOrReplaceTempView("final_df")



# Connect to a DB, truncate the target table
conn = pg8000.connect(database=db_name, host=host, port=port, user=db_user, password=password, ssl=True)
cur = conn.cursor()
query = 'TRUNCATE ' + target_table
cur.execute(query)
conn.commit()
cur.close()
conn.close()

df.write.mode('append') \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://" + host + ":" + str(port) + "/" + db_name) \
    .option("dbtable", target_table) \
    .option("user", db_user) \
    .option("password", password) \
    .save()

# Write data to a new S3 partition
df.write.mode('append').parquet(s3_target + target_file + '/' + dt)

# Release the glue data catalog pointer
spark.sql("use " + ca_s3_db_name)
old_partition = "SHOW PARTITIONS " + target_file
old_partition = spark.sql(old_partition).first()["partition"]
old_partition_nm = old_partition[-19:]
drop_old_partition = "ALTER TABLE " + ca_s3_db_name + "." + target_file + " DROP PARTITION (partition_0= '" + old_partition_nm + "\')"
partDF = spark.sql(drop_old_partition)

# Point Glue catalog to the latest partition
partition_location = s3_target + target_file + '/' + dt
table_partition_ddl = "ALTER TABLE " + ca_s3_db_name + "." + target_file + " ADD PARTITION" "(partition_0 = \'" + dt + "\') location \'" + partition_location + "/\' "
partDF = spark.sql(table_partition_ddl)

job.commit()
