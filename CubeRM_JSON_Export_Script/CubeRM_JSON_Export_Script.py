import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
import awswrangler as wr
import pandas as pd
from datetime import date, datetime, timedelta
import time
import json
import boto3
import pytz 
from pytz import timezone
from IPython.display import display
import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType, DoubleType, TimestampType, StructType, StructField, BooleanType
from pyspark.sql.functions import col, concat_ws, when,row_number,lit
from pyspark.sql.window import Window
from pyspark.sql.functions import col, length, StringType, array_join, size
from pyspark.sql.functions import lit
from pyspark.sql.functions import array
from pyspark.sql.functions import regexp_replace

s3_client = boto3.client('s3')
s3_resourse = boto3.resource('s3')

# Get the resolved Glue job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'athena_dbname','bucket_name','output_json_path_key'])

# Extract the database name from the arguments
athena_dbname = args['athena_dbname']
bucket_name = args['bucket_name']
output_json_path_key = args['output_json_path_key']

# Initialize Spark context and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
# spark = glueContext.spark_session

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Pandas to Spark") \
    .getOrCreate()
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
###############################################--Old Import-----------------###############################################--

today = date.today()
#today_date = datetime.now()

start_time_create_df = time.time()
print('start_time_create_df: ',start_time_create_df)

# Fetch yesterday's date using Wrangler
# yesterday_date_query = "SELECT MAX(created_time) FROM cuberm_export_log"
# yesterday_date = wr.athena.read_sql_query(sql=yesterday_date_query, database=athena_dbname, ctas_approach=False).iloc[0, 0]
yesterday_date = '2024-04-26 00:00:53.771000'
print(yesterday_date)
# Get today's date
today_dat = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d-%H-%M-%S')
today_date = datetime.strptime(today_dat, '%Y-%m-%d-%H-%M-%S')

###############################################--New Code-----------------###############################################--


def create_empty_spark_df(spark, columns):
    schema = StructType([
        (StructField(column, IntegerType(), True) if data_type in ('int', 'bigint')
         else StructField(column, StringType(), True) if data_type == 'string'
         else StructField(column, TimestampType(), True) if data_type == 'timestamp'
         else StructField(column, DoubleType(), True) if data_type == 'double'
         else StructField(column, BooleanType(), True) if data_type == 'boolean'
         else StructField(column, StringType(), True))
        for column, data_type in columns.items()
    ])
    return spark.createDataFrame([], schema)


###############################################--New Code-----------------###############################################--

def get_posting_ids(athena_table_name,athena_dbname):
    global posting_ids
    global today_date
    global yesterday_date
    script_name = ('sg_singhealth_spn','au_tenders_ca','ro_elicitatiee_amd','vn_muasamcong_ca','vn_muasamcong_spn','cn_ccgpla_spn','ar_comprar_ca','ar_comprarpub_spn','cn_ccgp_spn','ph_philgeps_ca','pl_propublico_spn','vn_muasamcongopp_pp','vn_muasamcongcpo_ca','it_sicilia_ca','pl_propublico_ca','za_etenders_spn','au_tenders_pp','au_tenders_spn','iq_businews_spn','nl_negometrix_spn','it_sicilia_spn','ae_proc_spn','ro_elicitatieadv_spn','vn_muasamcong_pp','za_etenders_ca','za_etenders_amd','ph_philgeps_spn','ar_comprar_spn','nl_tenderned_ca','cn_ccgp_ca','pl_logintrade_spn','ro_elicitatie_ca','fr_marcheson_spn','ro_elicitatiepi_spn','ro_elicitatiesad_spn','pl_logintradeprzy_spn','ro_elicitatie_spn','tr_ilan_spn')
    # ('fr_marchespublics_ca','fr_boamp_spn','pl_smartpzp_spn','cn_hainan_amd','fr_klekoon','it_cittametro','it_sicilia_spn','br_casadamoeda_spn','it_edisu','br_barueri','sg_sesaminet_spn','ro_elicitatie_spn','it_empulia_ca','it_bari_ca','it_altoadsn','cn_hngp_cor','it_ospedale_archive_ca','au_sa_spn','it_palermo_spn','it_regione','it_novara_spn','cn_cnbidding_ca','it_empulia_spn','it_trascultura_ca','cn_hebei_spn','it_appalti_ca','it_lombardia_spn','it_difesa','br_spgov','cn_hngp_pp','za_etenders_ca','br_ecompras_spn','it_aslnapoli1_spn','ar_comprar_spn','fr_achatpublic','fr_marches','fr_achats','pl_propublico_spn','cn_yancheng_pn','br_colatina','ae_proc_spn','au_vic_spn','br_elicsc','it_romagna_spn','za_etenders_spn','sa_nupco','cn_yancheng_ca','it_ospedale_archive_spn','it_bari_archive_spn','au_vic_ca','it_gpaappaltiamo_spn','it_aslbi','in_gem_spn','it_eappaltifvg_archive_spn','it_palermo_ca','br_ananindeua','fr_boamp_ca','cn_encnbid','it_gare_ca','fr_marcheson_ca','it_start_ca','it_trieste','cn_hngp_spn','nl_tenderned_spn','it_aou','it_bari_spn','cn_chnenergy_ppn','fr_cenedesmarch_spn','cn_gsei','no_doffin_ca','pl_platformaeb_spn','pl_platformaeb_ca','cn_kwbid_spn','it_edmerit','it_lombardia_ca','it_maggiolicloud_ca','in_gem_ca','in_cpppcn_spn','ph_philgeps','it_altoad_ca','br_santa','nl_s2cmercell_spn','br_saojo','pt_base_ca','th_gprogo_spn','it_acquistinretepaopen_spn','br_birigui','vn_muasamcong_ca','cn_encnbid_spn','th_gprogo','sa_nupco_archive_ca','br_ico','pt_base_spn','cn_hainan_spn','it_salerno','ca_merx_ca','cn_yancheng_spn','fr_loire','gb_findtenserv_pp','fr_emarches','it_novara_ca','it_catania','cn_gzqunsheng','ph_philgeps_ca','in_cpppcn_ca','it_ospedale_ca','it_agenziademanio','it_sardegnacat_archive_spn','it_salute_ca','in_odisha','it_maggiolicloud_archive_spn','it_sicilia_ca','it_carabinieri','it_empuliatno_spn','cn_chnenergy_spn','it_mase','it_altoad_spn','cn_zbycg','it_foggia_ca','it_maggiolicloud_spn','vn_muasamcong_pp','sg_singhealth_spn','it_sardegnacat_spn','it_eappaltifvg_spn','br_assisbrasil','br_unfpa_spn','it_aslnapoli1','sa_etimadav','it_vrprovincia','cn_gdebidding','ar_bcra_spn','it_salute_spn','ar_santacruz_spn','au_tenders_ca','it_unirc','it_stella_spn','it_suedtirol','sa_etimadt','fr_emarches_ca','it_mise','br_balneariocamboriu_spn','th_gprogo_ca','cn_hebei_amd','it_empulia','fr_marchespublics','pl_logintrade_spn','fr_cenedesmarch','ac_ungm_ca','it_altoadsn_spn','fr_megalis','ph_philgeps_spn','sa_nupco_ca','it_tuabru_ca','au_tenders_pp','fr_marcheson','cn_ccgp_spn','ca_bcbid_ca','br_leopoldina','it_liguria','vn_muasamcongopp_pp','it_trascultura_gpn','br_belohorizon','ar_santafe_spn','sg_sesami_spn','cn_cnbidding_cor','it_rimini_ca','ro_elicitatie_ca','it_soresa_spn','it_foggia_spn','cn_encnbid_ca','it_policlinic','fr_marcheson_spn','cn_sdicc','it_empuliacpm_spn','br_balneariocamboriu_ca','sa_nupco_spn','it_aoupu','it_gpaappaltiamo_ca','sa_etimadq','nl_tenderned_ca','it_acquistinretepa_spn','it_aslnapoli1_ca','cn_cnbidding_spn','it_rimini_spn','in_mptenders','ar_bcra_ca','it_aslnapoli1_gpn','fr_marcheawsol_spn','br_conlicitacao_spn','it_ospedale_spn','fr_centreo','it_uslumbria','it_unibas_ca','cn_kwbid_cor','pl_platformza_spn','it_trascultura_spn','no_doffin_pp','it_aric','fr_marcsecuris','it_puglia','br_conlicitacao_ca','cn_kwbid_ca','it_casadivetro_ca','ca_bcbid_spn','fr_lemoniteur','gb_findtenserv_spn','it_sardegna','cn_chinabidd','au_nsw_spn','br_tjsp','it_maggiolicloud_archive_ca','cn_chinabidd_spn','it_gpaappaltiamo_archive_ca','it_gpaappaltiamo_archive_spn','in_wbtenders','iq_businews_spn','it_romagna','br_portaldecompras','cn_gzswbc_spn','cn_gzswbc_amd','br_aracajuco','fr_proxil','sa_etimadast','be_enabel_spn','nl_tenderned_pp','sa_nupco_archive_spn','pl_logintradeprzy_spn','br_vendorun','it_starttoscana_archive_spn','it_ingate','cn_gzswbc_ca','it_roma','it_altoad_sn','ac_ungm_spn','za_etenders_amd','in_mahatenders','no_doffin_spn','th_gprogo_pp','it_start_spn','cn_chinabidd_ca','it_starttoscana_spn','fr_boamp_ted_ca','fr_opco2i','fr_marchespublics_spn','pl_propublico_ca','au_sa_ca','au_tenders_spn','cn_hainan_pp','it_centraleacquisti','cn_yancheng','it_romagna_ca','ca_merx_spn','gb_findtenserv_ca','fr_boamp_ted_spn','br_compraspub','br_comprasnet','it_trasparenza','br_serro','fr_coordination_spn','cn_mofcom','br_ouro')
    
    query = f"SELECT DISTINCT posting_id FROM {athena_dbname}.{athena_table_name} WHERE update_date BETWEEN TIMESTAMP'{yesterday_date}' AND TIMESTAMP'{today_date}' and notice_text is not null and is_publish_on_gec = true and (completed_steps != 'deleted' or completed_steps is null) and script_name IN {script_name}"
    # and  not exists  (select 1 from cuberm_export_log where tender.posting_id = cuberm_export_log.posting_ids)
    posting_ids = wr.athena.read_sql_query(sql=query, database=athena_dbname, ctas_approach=False)['posting_id'].tolist()

    return posting_ids


def CreateDFFromTable(athena_table_name, athena_dbname, column_mapping,start_posting_id, end_posting_id):
    global posting_ids
    global today_date
    global yesterday_date
    global spark
    
    # Get the column names and data types from the mapping
    columns = column_mapping.get(athena_table_name, {})
    
    script_name = ('sg_singhealth_spn','au_tenders_ca','ro_elicitatiee_amd','vn_muasamcong_ca','vn_muasamcong_spn','cn_ccgpla_spn','ar_comprar_ca','ar_comprarpub_spn','cn_ccgp_spn','ph_philgeps_ca','pl_propublico_spn','vn_muasamcongopp_pp','vn_muasamcongcpo_ca','it_sicilia_ca','pl_propublico_ca','za_etenders_spn','au_tenders_pp','au_tenders_spn','iq_businews_spn','nl_negometrix_spn','it_sicilia_spn','ae_proc_spn','ro_elicitatieadv_spn','vn_muasamcong_pp','za_etenders_ca','za_etenders_amd','ph_philgeps_spn','ar_comprar_spn','nl_tenderned_ca','cn_ccgp_ca','pl_logintrade_spn','ro_elicitatie_ca','fr_marcheson_spn','ro_elicitatiepi_spn','ro_elicitatiesad_spn','pl_logintradeprzy_spn','ro_elicitatie_spn','tr_ilan_spn')
    # ('fr_marchespublics_ca','fr_boamp_spn','pl_smartpzp_spn','cn_hainan_amd','fr_klekoon','it_cittametro','it_sicilia_spn','br_casadamoeda_spn','it_edisu','br_barueri','sg_sesaminet_spn','ro_elicitatie_spn','it_empulia_ca','it_bari_ca','it_altoadsn','cn_hngp_cor','it_ospedale_archive_ca','au_sa_spn','it_palermo_spn','it_regione','it_novara_spn','cn_cnbidding_ca','it_empulia_spn','it_trascultura_ca','cn_hebei_spn','it_appalti_ca','it_lombardia_spn','it_difesa','br_spgov','cn_hngp_pp','za_etenders_ca','br_ecompras_spn','it_aslnapoli1_spn','ar_comprar_spn','fr_achatpublic','fr_marches','fr_achats','pl_propublico_spn','cn_yancheng_pn','br_colatina','ae_proc_spn','au_vic_spn','br_elicsc','it_romagna_spn','za_etenders_spn','sa_nupco','cn_yancheng_ca','it_ospedale_archive_spn','it_bari_archive_spn','au_vic_ca','it_gpaappaltiamo_spn','it_aslbi','in_gem_spn','it_eappaltifvg_archive_spn','it_palermo_ca','br_ananindeua','fr_boamp_ca','cn_encnbid','it_gare_ca','fr_marcheson_ca','it_start_ca','it_trieste','cn_hngp_spn','nl_tenderned_spn','it_aou','it_bari_spn','cn_chnenergy_ppn','fr_cenedesmarch_spn','cn_gsei','no_doffin_ca','pl_platformaeb_spn','pl_platformaeb_ca','cn_kwbid_spn','it_edmerit','it_lombardia_ca','it_maggiolicloud_ca','in_gem_ca','in_cpppcn_spn','ph_philgeps','it_altoad_ca','br_santa','nl_s2cmercell_spn','br_saojo','pt_base_ca','th_gprogo_spn','it_acquistinretepaopen_spn','br_birigui','vn_muasamcong_ca','cn_encnbid_spn','th_gprogo','sa_nupco_archive_ca','br_ico','pt_base_spn','cn_hainan_spn','it_salerno','ca_merx_ca','cn_yancheng_spn','fr_loire','gb_findtenserv_pp','fr_emarches','it_novara_ca','it_catania','cn_gzqunsheng','ph_philgeps_ca','in_cpppcn_ca','it_ospedale_ca','it_agenziademanio','it_sardegnacat_archive_spn','it_salute_ca','in_odisha','it_maggiolicloud_archive_spn','it_sicilia_ca','it_carabinieri','it_empuliatno_spn','cn_chnenergy_spn','it_mase','it_altoad_spn','cn_zbycg','it_foggia_ca','it_maggiolicloud_spn','vn_muasamcong_pp','sg_singhealth_spn','it_sardegnacat_spn','it_eappaltifvg_spn','br_assisbrasil','br_unfpa_spn','it_aslnapoli1','sa_etimadav','it_vrprovincia','cn_gdebidding','ar_bcra_spn','it_salute_spn','ar_santacruz_spn','au_tenders_ca','it_unirc','it_stella_spn','it_suedtirol','sa_etimadt','fr_emarches_ca','it_mise','br_balneariocamboriu_spn','th_gprogo_ca','cn_hebei_amd','it_empulia','fr_marchespublics','pl_logintrade_spn','fr_cenedesmarch','ac_ungm_ca','it_altoadsn_spn','fr_megalis','ph_philgeps_spn','sa_nupco_ca','it_tuabru_ca','au_tenders_pp','fr_marcheson','cn_ccgp_spn','ca_bcbid_ca','br_leopoldina','it_liguria','vn_muasamcongopp_pp','it_trascultura_gpn','br_belohorizon','ar_santafe_spn','sg_sesami_spn','cn_cnbidding_cor','it_rimini_ca','ro_elicitatie_ca','it_soresa_spn','it_foggia_spn','cn_encnbid_ca','it_policlinic','fr_marcheson_spn','cn_sdicc','it_empuliacpm_spn','br_balneariocamboriu_ca','sa_nupco_spn','it_aoupu','it_gpaappaltiamo_ca','sa_etimadq','nl_tenderned_ca','it_acquistinretepa_spn','it_aslnapoli1_ca','cn_cnbidding_spn','it_rimini_spn','in_mptenders','ar_bcra_ca','it_aslnapoli1_gpn','fr_marcheawsol_spn','br_conlicitacao_spn','it_ospedale_spn','fr_centreo','it_uslumbria','it_unibas_ca','cn_kwbid_cor','pl_platformza_spn','it_trascultura_spn','no_doffin_pp','it_aric','fr_marcsecuris','it_puglia','br_conlicitacao_ca','cn_kwbid_ca','it_casadivetro_ca','ca_bcbid_spn','fr_lemoniteur','gb_findtenserv_spn','it_sardegna','cn_chinabidd','au_nsw_spn','br_tjsp','it_maggiolicloud_archive_ca','cn_chinabidd_spn','it_gpaappaltiamo_archive_ca','it_gpaappaltiamo_archive_spn','in_wbtenders','iq_businews_spn','it_romagna','br_portaldecompras','cn_gzswbc_spn','cn_gzswbc_amd','br_aracajuco','fr_proxil','sa_etimadast','be_enabel_spn','nl_tenderned_pp','sa_nupco_archive_spn','pl_logintradeprzy_spn','br_vendorun','it_starttoscana_archive_spn','it_ingate','cn_gzswbc_ca','it_roma','it_altoad_sn','ac_ungm_spn','za_etenders_amd','in_mahatenders','no_doffin_spn','th_gprogo_pp','it_start_spn','cn_chinabidd_ca','it_starttoscana_spn','fr_boamp_ted_ca','fr_opco2i','fr_marchespublics_spn','pl_propublico_ca','au_sa_ca','au_tenders_spn','cn_hainan_pp','it_centraleacquisti','cn_yancheng','it_romagna_ca','ca_merx_spn','gb_findtenserv_ca','fr_boamp_ted_spn','br_compraspub','br_comprasnet','it_trasparenza','br_serro','fr_coordination_spn','cn_mofcom','br_ouro')

    # Construct the SELECT query with dynamic casting
    select_query = f"SELECT "
    for column, data_type in columns.items():
        if data_type == 'timestamp':
            select_query += f"CAST({column} AS VARCHAR) AS {column}, "
        else:
            select_query += f"{column}, "
    select_query = select_query.rstrip(", ")  # Remove the trailing comma
    
    # Construct the full SQL query
    full_query = f"{select_query} FROM {athena_dbname}.{athena_table_name}"

  
    # Fetch unique posting_ids if the table is "tender" and matches the script_name
#    if athena_table_name == "tender":
#        query = f"SELECT DISTINCT posting_id FROM {athena_dbname}.{athena_table_name} WHERE update_date BETWEEN TIMESTAMP'{yesterday_date}' AND TIMESTAMP'{today_date}' and notice_text is not null and is_publish_on_gec = true and (completed_steps != 'deleted' or completed_steps is null) and script_name IN {script_name}"
#        posting_ids = wr.athena.read_sql_query(sql=query, database=athena_dbname, ctas_approach=False)['posting_id'].tolist()
    
#    print(posting_ids)
    # Read data from Athena using Wrangler
    if posting_ids is not None and posting_ids != []:
        # posting_ids_str = ','.join(map(str, posting_ids))
        # query = f"{select_query} FROM {athena_dbname}.{athena_table_name} WHERE posting_id IN ({posting_ids_str})"
        # print(posting_ids)
        df = pd.DataFrame()  # Initialize an empty DataFrame to store results
#        for i in range(0, len(posting_ids), 10000):  # Fetch data in batches of 10000 posting_ids
        print('start_posting_id:',start_posting_id)
        print('end_posting_id:',end_posting_id)
        batch_ids = posting_ids[start_posting_id:end_posting_id]  # Get a batch of posting_ids
        
        print('len of batch_ids: ',len(batch_ids))
        posting_ids_str = ','.join(map(str, batch_ids))  # Convert posting_ids to string format for SQL query
        query = f"{select_query} FROM {athena_dbname}.{athena_table_name} WHERE posting_id IN ({posting_ids_str})"
        # print("query",athena_table_name)
        
        batch_df = wr.athena.read_sql_query(sql=query, database=athena_dbname, ctas_approach=False)
        df = pd.concat([df, batch_df], ignore_index=True) # Concatenate batch_df with df

    else:
        print("no records")
        spark = SparkSession.builder.appName("CreateEmptyDF").getOrCreate()
        empty_spark_df = create_empty_spark_df(spark, columns)
        return empty_spark_df
        
    
#    df = wr.athena.read_sql_query(sql=query, database=athena_dbname, ctas_approach=False)
    
    # # Convert timestamp columns to datetime
    # for column, data_type in columns.items():
    #     if data_type == 'timestamp' and f'{column}' in df.columns:
    #         df[column] = pd.to_datetime(df[f'{column}'].replace(" UTC",""), format='%Y-%m-%d %H:%M:%S')
    #         #df.drop(columns=[f'{column}_string'], inplace=True)
    
    # Convert timestamp columns to datetime
    for column, data_type in columns.items():
        if data_type == 'timestamp' and f'{column}' in df.columns:
            df[column] = pd.to_datetime(df[f'{column}'].str.replace(" UTC", ""), format='%Y-%m-%d %H:%M:%S.%f')


    
    # Define the schema for the PySpark DataFrame
    schema = StructType([
        (StructField(column, IntegerType(), True) if data_type in ('int', 'bigint')
        else StructField(column, StringType(), True) if data_type == 'string'
        else StructField(column, TimestampType(), True) if data_type == 'timestamp'
        else StructField(column, DoubleType(), True) if data_type == 'double'
        else StructField(column, BooleanType(), True) if data_type == 'boolean'
        else StructField(column, StringType(), True))
        for column, data_type in columns.items()
    ])
    
    print('schema:',schema)

    # Set default values for each data type
    default_values = {
        'int': 0,
        'bigint': 0,
        'string': None,
        'timestamp': None,
        'date': None,
        'double': 0.0,
        'boolean': False
    }

    # Define a function to replace NA values with default values
    def replace_na(value, default_value):
        if pd.isna(value):
            return default_value
        return value
    
    # Convert the columns to appropriate data types and set default values for missing values
    for column, data_type in columns.items():
        if df[column].isnull().any():
            default_value = default_values[data_type]
            df[column] = df[column].apply(lambda x: replace_na(x, default_value))

    # Convert Pandas DataFrame to PySpark DataFrame using the retrieved schema
    spark_df = spark.createDataFrame(df, schema)

    return spark_df

posting_ids = None

# Define column mappings for each table
column_mapping = {
    "tender": {
        "is_deadline_assumed": "boolean",
        "est_amount": "double",
        "procurement_method": "bigint",
        "notice_deadline": "timestamp",
        "publish_date": "timestamp",
        "creator_id": "bigint",
        "is_publish_on_gec": "boolean",
        "crawled_at": "timestamp",
        "dispatch_date": "timestamp",
        "grossbudgeteuro": "double",
        "netbudgeteuro": "double",
        "grossbudgetlc": "double",
        "netbudgetlc": "double",
        "vat": "double",
        "notice_type": "bigint",
        "document_cost": "double",
        "document_purchase_start_time": "date",
        "document_purchase_end_time": "date",
        "pre_bid_meeting_date": "date",
        "document_opening_time": "date",
        "update_date": "timestamp",
        "performance_country": "string",
        "performance_state": "string",
        "cpvs": "string",
        "funding_agencies": "string",
        "lot_details": "string",
        "custom_tags": "string",
        "customer_details": "string",
        "tender_criteria": "string",
        "attachments": "string",
        "tender_cancellation_date": "timestamp",
        "tender_contract_end_date": "timestamp",
        "tender_contract_start_date": "timestamp",
        "tender_is_canceled": "boolean",
        "tender_award_date": "timestamp",
        "tender_max_quantity": "double",
        "tender_min_quantity": "double",
        "is_publish_assumed": "boolean",
        "posting_id": "bigint",
        "notice_no": "string",
        "notice_title": "string",
        "main_language": "string",
        "tender_quantity": "string",
        "currency": "string",
        "cpv_at_source": "string",
        "eligibility": "string",
        "tender_id": "string",
        "notice_contract_type": "string",
        "script_name": "string",
        "source_of_funds": "string",
        "class_at_source": "string",
        "notice_status": "string",
        "document_fee": "string",
        "completed_steps": "string",
        "class_codes_at_source": "string",
        "related_tender_id": "string",
        "additional_source_name": "string",
        "additional_source_id": "string",
        "additional_tender_url": "string",
        "category": "string",
        "local_title": "string",
        "contract_type_actual": "string",
        "class_title_at_source": "string",
        "bidding_response_method": "string",
        "notice_text": "string",
        "notice_url": "string",
        "document_type_description": "string",
        "type_of_procedure": "string",
        "type_of_procedure_actual": "string",
        "notice_summary_english": "string",
        "identifier": "string",
        "tender_contract_number": "string",
        "contract_duration": "string",
        "project_name": "string",
        "tender_quantity_uom": "string",
        "earnest_money_deposit": "string",
        "local_description": "string",
        "notice_id": "string",
        "is_lot_default": "boolean"

    },
    "attachments": {
        "posting_id":	"bigint",
        "created_time": "timestamp",
        "file_size":	"double",
        "file_description":	"string",
        "external_url":"string",
        "file_type":"string",
        "file_name":"string",
        "id":"string"
        # Add other columns and their data types for the "tender" table
    },
    "award_details": {
        "netawardvalueeuro": "double",
        "grossawardvaluelc": "double",
        "netawardvaluelc": "double",
        "award_quantity": "double",
        "posting_id": "bigint",
        "lot_id": "string",
        "initial_estimated_value": "double",
        "award_date": "timestamp",
        "final_estimated_value": "double",
        "grossawardvalueeuro": "double",
        "bidder_country": "string",
        "bidder_name": "string",
        "address": "string",
        "bid_recieved": "string",
        "contract_duration": "string",
        "winner_group_name": "string",
        "notes": "string",
        "lot_actual_number": "string",
        "id": "string"
    },
    "custom_tags": {
        "custom_tags_id": "bigint",
        "posting_id": "bigint",
        "tender_custom_tag_description": "string",
        "tender_custom_tag_value": "string",
        "tender_custom_tag_company_id": "string",
        "tender_custom_tag_tender_id": "string"
    },
    "customer_details": {
        "org_type": "bigint",
        "org_parent_id": "bigint",
        "org_status": "bigint",
        "posting_id": "bigint",
        "created_time": "timestamp",
        "address_id": "bigint",
        "org_id": "bigint",
        "org_phone": "string",
        "org_fax": "string",
        "org_website": "string",
        "org_city": "string",
        "customer_nuts": "string",
        "type_of_authority_code": "string",
        "customer_main_activity": "string",
        "postal_code": "string",
        "contact_person": "string",
        "org_name": "string",
        "org_description": "string",
        "org_email": "string",
        "org_address": "string",
        "org_state": "string",
        "org_country": "string",
        "org_language": "string"
    },
    "lot_cpv_mapping": {
        "posting_id": "bigint",
        "lot_id": "string",
        "is_primary": "boolean",
        "lot_cpv_code": "string",
        "id": "string"
    },
    "lot_criteria": {
        "posting_id": "bigint",
        "lot_id": "string",
        "lot_criteria_id": "bigint",
        "lot_is_price_related": "boolean",
        "lot_criteria_weight": "bigint",
        "lot_criteria_title": "string"
    },
    "lot_details": {
        "lot_cancellation_date": "timestamp",
        "lot_award_date": "timestamp",
        "lot_grossbudget_lc": "double",
        "lot_cpv_codes": "string",
        "lot_criteria": "string",
        "award_details": "string",
        "lot_is_canceled": "boolean",
        "lot_number": "bigint",
        "lot_netbudget_lc": "double",
        "lot_vat": "double",
        "lot_quantity": "double",
        "lot_min_quantity": "double",
        "lot_id": "string",
        "lot_max_quantity": "double",
        "posting_id": "bigint",
        "lot_grossbudget": "double",
        "contract_start_date": "timestamp",
        "contract_end_date": "timestamp",
        "lot_netbudget": "double",
        "lot_contract_type_actual": "string",
        "lot_actual_number": "string",
        "lot_title": "string",
        "lot_description": "string",
        "lot_quantity_uom": "string",
        "contract_number": "string",
        "contract_duration": "string",
        "lot_nuts": "string",
        "contract_type": "string",
        "lot_description_english": "string",
        "lot_title_english": "string",
        "lot_class_codes_at_source": "string",
        "lot_cpv_at_source": "string"
    },
    "notice_cpv_mapping": {
        "posting_id": "bigint",
        "is_primary": "boolean",
        "cpv_code": "string",
        "id": "string"
    },
    "notice_fundings": {
        "posting_id": "bigint",
        "funding_agency": "bigint",
        "id": "string"
    },
    "performance_country": {
        "posting_id": "bigint",
        "performance_country": "string",
        "id": "string"
    },
    "performance_state": {
        "posting_id": "bigint",
        "performance_state": "string",
        "id": "string"
    },
    "tender_criteria": {
        "posting_id": "bigint",
        "tender_criteria_id": "string",
        "tender_criteria_weight": "bigint",
        "tender_is_price_related": "boolean",
        "tender_criteria_title": "string"
    }
    
    # Add mappings for other tables
}

posting_ids_list  = get_posting_ids("tender", athena_dbname)
print('posting_ids_list:',len(posting_ids_list))
# You need to pass the connection as well
for i in range(0, len(posting_ids_list), 10000):  # Fetch data in batches of 25000 posting_ids

    
    # Example usage
    # You need to pass the connection as well
    df_tender = CreateDFFromTable("tender", athena_dbname, column_mapping,i, i+10000)
    df_attachments = CreateDFFromTable("attachments", athena_dbname, column_mapping,i, i+10000)
    df_award_details = CreateDFFromTable("award_details", athena_dbname, column_mapping,i, i+10000)
    df_custom_tags = CreateDFFromTable("custom_tags", athena_dbname, column_mapping,i, i+10000)
    df_lot_criteria = CreateDFFromTable("lot_criteria", athena_dbname, column_mapping,i, i+10000)
    df_lot_cpv_mapping = CreateDFFromTable("lot_cpv_mapping", athena_dbname, column_mapping,i, i+10000)
    df_customer_details = CreateDFFromTable("customer_details", athena_dbname, column_mapping,i, i+10000)
    df_lot_details = CreateDFFromTable("lot_details", athena_dbname, column_mapping,i, i+10000)
    df_notice_cpv_mapping = CreateDFFromTable("notice_cpv_mapping", athena_dbname, column_mapping,i, i+10000)
    df_notice_fundings = CreateDFFromTable("notice_fundings", athena_dbname, column_mapping,i, i+10000)
    df_performance_state = CreateDFFromTable("performance_state", athena_dbname, column_mapping,i, i+10000)
    df_performance_country = CreateDFFromTable("performance_country", athena_dbname, column_mapping,i, i+10000)
    df_tender_criteria = CreateDFFromTable("tender_criteria", athena_dbname, column_mapping,i, i+10000)
    # Call for other tables...
    
    
    ###############################################--Old Code-----------------###############################################--
    
    
    
    df_posting_id_1 = df_tender.filter(
        (col('notice_title').isNotNull()) &
        (col('local_title').isNotNull()) &
        (col('publish_date').isNotNull())
    ).select('posting_id').distinct()
    
    
    print('number of posting_ids:',df_posting_id_1.count())
    
    
    # print('====================')
    new_df_tender = df_tender.join(df_posting_id_1,['posting_id'],'inner')
    
    print('Processing on df_tender')
    
    
    if new_df_tender.isEmpty():
        print("df is empty")
    
    else:
    
    
    
        new_df_tender = new_df_tender.withColumn(
            "notice_no",
            when(
                (col("notice_no").isNotNull()) & (col("notice_no") != ""),
    #        (col("notice_no").isNotNull()) & (col("notice_no") != ""),
                concat_ws(
                    "_",
                    col("script_name"),
                    col("notice_no"),
                    col("posting_id")
                )
            ).otherwise(
                concat_ws(
                    "_",
                    col("script_name"),
                    col("posting_id")
                )
            )
        )
    
    
        new_df_tender = new_df_tender.withColumn(
            "notice_type",
            when((new_df_tender["notice_type"] == 2) | (new_df_tender["notice_type"] == 3), 1)
            .when((new_df_tender["notice_type"] == 4) | (new_df_tender["notice_type"] == 5) | (new_df_tender["notice_type"] == 6) | (new_df_tender["notice_type"] == 8), 2)
            .when(new_df_tender["notice_type"] == 7, 3)
            .when(new_df_tender["notice_type"] == 16, 4)
            .otherwise(0)
        )
    
        new_df_tender = new_df_tender.select('crawled_at','script_name','notice_no','related_tender_id','additional_source_name','additional_source_id','dispatch_date','publish_date','notice_deadline','notice_type','document_type_description','type_of_procedure','type_of_procedure_actual','notice_title','local_title','notice_summary_english','grossbudgeteuro','netbudgeteuro','grossbudgetlc','netbudgetlc','vat','notice_url','additional_tender_url','currency','main_language','posting_id','notice_text','local_description',
                                            'tender_quantity',
                'tender_min_quantity','tender_max_quantity','tender_quantity_uom','tender_contract_number','contract_duration',
                'tender_contract_start_date','tender_contract_end_date','tender_is_canceled','tender_cancellation_date',
                'tender_award_date','contract_type_actual','cpv_at_source','is_lot_default')
                
    #    df2 = df.withColumn("age",col("age").cast(StringType())) \
    #            .withColumn("isGraduated",col("isGraduated").cast(BooleanType())) \
    #            .withColumn("jobStartDate",col("jobStartDate").cast(DateType()))
        print('processed on df_tender')
    
    
        # Creating cubeRM df for cubeRM table
        print('Creating cubeRM df for cubeRM table')
    ##536
    
        today_dates = [today_date for i in range(1, new_df_tender.count() + 1)]
    
        id_column = [str(uuid.uuid4()) + str(datetime.now().strftime("-%Y-%m-%d-%H:%M:%S.%f")) for i in range(1,new_df_tender.count()+1)]
    
        new_columns_df_1 = spark.createDataFrame(zip(id_column, today_dates), ["id_col", "created_time"])
        new_columns_df_1.show()
    
        new_columns_df_2 = new_columns_df_1.withColumn("created_time", col("created_time").cast(TimestampType()))
    
        cuberm_temp = new_df_tender.select('posting_id','script_name')
    
        w = Window().orderBy(lit('A'))
        cuberm_temp = cuberm_temp.withColumn('id', row_number().over(w))
        new_columns_df_2 = new_columns_df_2.withColumn('id', row_number().over(w))
    
        #join together both DataFrames using 'id' column
        CubeRM_df = cuberm_temp.join(new_columns_df_2, on=['id']).drop('id')
    
        CubeRM_df = CubeRM_df.withColumnRenamed("id_col","id")
        CubeRM_df = CubeRM_df.withColumnRenamed("posting_id","posting_ids")
    
        CubeRM_df = CubeRM_df.select('id','posting_ids','created_time','script_name')
        # print("CubeRM_df: after selecting 'id','posting_ids','created_time','script_name': ")
        
        
        CubeRM_df.show()
    
        # glueContext.write_data_frame.from_catalog(frame = CubeRM_df,database = athena_dbname,table_name = 'cuberm_export_log')
        # print('new data added in cuberm_export_log tables ')
    
    
        # performance_country
        print('processing on df_performance_country')
    
        performance_country_df_1 = df_performance_country.join(df_posting_id_1,['posting_id'],'inner')
    
        performance_country_df_1 = performance_country_df_1.select('performance_country','posting_id')
    
        # print('processed on df_performance_country')
    #        performance_country_df_1.show()
    
    
        # customer_details 
        print('processing on df_customer_details')
    
        customer_details_df_1 = df_customer_details.join(df_posting_id_1,['posting_id'],'inner')
    
        customer_details_df_new = customer_details_df_1.select('org_name','org_country','org_city','org_address','postal_code','org_email','org_phone','customer_nuts','type_of_authority_code','customer_main_activity','posting_id')
    
        customer_schema = F.struct(
                F.col("org_city").alias("city"),
                F.col("org_country").alias("country"),
                F.col("org_email").alias("email"),
                F.col("customer_main_activity").alias("mainActivity"),
                F.col("org_name").alias("name"),
                F.col("customer_nuts").alias("nuts"),
                F.col("org_phone").alias("phone"),
                F.col("postal_code").alias("postalCode"),
                F.col("org_address").alias("street"),
                F.col("type_of_authority_code").alias("typeOfAuthorityCode")
    
    
        ).alias("customer")
    
        customer_df_with_schema = customer_details_df_new.withColumn("customer", customer_schema)#
    
        customer_df_with_schema = customer_df_with_schema.drop('org_name','org_city','org_address','org_country','postal_code','org_email','org_phone','customer_nuts','type_of_authority_code','customer_main_activity')
    
        customer_df_with_schema = (customer_df_with_schema.groupby('posting_id').agg(F.collect_list("customer").alias('customer')))
    
        customer_details_df_new_org_country = customer_details_df_1.select('org_country','posting_id').distinct()
        print('processed on df_customer_details')
            # customer_df_with_schema.show()
    
        # custom_tags
        print('processing on df_custom_tags')
        custom_tags_df_1 = df_custom_tags.join(df_posting_id_1,['posting_id'],'inner')
    
        custom_tags_df_new = custom_tags_df_1.select('tender_custom_tag_description','tender_custom_tag_value','tender_custom_tag_company_id','posting_id')
    
        custom_tag_schema = F.struct(
                F.col("tender_custom_tag_description").alias("description"),
                F.col("tender_custom_tag_value").alias("value"),
                F.col("tender_custom_tag_company_id").alias("companyId")
    
        ).alias('customTags')
    
        custom_tag_with_schema = custom_tags_df_new.withColumn("customTags", custom_tag_schema)
    
        custom_tag_with_schema = custom_tag_with_schema.drop('tender_custom_tag_description','tender_custom_tag_value','tender_custom_tag_company_id')
    
        custom_tag_df_with_schema = (custom_tag_with_schema.groupby('posting_id').agg(F.collect_list("customTags").alias('customTags')))
        print('processed on df_custom_tags')
    #        custom_tag_df_with_schema.show()
    
        # tender_criteria
        print('processing on df_tender_criteria')
        tender_criteria_df_1 = df_tender_criteria.join(df_posting_id_1,['posting_id'],'inner')
    
        tender_criteria_df_new = tender_criteria_df_1.select('tender_criteria_title','tender_criteria_weight','tender_is_price_related','posting_id')
    
        tender_criteria_schema = F.struct(
                F.col("tender_criteria_title").alias("title"),
                F.col("tender_criteria_weight").alias("weight"),
                F.col("tender_is_price_related").alias("isPriceRelated")
    
        ).alias("awardCriteria")
    
        tender_criteria_with_schema = tender_criteria_df_new.withColumn("awardCriteria", tender_criteria_schema)
    
        tender_criteria_with_schema = tender_criteria_with_schema.drop('tender_criteria_title','tender_criteria_weight','tender_is_price_related')
    
        tender_criteria_df_with_schema = (tender_criteria_with_schema.groupby('posting_id').agg(F.collect_list("awardCriteria").alias('awardCriteria')))
        print('processed on df_tender_criteria')
    #        tender_criteria_df_with_schema.show()
    
        # lot_details
        print('processing on df_lot_details')
        lot_details_df_1 = df_lot_details.join(df_posting_id_1,['posting_id'],'inner')
    
        distinct_lot_id = lot_details_df_1.select('lot_id')
    
        lot_details_df_2 = lot_details_df_1.join(distinct_lot_id,['lot_id'],'inner')
    
        lot_details_df_new = lot_details_df_2.select('lot_number','lot_title','lot_description','lot_grossbudget','lot_netbudget',
                            'lot_grossbudget_lc','lot_netbudget_lc','lot_vat','lot_quantity','lot_min_quantity',
                            'lot_max_quantity','lot_quantity_uom','contract_number','contract_duration','contract_start_date','contract_end_date',
                            'lot_nuts','lot_is_canceled','lot_cancellation_date','lot_award_date','contract_type','lot_actual_number','posting_id','lot_id','lot_cpv_at_source')
    
        lot_details_df_new = lot_details_df_new.withColumnRenamed("contract_duration","lot_detail_contract_duration")
        print('processed on df_lot_details')
        # print('lot_details_df_new_schema:',lot_details_df_new.printSchema())
    
    
    
        # award_details
        print('processing on df_award_details')
    
        award_details_df_1 = df_award_details.join(distinct_lot_id,['lot_id'],'inner')
    
        award_details_df_new = award_details_df_1.select('bidder_name','winner_group_name','grossawardvalueeuro',
                            'netawardvalueeuro','grossawardvaluelc','netawardvaluelc','award_quantity','notes','posting_id','lot_id')
    
        award_details_df_new_distinct = award_details_df_new
        print('processed on df_award_details')
        # print('award_details_df_new_distinct_schema:',award_details_df_new_distinct.printSchema())
    
        # lot criteria
        print('processing on df_lot_criteria')
        lot_criteria_df_1 = df_lot_criteria.join(distinct_lot_id,['lot_id'],'inner')
    
        lot_criteria_df_new = lot_criteria_df_1.select('lot_criteria_title','lot_criteria_weight','lot_is_price_related','posting_id','lot_id')
    
        lot_criteria_df_new_distinct = lot_criteria_df_new
        print('processed on df_lot_criteria')
        # print('lot_criteria_df_new_distinct_schema:',lot_criteria_df_new_distinct.printSchema())
    
        # attachment
        print('processing on df_attachments')
    
        df_attachments_df_1 = df_attachments.join(df_posting_id_1,['posting_id'],'inner')
    
        df_attachments_df_1 = df_attachments_df_1.filter((df_attachments_df_1['external_url'].isNotNull()) & (df_attachments_df_1['file_description'].isNotNull()))
    
        attachments_df_new = df_attachments_df_1.select('file_description','external_url','file_size','posting_id')
    
        attachments_schema = F.struct(
                F.col("file_description").alias("description"),
                F.col("file_size").alias("size"),
                F.col("external_url").alias("url")
    
        ).alias("attachments")
    
        attachments_df_with_schema = attachments_df_new.withColumn("attachments", attachments_schema)
    
        attachments_df_with_schema = attachments_df_with_schema.drop('file_description', 'file_size', 'external_url')
    
        attachments_df_with_schema = (attachments_df_with_schema.groupby('posting_id').agg(F.collect_list("attachments").alias('attachments')))
        print('processed on df_attachments')
    #        attachments_df_with_schema.show()
    
    
        
        
    
    #            df_awardDetails
        print('processing on df_awardDetails')
        award_details_df_new_distinct_temp = award_details_df_new_distinct.select('bidder_name','winner_group_name','grossawardvalueeuro','netawardvalueeuro','grossawardvaluelc','netawardvaluelc','award_quantity','notes','posting_id','lot_id')
    
        award_details_df_new_distinct_schema = F.struct(
                F.col("bidder_name").alias("awardWinner"),
                F.col("winner_group_name").alias("awardWinnerGroupName"),
                F.col("grossawardvalueeuro").alias("grossBudgetEuro"),
                F.col("netawardvalueeuro").alias("netBudgetEuro"),
                F.col("grossawardvaluelc").alias("grossBudgetLC"),
                F.col("netawardvaluelc").alias("netBudgetLC"),
                F.col("award_quantity").alias("quantity"),
                F.col("notes").alias("notes")
    
        ).alias("awardDetails")
    
        award_details_df_new_distinct_temp_schema = award_details_df_new_distinct_temp.withColumn("awardDetails", award_details_df_new_distinct_schema)
    
        award_details_df_new_distinct_temp_schema = award_details_df_new_distinct_temp_schema.drop('bidder_name','winner_group_name','grossawardvalueeuro','netawardvalueeuro','grossawardvaluelc','netawardvaluelc','award_quantity','notes')
    
        award_details_df_new_distinct_temp_schema = (
                award_details_df_new_distinct_temp_schema
                .groupby('lot_id')
                .agg(
                    F.collect_list("awardDetails").alias('awardDetails'),
                    F.first("posting_id").alias("posting_id")  # Include posting_id in the result
                )
            )
        
        print('processed on df_awardDetails')
        # print('award_details_df_new_distinct_temp_schema:',award_details_df_new_distinct_temp_schema.printSchema())
        
    #             df_lots.awardCriteria
        print('processing on df_lots.awardCriteria')
        
        lot_criteria_df_new_distinct_temp = lot_criteria_df_new_distinct.select('lot_criteria_title','lot_criteria_weight','lot_is_price_related','posting_id','lot_id')
    
        lot_criteria_criteria_schema = F.struct(
                F.col("lot_criteria_title").alias("title"),
                F.col("lot_criteria_weight").alias("weight"),
                F.col("lot_is_price_related").alias("isPriceRelated")
    
        ).alias("awardCriteria")
    
        lot_criteria_df_new_distinct_temp_schema = lot_criteria_df_new_distinct_temp.withColumn("awardCriteria", lot_criteria_criteria_schema)
    
        lot_criteria_df_new_distinct_temp_schema = lot_criteria_df_new_distinct_temp_schema.drop('lot_criteria_title','lot_criteria_weight','lot_is_price_related')
    
        lot_criteria_df_new_distinct_temp_schema = (
                lot_criteria_df_new_distinct_temp_schema
                .groupby('lot_id')
                .agg(
                    F.collect_list("awardCriteria").alias('awardCriteria'),
                    F.first("posting_id").alias("posting_id")  # Include posting_id in the result
                )
            )
        
        print('processed on df_lots.awardCriteria')
        # print('lot_criteria_df_new_distinct_temp_schema:',lot_criteria_df_new_distinct_temp_schema.printSchema())
    
        join_lot_detail_and_lot_criteria_and_award_details = (
                lot_details_df_new
                .join(
                    lot_criteria_df_new_distinct_temp_schema.select(
                        col("lot_id"),
                        col("posting_id").alias("criteria_posting_id"),  # Rename to distinguish from other 'posting_id'
                        col("awardCriteria")
                    ),
                    ['lot_id'],
                    'left'
                )
                .join(
                    award_details_df_new_distinct_temp_schema.select(
                        col("lot_id"),
                        col("posting_id").alias("award_posting_id"),  # Rename to distinguish from other 'posting_id'
                        col("awardDetails")
                    ),
                    ['lot_id'],
                    'left'
                )
            )
                                            
        # print('join_lot_detail_and_lot_criteria_and_award_details:',join_lot_detail_and_lot_criteria_and_award_details.printSchema())
    
        print('join all df which contains lot_id on posting_id')
    
    
        print('creating schema for lots')
    
        lots_schema = F.struct(
                F.col("lot_actual_number").alias("actualNumber"),
                F.col("awardCriteria").alias("awardCriteria"),
                F.col("awardDetails").alias("awardDetails"),
                F.col("lot_cancellation_date").cast("string").alias("cancellationDate"),
                F.col("lot_detail_contract_duration").alias("contractDuration"),
                F.col("contract_end_date").cast("string").alias("contractEndDate"),
                F.col("contract_number").alias("contractNumber"),
                F.col("contract_start_date").cast("string").alias("contractStartDate"),
                F.col("contract_type").alias("contractType"),
                F.col("lot_description").alias("description"),
                F.col("lot_grossbudget").alias("grossBudgetEuro"),
                F.col("lot_grossbudget_lc").alias("grossBudgetLC"),
                F.col("lot_is_canceled").alias("isCanceled"),
                F.col("lot_award_date").cast("string").alias("lotAwardDate"),
                F.col("lot_cpv_at_source").alias("lotCPVCodes"),
                F.col("lot_nuts").alias("lotNuts"),
                F.col("lot_max_quantity").alias("maxQuantity"),
                F.col("lot_min_quantity").alias("minQuantity"),
                F.col("lot_netbudget").alias("netBudgetEuro"),
                F.col("lot_netbudget_lc").alias("netBudgetLC"),
                F.col("lot_number").alias("number"),
                F.col("lot_quantity").alias("quantity"),
                F.col("lot_quantity_uom").alias("quantityUOM"),
                F.col("lot_title").alias("title"),
                F.col("lot_vat").alias("vat")
    
        ).alias("lots")
    
        lot_detail_and_lot_criteria_and_award_details_with_schema = join_lot_detail_and_lot_criteria_and_award_details.withColumn("lots", lots_schema)
    
        lot_detail_and_lot_criteria_and_award_details_with_schema = lot_detail_and_lot_criteria_and_award_details_with_schema.drop('lot_number','lot_title','lot_description','lot_grossbudget','lot_netbudget',
                    'lot_grossbudget_lc','lot_netbudget_lc','lot_vat','lot_quantity','lot_min_quantity',
                    'lot_max_quantity','lot_quantity_uom','contract_number','contract_duration','contract_start_date','contract_end_date',
                    'lot_nuts','lot_is_canceled','lot_cancellation_date','lot_award_date','contract_type','lot_actual_number','lot_cpv_mapping_tenderCPVCodes','lot_cpv_at_source')
    
    
    
    
        lot_detail_and_lot_criteria_and_award_details = (lot_detail_and_lot_criteria_and_award_details_with_schema.groupby('posting_id').agg(F.collect_list("lots").alias('lots')))
        print('created schema for lots')
    
        print('joining all dataframe to create single dataframe')
        result_df_table = new_df_tender.join(performance_country_df_1, ["posting_id"], "left")\
              .join(customer_df_with_schema, ["posting_id"], "left")\
             .join(custom_tag_df_with_schema, ["posting_id"], "left")\
             .join(tender_criteria_df_with_schema, ["posting_id"], "left")\
             .join(lot_detail_and_lot_criteria_and_award_details, ["posting_id"], "left")\
             .join(attachments_df_with_schema, ["posting_id"], "left")\
             .join(customer_details_df_new_org_country,['posting_id'],'left')
    
        print('joined all dataframe and created single dataframe')
    
    
        print('applying required names to column of dataframe')
        df_with_mapping_name =  result_df_table.selectExpr(
            'additional_source_id AS additionalSourceId',
            'additional_source_name AS additionalSourceName',
            'additional_tender_url AS additionalTenderURL',
            'attachments AS attachments',
            'awardCriteria AS awardCriteria',
            'cast(tender_cancellation_date as string) AS cancellationDate',
            'contract_duration AS contractDuration',
            'cast(tender_contract_end_date as string) AS contractEndDate',
            'tender_contract_number AS contractNumber',
            'cast(tender_contract_start_date as string) AS contractStartDate',
            'contract_type_actual AS contractType',
            'org_country AS country',
            'cast(crawled_at as string) AS crawledAt',
            'currency AS currency',
            'customTags AS customTags',
            'customer AS customer',
            'notice_summary_english AS description',
            'cast(dispatch_date as string) AS dispatchDate',
            'notice_type AS documentType',
            'document_type_description AS documentTypeDescription',
            'grossbudgeteuro AS grossBudgetEuro',
            'grossbudgetlc AS grossBudgetLC',
            'notice_text AS htmlBody',
            'cast(posting_id as string) AS identifier',
            'tender_is_canceled AS isCanceled',
            'main_language AS language',
            'local_description AS localDescription',
            'local_title AS localTitle',
            'cast(tender_award_date as string) AS lotAwardDate',
            'lots AS lots',
            'tender_max_quantity AS maxQuantity',
            'tender_min_quantity AS minQuantity',
            'netbudgeteuro AS netBudgetEuro',
            'netbudgetlc AS netBudgetLC',
            'cast(publish_date as string) AS publicationDate',
            'tender_quantity AS quantity',
            'tender_quantity_uom AS quantityUOM',
            'related_tender_id AS relatedTenderId',
            'script_name AS sourceName',
            'cast(notice_deadline as string) AS submissionDeadline',
            'cast(cast(cpv_at_source as integer) as string) AS tenderCPVCodes',
            'notice_no AS tenderId',
            'notice_url AS tenderURL',
            'notice_title AS title',
            'type_of_procedure AS typeOfProcedure',
            'type_of_procedure_actual AS typeOfProcedureActual',
            'vat AS vat',
            'is_lot_default AS is_lot_default'
    
        )
    
        # df_with_mapping_name.printSchema()
    
    
        print('applying condition on dataframe to convert null values to blank array')
        final_result_df_table = df_with_mapping_name \
            .withColumn("awardCriteria",when(col("awardCriteria").isNull(), array()).otherwise(col("awardCriteria"))) \
            .withColumn("customer",when(col("customer").isNull(), array()).otherwise(col("customer"))) \
            .withColumn("customTags",when(col("customTags").isNull(), array()).otherwise(col("customTags"))) \
            .withColumn("attachments",when(col("attachments").isNull(), array()).otherwise(col("attachments"))) \
            .withColumn("lots", when((col("sourceName") == 'br_conlicitacao%') | (col("is_lot_default") == True), None).otherwise(col("lots"))) \
            .withColumn(
                    "quantity",
                    when(
                        col("quantity").rlike("\d+"),
                        regexp_replace(col("quantity"), "\D", "").cast("integer")
                    ).otherwise(None)
                )
            
        end_time_create_df = time.time()
        print('end_time_create_df: ',end_time_create_df)
        print('total time taken to create final df: ',(end_time_create_df-start_time_create_df) * 10**3, "ms")
    
        print('count final_result_df_table:',final_result_df_table.count())
        
        distinct_script_names_df = final_result_df_table.select("sourceName").distinct()
        distinct_script_names_list = distinct_script_names_df.collect()
        print('distinct_script_names_list',distinct_script_names_list)
        print(len(distinct_script_names_list))
        
        def serialize_datetime(obj): 
            if isinstance(obj, datetime.datetime): 
                return obj.isoformat() 
            raise TypeError("Type not serializable") 
        
        for name in distinct_script_names_list:
            print('script_name',name['sourceName'])
    
            filtered_script_name_df = final_result_df_table.filter(final_result_df_table.sourceName == name['sourceName'])
    
            
            columns_to_remove = ["quantity","minQuantity","maxQuantity","quantityUOM","contractNumber","contractDuration","contractStartDate","contractEndDate","isCanceled","cancellationDate","lotAwardDate","contractType","is_lot_default"] 
    
            
            filtered_df_lots_NonNull = filtered_script_name_df.filter(col("lots").isNotNull()).drop(*columns_to_remove)
            filtered_df_lots_Null = filtered_script_name_df.filter(col("lots").isNull()).drop('is_lot_default')
            
            
            if filtered_df_lots_NonNull.isEmpty():
                result_json_NonNull = []
            else:
                result_json_NonNull = [row.asDict(recursive=True) for row in filtered_df_lots_NonNull.collect()]
    
            if filtered_df_lots_Null.isEmpty():
                result_json_Null = []
            else:
                result_json_Null = [row.asDict(recursive=True) for row in filtered_df_lots_Null.collect()]
    
    
            print('converting dataframe into json format')
            result_combined_json_Null_NonNull = result_json_NonNull + result_json_Null
            
            # Handling the null value for awardDetails and awardCriteria from lots
            
    
            for obj in result_combined_json_Null_NonNull:
                lots = obj.get('lots')
                
                if lots is None:
                    lots
                else:
                    for lot in obj['lots']:
                        if lot.get('awardCriteria') is None:
                            lot['awardCriteria'] = []
                        if lot.get('awardDetails') is None:
                            lot['awardDetails'] = [] 
        
            print('applied null array condition to result_combined_json_Null_NonNull')
            
    
    
    
            # today_date = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d-%H-%M-%S')
            # today_formatted = datetime.strptime(today_date, '%Y-%m-%d-%H-%M-%S')
            
            n = 500
            chunks_df_result_combined_json_Null_NonNull = [result_combined_json_Null_NonNull[i:i+n] for i in range(0,len(result_combined_json_Null_NonNull),n)]
            print('len of chunks_df_result_combined_json_Null_NonNull list:',len(chunks_df_result_combined_json_Null_NonNull))
            for subdataframe in chunks_df_result_combined_json_Null_NonNull:
                
                
                # Generate datetime string
                today_formatted = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d-%H-%M-%S')
                print(str(today_formatted))
                file_name = name['sourceName']+"_"+str(today_formatted)+".json"
                print(file_name)
                print('pushing json result into the s3 bucket')
                Bucket=bucket_name
                Key=f'{output_json_path_key}/{file_name}'
            
                print(Key)
                object = s3_resourse.Object(Bucket, Key)
        #        object.put(Body=(bytes(json.dumps(result_combined_json_Null_NonNull).encode('UTF-8'),default=serialize_datetime)))
                object.put(Body=(bytes(json.dumps(subdataframe).encode('UTF-8'))))
                
                print('final json output pushed to s3 bucket')
                
                if len(chunks_df_result_combined_json_Null_NonNull) > 1:
                    time.sleep(1)
        
        
                
        
        #        final_result_df_table.printSchema()


job.commit()
