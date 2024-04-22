
from gec_common.gecclass import *
import logging
from gec_common import log_config
SCRIPT_NAME = "br_casadamoeda_spn"
log_config.log(SCRIPT_NAME)
import re
import jsons
import time
from datetime import date, datetime, timedelta
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC 
from deep_translator import GoogleTranslator
from selenium import webdriver
from selenium.webdriver.common.by import By
import gec_common.OutputJSON
from gec_common import functions as fn

NOTICE_DUPLICATE_COUNT = 0
MAX_NOTICES_DUPLICATE = 4
MAX_NOTICES = 2000
notice_count = 0
SCRIPT_NAME = "br_casadamoeda_spn"
output_json_file = gec_common.OutputJSON.OutputJSON(SCRIPT_NAME)
previous_scraping_log_check = fn.previous_scraping_log_check(SCRIPT_NAME)
output_json_folder = "jsonfile"
def extract_and_save_notice(tender_html_element):
    global notice_count
    global notice_data
    notice_data = tender()
   
    notice_data.script_name = 'br_casadamoeda_spn'
   
    notice_data.main_language = 'PT'
    
    notice_data.currency = 'EUR'
    
    notice_data.procurement_method = 2
   
    performance_country_data = performance_country()
    performance_country_data.performance_country = 'BR'
    notice_data.performance_country.append(performance_country_data)
    
    notice_data.notice_type = 4
    
    notice_data.notice_url = url
        
    try:
        notice_data.document_type_description = page_main.find_element(By.CSS_SELECTOR, '#geral > h1').text
    except Exception as e:
        logging.info("Exception in document_type_description: {}".format(type(e).__name__))
        pass
    
    # Onsite Field -Edital
    try:
        notice_data.notice_no = tender_html_element.find_element(By.CSS_SELECTOR, 'p:nth-child(3)').text.split('Edital:')[1].strip()
    except Exception as e:
        logging.info("Exception in notice_no: {}".format(type(e).__name__))
        pass
    
    try:
        notice_data.local_title = tender_html_element.find_element(By.CSS_SELECTOR, 'ul > li h2').text
        notice_data.notice_title = GoogleTranslator(source='auto', target='en').translate(notice_data.local_title)
    except Exception as e:
        logging.info("Exception in local_title: {}".format(type(e).__name__))
        pass

#     notice_data.publish_date = 'take as threshold'
    
    # Onsite Field -Sessão Pública
    try:
        notice_deadline = tender_html_element.find_element(By.CSS_SELECTOR, "p:nth-child(5)").text
        notice_deadline = GoogleTranslator(source='auto', target='en').translate(notice_deadline)
        notice_deadline = re.findall('\d+/\d+/\d{4} at \d+:\d+',notice_deadline)[0]
        notice_data.notice_deadline = datetime.strptime(notice_deadline,'%m/%d/%Y at %H:%M').strftime('%Y/%m/%d %H:%M:%S')
        logging.info(notice_data.notice_deadline)
    except Exception as e:
        logging.info("Exception in notice_deadline: {}".format(type(e).__name__))
        pass
    
    if notice_data.notice_deadline is None:
        return
    
    if notice_data.notice_deadline is not None and notice_data.notice_deadline < threshold:
        return
   
    try:
        notice_data.notice_text += tender_html_element.get_attribute("outerHTML")                     
    except:
        pass

    try:              
        customer_details_data = customer_details()
        customer_details_data.org_name = 'CASA DA MOEDA DO BRASIL CMB'
        customer_details_data.org_parent_id = '7699021'
        customer_details_data.org_country = 'BR'
        customer_details_data.org_language = 'PT'
        customer_details_data.customer_details_cleanup()
        notice_data.customer_details.append(customer_details_data)
    except Exception as e:
        logging.info("Exception in customer_details: {}".format(type(e).__name__)) 
        pass

    try:              
        for single_record in tender_html_element.find_elements(By.CSS_SELECTOR, '#geral  li a'):
            attachments_data = attachments()
        # Onsite Field -Arquivos Anexos:       
            try:
                attachments_data.file_type = single_record.text.split('.')[-1].strip()
            except Exception as e:
                logging.info("Exception in file_type: {}".format(type(e).__name__))
                pass
            
            attachments_data.file_name = single_record.text.replace(attachments_data.file_type,'').strip()
        
            attachments_args = single_record.get_attribute('href')
            attachment_name = attachments_args.split("'")[1].split("'")[-1]
            attached_id = attachments_args.split(attachment_name)[1]
            attachment_id = re.findall('\d+',attached_id)[0]
            attachments_data.external_url = 'https://www.casadamoeda.gov.br/licitacao/conDownloadArquivo.php?nomearquivo='+str(attachment_name)+'&arquivo='+str(attachment_id)
            
            attachments_data.attachments_cleanup()
            notice_data.attachments.append(attachments_data)
    except Exception as e:
        logging.info("Exception in attachments: {}".format(type(e).__name__)) 
        pass
    
    notice_data.identifier = str(notice_data.script_name) + str(notice_data.notice_no) +  str(notice_data.notice_url) +  str(notice_data.notice_type) + str(notice_data.publish_date) + str(notice_data.notice_deadline)
    logging.info(notice_data.identifier)
    duplicate_check_data = fn.duplicate_check_data_from_previous_scraping(SCRIPT_NAME,MAX_NOTICES_DUPLICATE,notice_data.identifier,previous_scraping_log_check)
    NOTICE_DUPLICATE_COUNT = duplicate_check_data[1]
    if duplicate_check_data[0] == False:
        return
    notice_data.tender_cleanup()
    output_json_file.writeNoticeToJSONFile(jsons.dump(notice_data))
    notice_count += 1
    logging.info('----------------------------------')
# ----------------------------------------- Main Body
arguments= ['--incognito','ignore-certificate-errors','allow-insecure-localhost','--start-maximized']
page_main = fn.init_chrome_driver(arguments)
try:
    th = date.today() - timedelta(1)
    threshold = th.strftime('%Y/%m/%d')
    logging.info("Scraping from or greater than: " + threshold)
    urls = ["https://www.casadamoeda.gov.br/portal/negocios/licitacoes/audiencia-publica-e-outros.html"] 
    for url in urls:
        fn.load_page(page_main, url, 60)
        
        
        logging.info('----------------------------------')
        logging.info(url)
        time.sleep(5)

        try:
            iframe = page_main.find_element(By.CSS_SELECTOR,'#blockrandom')
            page_main.switch_to.frame(iframe)
        except:
            pass

        try:
            rows = WebDriverWait(page_main, 60).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#geral > ul > li')))
            length = len(rows)
            for records in range(0,length):
                tender_html_element = WebDriverWait(page_main, 60).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#geral > ul > li')))[records]
                extract_and_save_notice(tender_html_element)
                if notice_count >= MAX_NOTICES:
                    break

                if notice_data.notice_deadline is not None and notice_data.notice_deadline < threshold:
                    break

                if NOTICE_DUPLICATE_COUNT >= MAX_NOTICES_DUPLICATE:
                    logging.info("NOTICES_DUPLICATE::", MAX_NOTICES_DUPLICATE)
                    break
        except:
            logging.info('No new record')
            break
            

    logging.info("Finished processing. Scraped {} notices".format(notice_count))
except Exception as e:
    raise e
    logging.info("Exception:"+str(e))
finally:
    page_main.quit()
    output_json_file.copyFinalJSONToServer(output_json_folder)
    
