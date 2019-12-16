import json
import logging
from pprint import pprint
from time import sleep

import requests
from bs4 import BeautifulSoup
from elasticsearch import helpers
from elasticsearch import Elasticsearch


def search(es_object, index_name, search):
    """
        This method will display results with respect to queries.
    """
    res = es_object.search(index=index_name, body=search)
    pprint(res)


def create_index(es_object, index_name):
    """
       In this method we passed a config variable that contains the mapping
       of entire document structure.
    """
    created = False
    """ index settings """
    settings = {
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        },
        "mappings": {
            "physicians": {
                "dynamic": "strict",
                "properties": {
                    "overview": {
                        "type": "text"
                    },
                    "full_name": {
                        "type": "text"
                    },
                    "years_of_practice": {
                        "type": "text"
                    },
                    "language": {
                        "type": "text"
                    },
                    "office_location": {
                        "type": "text"
                    },
                    "hospital_affiliation": {
                        "type": "text"
                    },
                    "specialties": {
                        "type": "text"
                    },
                    "education_and_medical_training": {
                        "type": "text"
                    },
                    "certification_and_licensure": {
                        "type": "text"
                    },
                }
            }
        }
    }

    try:
        if not es_object.indices.exists(index_name):
            # Ignore 400 means to ignore "Index Already Exist" error.
            es_object.indices.create(index=index_name, ignore=400, body=settings)
            print('Created Index')
        created = True
    except Exception as ex:
        print(str(ex))
    finally:
        return created


def store_record(elastic_object, index_name, record):
    """
        This method is use to storing the actual data or document
    """
    is_stored = True
    try:
        outcome = elastic_object.index(index=index_name, doc_type='physicians', body=record)
        print(outcome)
    except Exception as ex:
        print('Error in indexing data')
        print(str(ex))
        is_stored = False
    finally:
        return is_stored


def connect_elasticsearch():
    """
        This method is use to connect the ElasticSearch server
    """
    _es = None
    # create an instance of elasticsearch and assign it to port 9200
    _es = Elasticsearch([{'host': 'localhost', 'port': 9200}]) 
    _es.cluster.health(wait_for_status='yellow', request_timeout=1)

    # pings the server and returns True if gets connected.
    if _es.ping():  
        print('Connected')
    else:
        print('It could not connect!')
    return _es


def parse(u):
    """
        This method is use to pull the data.
        Since we need data in JSON format, therefore, It convert the data accordingly.
    """
    rec = {}

    try:
        r = requests.get(u, headers=headers)

        if r.status_code == 200:
            html = r.text
            soup = BeautifulSoup(html, 'lxml')
            overview_section = soup.select('.Raw-s14xcvr1-0 gXqFYO')
            full_name_section = soup.select('.sc-iwsKbI kjxnCg')
            years_of_practice_section = soup.select('.DataField__Data-c3wc7f-1 gLHSHx')
            language_section = soup.select('.DataField__Data-c3wc7f-1 gLHSHx')
            office_location_section = soup.select('.Paragraph-fqygwe-0 cojhks')
            hospital_affiliation_section = soup.select('.Paragraph-fqygwe-0 fwayNy')
            specialties = soup.select('.DataField__Data-c3wc7f-1 gLHSHx')
            education_and_medical_training_section = soup.select('.EducationAndExperience__Item-xn5fll-0 bzYYRk')
            certification_and_licensure_section = soup.select('.Paragraph-fqygwe-0 bQPwuv')

            if overview_section:
                overview = overview_section[0].text.replace('"', '')
            if full_name_section:
                full_name = full_name_section[0].text
            if years_of_practice_section:
                years_of_practice = years_of_practice_section[0].text.strip().replace('"', '')
            if language_section:
                language = language_section[0].text.strip().replace('"', '')
            if office_location_section:
                office_location = office_location_section[0].text
            if hospital_affiliation_section:
                hospital_affiliation = hospital_affiliation_section[0].text.strip().replace('"', '')
            if specialties_section:
                specialties = specialties_section[0].text.replace('"', '')
            if education_and_medical_training_section:
                education_and_medical_training = education_and_medical_training_section[0].text
            if certification_and_licensure_section:
                certification_and_licensure = certification_and_licensure_section[0].text


            rec = {'overview': overview, 'full_name': full_name, 'years_of_practice': years_of_practice, 'language': language,
                   'office_location': office_location, 'hospital_affiliation': hospital_affiliation, 'specialties':specialties,
                    'education_and_medical_training': education_and_medical_training,
                   'certification_and_licensure': certification_and_licensure}
    except Exception as ex:
        print('Exception while parsing')
        print(str(ex))
    finally:
        return json.dumps(rec)


if __name__ == '__main__':
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',
        'Pragma': 'no-cache'
    }
    logging.basicConfig(level=logging.ERROR)

    url = 'https://health.usnews.com/doctors/city-index/new-jersey'
    r = requests.get(url, headers=headers)

    if r.status_code == 200:
        html = r.text
        soup = BeautifulSoup(html, 'lxml')
        links = soup.select('.List__ListWrap-e439ne-0 hobCNJ .List__ListItem-e439ne-1 hgSqfk a .List__ListItem-e439ne-1 hgSqfk .s85n6m5-0-Box-cwadsP fVAhQS a')
        if len(links) > 0:
            es = connect_elasticsearch()

        for link in links:
            sleep(2)
            result = parse(link['href'])
            if es is not None:
                if create_index(es, 'physicians'):
                    out = store_record(es, 'physicians', result)
                    print('Data indexed successfully')

    es = connect_elasticsearch()
    if es is not None:
        search_object = {'_source': ['full_name'], "query": { "aggs": {"doctors":{"terms":{"field":"full_name"}}}}}
        search(es, 'physicians', json.dumps(search_object))