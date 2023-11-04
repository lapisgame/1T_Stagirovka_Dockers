from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import re
import os
import time
import csv
import random

import requests
from bs4 import BeautifulSoup
import lxml

from dotenv import dotenv_values
from fake_useragent import FakeUserAgent
from datetime import date, timedelta

import pandas as pd

access_token = ''

def fetch():
    global access_token
    access_token = fetch_token()
    
def get_new_token():
    params = {
        'grant_type': 'client_credentials',
        'client_id': 'I5D4FF6USNJSGHOHOECGGDDEOVU3554D2HSP06ERC7GVEC73E8SLMHA0SR80CJN3',
        'client_secret': 'IF7VIJCASG4NS78OG1U8JBTLQMQRG6EPIRRB28FBNGO3KSH0H1T96SSSTAMISVEI'
    }
    
    response = requests.post('https://hh.ru/oauth/token', params=params)
    if response.status_code == 200:
        access_token = response.json()['access_token']
        print('Токен приложения успешно получен:', access_token)
    else:
        print('Произошла ошибка при получении токена приложения')
        print(response.text)    

def check_token_validity(access_token):
    headers = {
        'Authorization': f'Bearer {access_token}'
    }

    response = requests.get('https://api.hh.ru/me', headers=headers)
    if response.status_code == 200:
        print('Токен приложения действителен')
        return True
    else:
        print('Токен приложения недействителен')
        return False

def revoke_access_token(access_token):
    params = {
        'token': access_token
    }

    response = requests.post('https://hh.ru/oauth/revoke', params=params)
    if response.status_code == 200:
        print('Токен доступа успешно отозван')
        return True
    else:
        print('Произошла ошибка при отзыве токена доступа')
        print(response)
        return False

def fetch_token():
    access_token = ''
    test_access_token = 'APPLRO5K4638656L3VPEES0IS0L242C1I065V7TQ8NNGAOS7ASC8ANEP5G8EPV14'  # Замените на ваш токен доступа
    if not check_token_validity(test_access_token):
        if not revoke_access_token(test_access_token):
            access_token = get_new_token()
    else:
        access_token = test_access_token
    
    return access_token

# Основные регулярные выражения для проекта
re_vacancy_id_hh = r'\/vacancy\/(\d+)\?'
re_vacancy_id_rabota = r'\/vacancy\/(\d+)'
re_vacancy_id_finder = r'\/vacancies\/(\d+)'
re_vacancy_id_zarplata = r'\/vacancy\/card\/id(\d+)'
# re.search(re_vacancy_id, string).group(1)

re_html_tag_remove = r'<[^>]+>'
# re.sub(re_html_tag_remove, replace, string)

class Rabota1000_Parser:
    global re_vacancy_id_hh
    global re_vacancy_id_rabota
    global re_vacancy_id_finder
    global re_vacancy_id_zarplat
    global re_html_tag_remove
    global access_token
    # Класс для парсинга вакансий с ресурса Rabota1000.ru
    def proxys(self):
        url = 'https://free-proxy-list.net/'
        response = requests.get(url)
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table')
        rows = table.tbody.find_all('tr')

        proxies = []
        for row in rows:
            cols = row.find_all('td')
            if cols[6].text == 'yes':
                proxy = cols[0].text + ':' + cols[1].text
                proxies.append(proxy)
            
        print(proxy)
        return proxies

    def __init__(self, city:str='russia'):
        self.pre_resualt = []
        self.max_page_count = 5
        self.basic_url = f'https://rabota1000.ru/{city}/'
        self.vac_name_list = []
        self.vac_name_list = [
            'data+scientist', 'data+science', 'дата+сайентист',
            'младший+дата+сайентист', 'стажер+дата+сайентист',
            'machine+learning', 'ml', 'ml+engineer',
            'инженер+машинного+обучения', 'data+engineering',
            'инженер+данных', 'младший+инженер+данных',
            'junior+data+analyst', 'junior+data+scientist',
            'junior+data+engineer', 'data+analyst',
            'data+analytics','аналитик+данных', 'big+data+junior'
        ]
        self.proxy = self.proxys()
        
        ua = FakeUserAgent()
        self.headers = {'user-agent':ua.random}
        print('INIT OK')

    def pars(self):
        print('PARS 1')

        with open('pars_link.csv', 'w', newline='', encoding='utf-8') as csv_file:
            print('OPEN pars_link.csv')
            names = ['vac_name', 'link', 'source', 'vac_id']
            file_writer = csv.DictWriter(csv_file, delimiter = ",", lineterminator="\r", fieldnames=names)
            file_writer.writeheader()

        print('CLOSE pars_link.csv')
        for vac_name in self.vac_name_list:
            print(vac_name)
            try:
                used_url = self.basic_url + vac_name + "/"
                response = requests.get(used_url, headers=self.headers, proxies={'https': random.choice(self.proxy)})
                response.raise_for_status()
                for i in range(1, self.max_page_count+1):
                    print(i, end=' ')
                    used_url = f'{self.basic_url}{vac_name}?p={i}'
                    page = requests.get(used_url, headers=self.headers, proxies={'https': random.choice(self.proxy)})
                    soup = BeautifulSoup(page.text, 'html.parser')

                    # 20 ссылок на одной странице
                    links = [requests.get(link['href']).url for link in soup.findAll('a', attrs={'@click':'vacancyLinkClickHandler'})]
                    sources = [source.text for source in soup.findAll('span', attrs={'class':'text-sky-600'})]

                    links_to_save = [[vac_name, link, source] for link, source in zip(links, sources)]

                    with open('pars_link.csv', 'a', newline='', encoding='utf-8') as csv_file:
                        writer = csv.writer(csv_file)
                        writer.writerows(links_to_save)
                        
            except Exception as e:
                print(e)
            print()


        print('PARS 2')
        if not os.path.exists('finaly.csv'):
            links_for_processing = []
            with open('pars_link.csv', 'r', encoding='utf-8') as csv_file:
                print('OPEN pars_link.csv r')
                reader = csv.reader(csv_file)
                labels = next(reader, None)
                for row in reader:
                    links_for_processing.append(dict(zip(labels, row)))

            for item in links_for_processing:
                if item['source'] == 'hh.ru':
                    item['vac_id'] = re.search(re_vacancy_id_hh, item['link']).group(1)
                elif item['source'] == 'finder.vc':
                    item['vac_id'] = re.search(re_vacancy_id_finder, item['link']).group(1)
                elif item['source'] == 'zarplata.ru':
                    item['vac_id'] = re.search(re_vacancy_id_zarplata, item['link']).group(1)
                else:
                    item['vac_id'] = re.search(re_vacancy_id_rabota, item['link']).group(1)

            
            for link in links_for_processing:
                if link['source'] == 'hh.ru':
                    self.pre_resualt.append(self._pars_url_hh(link['vac_id']))
                elif link['source'] == 'zarplata.ru':
                    self.pre_resualt.append(self._pars_url_zarplata(link['vac_id']))
                elif link['source'] == 'finder.vc':
                    self.pre_resualt.append(self._pars_url_finder(link['vac_id']))
                else:
                    self.pre_resualt.append(self._pars_url_other(link['vac_id']))
            
            self._save_frame_to_csv()
        else:
            print('PARS r')
            with open('finaly.csv', 'r', encoding='utf-8') as csv_file:
                reader = csv.reader(csv_file)
                labels = next(reader, None)
                for row in reader:
                    self.pre_resualt.append(dict(zip(labels, row)))


    def get_vac_name_into_file(self, vac_file_path:str)->list:
        vac_name_list = []
        with open(vac_file_path, mode='r', encoding="utf-8") as file_vac:
            vac_name_list = list(map(lambda x: x.lower().replace('\n', '').replace(' ','+'), file_vac.readlines()))

        return vac_name_list

    def _pars_url_hh(self, id:str)->dict:
        res = {}
        try:
            data = requests.get(f'https://api.hh.ru/vacancies/{id}', headers = {'Authorization': f'Bearer {access_token}'}).json()
            res['vac_link'] = f'https://hh.ru/vacancy/{id}'                             # Ссылка
            res['name'] = data['name']                                                  # Название
            res['city'] = data['area']['name']                                          # Город
            res['company'] = data['employer']['name']                                   # Назвнание компании публикующей вакансию
            res['experience'] = data['experience']['name']                              # Опыт работы (нет замены на jun mid и sin)
            res['schedule'] = data['schedule']['name']                                  # Тип работы (офис/удаленка и тд)
            res['employment'] = data['employment']['name']                              # График работы
            res['skills'] = [item['name'] for item in data['key_skills']]               # Ключевые навыки
            res['description'] = re.sub(re_html_tag_remove, '', data['description'])    # Полное описание (html теги не убраны)
            if data['salary'] == None: 
                res['salary'] = 'Договорная'                                            # Если ЗП не указано то пишем договорная
            else:
                res['salary'] = data['salary']                                          # Если есть то берем как есть
            res['time'] = data['published_at']                                          # Дата и время публикации
        except Exception as e:
            print(f'Not Found {e}')
            print(f'https://api.hh.ru/vacancies/{id}')
            data = requests.get(f'https://api.hh.ru/vacancies/{id}', headers = {'Authorization': f'Bearer {access_token}'}).json()
            print(data)

        return res


    def _pars_url_zarplata(self, id:str)->dict:
        res = {}
        try:
            data = requests.get(f'https://api.zarplata.ru/vacancies/{id}').json()
            res['vac_link'] = f'https://www.zarplata.ru/vacancy/card/id{id}'            # Ссылка
            res['name'] = data['name']                                                  # Название
            res['city'] = data['area']['name']                                          # Город
            res['company'] = data['employer']['name']                                   # Назвнание компании публикующей вакансию
            res['experience'] = data['experience']['name']                              # Опыт работы (нет замены на jun mid и sin)
            res['schedule'] = data['schedule']['name']                                  # Тип работы (офис/удаленка и тд)
            res['employment'] = data['employment']['name']                              # График работы
            res['skills'] = [item['name'] for item in data['key_skills']]               # Ключевые навыки
            res['description'] = re.sub(re_html_tag_remove, '', data['description'])    # Полное описание
            if data['salary'] == None: 
                res['salary'] = 'Договорная'                                            # Если ЗП не указано то пишем договорная
            else:
                res['salary'] = data['salary']                                          # Если есть то берем как есть
            res['time'] = data['published_at']
            
        except Exception as e:
            print(f'Not Found {e}')
            print(f'https://api.zarplata.ru/vacancies/{id}')
            data = requests.get(f'https://api.zarplata.ru/vacancies/{id}').json()
            print(data)

        return res


    def _pars_url_other(self, id:str)->dict:
        res = {}
        soup = BeautifulSoup(requests.get(f'https://rabota1000.ru/vacancy/{id}').text, 'html.parser')
        dom = lxml.etree.HTML(str(soup)) 
        res['vac_link'] = f'https://rabota1000.ru/vacancy/{id}'                                                                                             # Ссылка
        res['name'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[1]/h2')[0].text.replace('\n', '').lstrip().rstrip()            # Название
        res['city'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[3]/p[2]/span')[0].text                                         # Город (НЕТ)
        res['company'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[3]/p[1]')[0].text.replace('\n', '').lstrip().rstrip()       # Назвнание компании публикующей вакансию
        res['experience'] = ''                                                                                                                              # Опыт работы (нет замены на jun mid и sin)
        res['schedule'] = ''                                                                                                                                # Тип работы (офис/удаленка и тд) (НЕТ)
        res['employment'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[3]/ul/li[2]/span')[0].text                                      # График работы
        res['skills'] = ''                                                                                                                                  # Ключевые навыки
        res['description'] = re.sub(re_html_tag_remove, '', dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[4]')[0].text)                                                   # Полное описание (НЕТ)
        if len(dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[2]/span'))>0:
            res['salary'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[2]/span')[0].text.replace('\n', '').lstrip().rstrip()        # ЗП
        else:
            res['salary'] = 'Договорная'
        res['time'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[3]/ul/li[1]/span')[0].text.replace('\n', '').lstrip().rstrip()        # Дата публикации

        return res
        
    def _pars_url_finder(self, id:str)->list:
        res = {}
        soup = BeautifulSoup(requests.get(f'https://finder.vc/vacancies/{id}').text, 'html.parser')
        dom = lxml.etree.HTML(str(soup)) 
        res['vac_link'] = f'https://finder.vc/vacancies/{id}' # Ссылка
        res['name'] = soup.find('h1', attrs={'class':'vacancy-info-header__title'}).text # Название
        res['city'] = ''              # Город (НЕТ)
        res['company'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/a')[0].text        # Назвнание компании публикующей вакансию
        res['experience'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div[3]/div[1]/div[2]/div')[0].text  # Опыт работы (нет замены на jun mid и sin)
        res['schedule'] = ''     # Тип работы (офис/удаленка и тд) (НЕТ
        res['employment'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[3]/div/div[2]/a')[0].text # График работы
        res['skills'] = [li.text for li in dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[3]/div[1]/div[2]/div[1]/ul')[0]]           # Ключевые навыки
        res['description'] = ''    # Полное описание (НЕТ)
        res['salary'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div[2]/div[2]/div')[0].text.replace(u'\xa0', '')

        if 'сегодня' in dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[1]')[0].text:
            res['time'] = str(date.today())
        elif 'вчера' in dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[1]')[0].text:
            res['time'] = str(date.today() - timedelta(days=1))
        else:
            res['time'] = str(date.today() - timedelta(days=int(re.search(r'Опубликована (\d+)', dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[1]')[0].text).group(1))))

        return res
    
    def _save_frame_to_csv(self):
        try:
            keys = self.pre_resualt[0].keys()

            with open('finaly.csv', 'w', newline='', encoding='utf-8') as output_file:
                dict_writer = csv.DictWriter(output_file, keys, delimiter = ",", lineterminator="\r")
                dict_writer.writeheader()
                dict_writer.writerows(self.pre_resualt)
        except Exception as e:
            print(e)

# Инициализация DAG
default_args = {
    'start_date': datetime(2021, 1, 1)
}

dag = DAG(
    'Sync_Pars_DAG',
    default_args=default_args,
    description='DAG для парсинга вакансий из списка',
    schedule_interval='@daily'
)

def run_parser():
    fetch()
    parser = Rabota1000_Parser()
    parser.pars()

# Определение трех задач, каждая из которых вызывает соответствующий метод пользовательского класса
run_parser_task = PythonOperator(
    task_id='run_parser_task',
    python_callable=run_parser,
    dag=dag,
)


# Определение порядка выполнения задач
run_parser_task