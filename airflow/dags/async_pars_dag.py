from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import aiohttp
import asyncio

import re
import json
import random
import os

import requests
from bs4 import BeautifulSoup
import lxml

from dotenv import dotenv_values
from fake_useragent import FakeUserAgent
from datetime import date, timedelta

import pandas as pd
import psycopg2

class Rabota1000_parser_async:
    
    #* Функция инициализации
    def __init__(self, city='russia') -> None:
        self.max_page_count = 1
        self.basic_url = f'https://rabota1000.ru/{city}/'
        self.df = pd.DataFrame(columns=['vac_link', 'name', 
                                        'city', 'company', 'experience', 
                                        'schedule', 'employment', 
                                        'skills', 'description', 
                                        'salary', 'time'])
        self.vac_name_list = []
        self.get_vac_name_list_into_csv()

        if os.path.exists('async_pars.csv'):
            os.remove('async_pars.csv')

        # proxy_login = 'T2Q2cXJE'
        # proxy_password = '7fhVaNGg'
        # self.proxy = {'http':f'http://{proxy_login}:{proxy_password}@45.132.129.151:62898', 
        #               'https':f'socks5://{proxy_login}:{proxy_password}@45.132.129.151:62899'}

        self.main_proxy = ''

        self.re_vacancy_id_hh = r'\/vacancy\/(\d+)\?'
        self.re_vacancy_id_rabota = r'\/vacancy\/(\d+)'
        self.re_vacancy_id_finder = r'\/vacancies\/(\d+)'
        self.re_vacancy_id_zarplata = r'\/vacancy\/card\/id(\d+)'
        # re.search(re_vacancy_id, string).group(1)

        self.re_html_tag_remove = r'<[^>]+>'

        self.access_token = self.fetch_token()
        # print(self.access_token)

    def create_postgres_row_table(self):
        pass

    def insert_dataframe_to_postgres(self):
        pass

    def get_proxy_list(self):
        proxies = []

        url = 'https://free-proxy-list.net/'
        res = requests.get(url)

        soup = BeautifulSoup(res.text, 'html.parser')   
        table = soup.find('tbody')
        
        for row in table:
            tds = row.find_all('td')
            if tds[6].text =='yes' and ('secs' in tds[7].text or int(tds[7].text[0:2])<10):
                proxy = ':'.join([tds[0].text, tds[1].text])
                proxies.append(proxy)
            else:
                pass
        
        ins = ['37.19.220.129:8443','37.19.220.179:8443','37.19.220.180:8443',
               '138.199.48.1:8443','138.199.48.4:8443','34.229.218.102:3128',
               '216.80.39.89:3129','64.189.106.6:3129','159.203.120.97:10009',
               '164.92.184.84:8888','43.157.105.92:8888','35.236.207.242:33333',
               '82.102.26.38:8443','141.147.93.160:8080','176.31.129.223:8080',
               '201.182.55.205:999','96.95.164.43:3128','65.108.200.102:3128',
               '178.18.252.98:6789','154.236.189.23:1976','194.165.140.122:3128',
               '5.188.168.199:8443','93.84.70.131:3128','154.236.189.23:1981',
               '91.113.220.210:3128','67.43.228.251:18181','185.82.176.34:80',
               '41.65.103.13:1976','217.52.247.75:1981','217.52.247.79:1981',
               '78.47.137.54:3128','138.199.23.163:8443','79.135.219.223:8080',
               '5.161.200.98:3128','154.236.189.15:1981']

        for item in ins:
            proxies.append(item)

        url = 'https://freeproxyupdate.com/'
        res = requests.get(url)

        soup = BeautifulSoup(res.text, 'html.parser')   
        table = soup.find('table', attrs={'class':'list-proxy'}).find('tbody')

        for row in table:
            try:
                tds = row.find_all('td')
                if tds[3].find('a')['href'] == 'https' and tds[5].text == 'Fast':
                    proxy = ':'.join([tds[0].text, tds[1].text])
                    print(2, tds[0].text, tds[1].text)
                    proxies.append(proxy)
            except:
                continue

        return proxies

    def set_main_proxy(self):
        self.main_proxy = ''
        while self.main_proxy == '':
            proxy_list = self.get_proxy_list()
            print(proxy_list)

            for prox in proxy_list:
                url = 'https://rabota1000.ru/'
                try:
                    res = requests.get(url, proxies={'http':prox, 'https':prox}, timeout=2)
                    if res.status_code == 200:
                        print('GOOD PROXY ', prox)
                        self.main_proxy = prox
                        break
                except:
                    ...
            
    def get_new_token(self):
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

    def check_token_validity(self, access_token):
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

    def revoke_access_token(self, access_token):
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

    def fetch_token(self):
        access_token = ''
        test_access_token = 'APPLRO5K4638656L3VPEES0IS0L242C1I065V7TQ8NNGAOS7ASC8ANEP5G8EPV14'  # Замените на ваш токен доступа
        if not self.check_token_validity(test_access_token):
            if not self.revoke_access_token(test_access_token):
                access_token = self.get_new_token()
        else:
            access_token = test_access_token
        
        return access_token

    def get_vac_name_list_into_csv(self):
        with open('/opt/airflow/data_dag_pars/vac_name_list.csv', encoding='utf-8') as f:
            for line in f:
                self.vac_name_list.append(line)

    def topars(self):
        for vac_name in self.vac_name_list:
            print(vac_name)
            self.set_main_proxy()
            links = self.get_list_links_into_rabota1000(vac_name)
            pre_pars_dict = self.async_pars_url_list(links)
            print('pre_pars_dict')
            for item in pre_pars_dict:
                self.fetch_data_into_url(item)

            print()
            # self.df = self.df.drop_duplicates()
            self.df.to_csv('async_pars.csv', index=False, sep=';')
            os.system('cp -f /opt/airflow/async_pars.csv /opt/airflow/async_pars_copy.csv')

    #* Достает id вакансии и название сайта для дальнейшей обработки 
    def get_vac_id_into_url(self, url:str):
        if 'hh' in url:
            return {'source': 'hh.ru', 'vac_id':re.search(self.re_vacancy_id_hh, url).group(1)}
        elif 'finder' in url:
            return {'source': 'finder.vc', 'vac_id':re.search(self.re_vacancy_id_finder, url).group(1)}
        elif 'zarplata' in url:
            return {'source': 'zarplata.ru', 'vac_id':re.search(self.re_vacancy_id_zarplata, url).group(1)}
        elif 'rabota' in url:
            return {'source': 'rabota.ru', 'vac_id':re.search(self.re_vacancy_id_rabota, url).group(1)}
        else:
            return {'source':'', 'vac_id':''}
    
    #& Вспомогательная функция для объединения списков
    def list_simple_merge(self, list1:list, list2:list)->list:
        i, j = 0, 0
        res = []
        while i < len(list1) and j < len(list2):
            res.append(list1[i])
            i += 1
            res.append(list2[j])
            j += 1
        res += list1[i:]
        res += list2[j:] 
        return res

    #* Асинхронная функция запросов на редирект
    #* принимает aiohttp.ClientSession(), ссылку с rabota1000, FakeUserAgent().random
    #* возвращает в результате dict[source, vac_id]
    async def fetch_vacancy_redirect_url(self, rabota_url:str, temp_list:list)->dict:
        try:
            print('Try 1  fetch_vacancy_redirect_url')
            ua = FakeUserAgent()
            url = f'https://rabota1000.ru/vacancy/source?id={rabota_url[rabota_url.rfind("/")+1::]}'
            
            #! PROXY
            headers = {'User-Agent':ua.random, 'Authorization':f'Bearer {self.access_token}'}
            res = requests.get(url=url, headers=headers, proxies={'http':self.main_proxy, 'https':self.main_proxy}, timeout=5)
            url = res.url

            ret = self.get_vac_id_into_url(str(url))
            temp_list.append(ret)
            return ret

        except Exception as e:
            print('change Proxy  230')
            self.set_main_proxy()
            try:
                print('Try 2  fetch_vacancy_redirect_url')
                ua = FakeUserAgent()
                url = rabota_url

                #!PROXY
                headers = {'User-Agent':ua.random, 'Authorization':f'Bearer {self.access_token}'}
                res = requests.get(url=url, headers=headers, proxies={'http':self.main_proxy, 'https':self.main_proxy}, timeout=10)
                url = res.url
                
                ret = self.get_vac_id_into_url(str(url))
                temp_list.append(ret)
                return ret

            except Exception as ee:
                print('AAAAAAAAAA')
                print('change Proxy  248')
                self.set_main_proxy()
                print('EEE fetch_vacancy_redirect_url   ', e)
            finally:    
                return {'source':'', 'vac_id':''}
            
        finally:    
            return {'source':'', 'vac_id':''}

    #* Асинхронная функция принимает список ссылок, возвращает список из dict[source, vac_id]
    async def async_pars_url_list_main(self, links, temp_list):
        tasks = []
        
        for vacancy_id in links:
            task = asyncio.create_task(self.fetch_vacancy_redirect_url(rabota_url=str(vacancy_id), temp_list=temp_list))
            tasks.append(task)
        
        return [await asyncio.gather(*tasks)]

    #* Запускает tasks с задачей fetch_vacancy_redirect_url, 
    #* разделяя полный список ссылок на кластеры по step штук
    #* Возвращает единый список из dict[source, vac_id]
    def async_pars_url_list(self, links:list):
        res = []
        temp_list = []
        step = 20
        for i in range(0, len(links), step):
            res += asyncio.run(self.async_pars_url_list_main(links=links[i:i+step], temp_list=temp_list))

        # merge = []
        # for item in res:
        #     merge = self.list_simple_merge(merge, item)

        return temp_list

    #* Парсит все ссылки на вакансии без редиректа по названию вакансий, на max_page_count страниц выдачи
    def get_list_links_into_rabota1000(self, vac_name:str):
        res = []
        for page_num in range(1, self.max_page_count+1):
            print(page_num)
            used_url = f'{self.basic_url}{vac_name}?p={page_num}'

            #! PROXY
            try:
                ua = FakeUserAgent()
                headers = {'User-Agent':ua.random, 'Authorization':f'Bearer {self.access_token}'}
                page = requests.get(used_url, proxies={'http':self.main_proxy, 'https':self.main_proxy}, headers=headers, timeout=5)
                if int(page.status_code) == 200:
                    soup = BeautifulSoup(page.text, 'html.parser')

                    links = [link['href'] for link in soup.findAll('a', attrs={'@click':'vacancyLinkClickHandler'})]

                    res = self.list_simple_merge(res, links)
                else:
                    print(page.status_code)

                return res
            except:
                print('EEE get_list_links_into_rabota1000')
                print('change Proxy  307')
                self.set_main_proxy()
                return res

    #* Получаем все данные из dict[source, vac_id] и записываем их в датафрейм
    def fetch_data_into_url(self, link_dict):
        if link_dict['source'] != '':
            if link_dict['source'] == 'hh.ru':
                self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(self._pars_url_hh(link_dict['vac_id'])))], ignore_index=True)
            elif link_dict['source'] == 'zarplata.ru':
                self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(self._pars_url_zarplata(link_dict['vac_id'])))], ignore_index=True)
            elif link_dict['source'] == 'finder.vc':
                self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(self._pars_url_finder(link_dict['vac_id'])))], ignore_index=True)
            else:
                self.df = pd.concat([self.df, pd.DataFrame(pd.json_normalize(self._pars_url_other(link_dict['vac_id'])))], ignore_index=True)
                
    #& Парсинг HH.RU            (use API)    
    def _pars_url_hh(self, id:str)->dict:
        res = {}
        try:
            data = requests.get(f'https://api.hh.ru/vacancies/{id}', headers = {'Authorization': f'Bearer {self.access_token}'}).json()
            if data['description'] != 'Not Found':
                res['vac_link'] = f'https://hh.ru/vacancy/{id}'                             # Ссылка
                res['name'] = data['name']                                                  # Название
                res['city'] = data['area']['name']                                          # Город
                res['company'] = data['employer']['name']                                   # Назвнание компании публикующей вакансию
                res['experience'] = data['experience']['name']                              # Опыт работы (нет замены на jun mid и sin)
                res['schedule'] = data['schedule']['name']                                  # Тип работы (офис/удаленка и тд)
                res['employment'] = data['employment']['name']                              # График работы
                res['skills'] = '  '.join([item['name'] for item in data['key_skills']])    # Ключевые навыки
                res['description'] = re.sub(self.re_html_tag_remove, '', data['description']).replace(';', '')    # Полное описание (html теги не убраны)
                if data['salary'] == None: 
                    res['salary'] = 'Договорная'                                            # Если ЗП не указано то пишем договорная
                else:
                    if data['salary']['from'] != None:                                      # Если есть то берем как есть
                        res['salary.from'] = data['salary']['from']
                    else:
                        res['salary.from'] = '0'   

                    if data['salary']['to'] != None:                                      # Если есть то берем как есть
                        res['salary.to'] = data['salary']['to']
                    else:
                        res['salary.to'] = '0'   

                    if data['salary']['currency'] != None:
                        res['salary.currency'] = data['salary']['currency']
                    else:
                        res['salary.currency'] = 'Тургрики'

                res['time'] = data['published_at']                                          # Дата и время публикации
                return res
            
        except Exception as e:
            print(f'Not Found {e}')
            print(f'EEE  _pars_url_hh   https://api.hh.ru/vacancies/{id}')
            data = requests.get(f'https://api.hh.ru/vacancies/{id}', headers = {'Authorization': f'Bearer {self.access_token}'}).json()

        finally:
            return res

    #& Парсинг ZARPLATA.RU      (use API)
    def _pars_url_zarplata(self, id:str)->dict:
        res = {}
        try:
            ua = FakeUserAgent()
            data = requests.get(f'https://api.zarplata.ru/vacancies/{id}', headers={'User-Agent': ua.random}).json()
            res['vac_link'] = f'https://www.zarplata.ru/vacancy/card/id{id}'            # Ссылка
            res['name'] = data['name']                                                  # Название
            res['city'] = data['area']['name']                                          # Город
            res['company'] = data['employer']['name']                                   # Назвнание компании публикующей вакансию
            res['experience'] = data['experience']['name']                              # Опыт работы (нет замены на jun mid и sin)
            res['schedule'] = data['schedule']['name']                                  # Тип работы (офис/удаленка и тд)
            res['employment'] = data['employment']['name']                              # График работы
            res['skills'] = '  '.join([item['name'] for item in data['key_skills']])    # Ключевые навыки
            res['description'] = re.sub(self.re_html_tag_remove, '', data['description']).replace(';', '')    # Полное описание
            if data['salary'] == None: 
                res['salary'] = 'Договорная'                                            # Если ЗП не указано то пишем договорная
            else:
                if data['salary']['from'] != None:                                      # Если есть то берем как есть
                    res['salary.from'] = data['salary']['from']
                else:
                    res['salary.from'] = '0'   

                if data['salary']['to'] != None:                                      # Если есть то берем как есть
                    res['salary.to'] = data['salary']['to']
                else:
                    res['salary.to'] = '0'   

                if data['salary']['currency'] != None:
                    res['salary.currency'] = data['salary']['currency']
                else:
                    res['salary.currency'] = 'Тургрики'
            res['time'] = data['published_at']
            
            return res
        
        except Exception as e:
            print(f'Not Found {e}')
            print(f'EEE  _pars_url_zarplata   https://api.zarplata.ru/vacancies/{id}')
            ua = FakeUserAgent()
            data = requests.get(f'https://api.zarplata.ru/vacancies/{id}', headers={'User-Agent': ua.random}).json()

        finally:
            return res

    #& Парсинг RABOTA1000.RU    (use xpath)
    def _pars_url_other(self, id:str)->dict:
        res = {}
        try:
            #! PROXY
            #prox = self.proxy[random.randint(0, len(self.proxy)-1)]
            ua = FakeUserAgent()
            soup = BeautifulSoup(requests.get(f'https://rabota1000.ru/vacancy/{id}', proxies={'http':self.main_proxy, 'https':self.main_proxy}, headers={'User-Agent': ua.random}).text, 'html.parser')
            dom = lxml.etree.HTML(str(soup)) 
            res['vac_link'] = f'https://rabota1000.ru/vacancy/{id}'                                                                                             # Ссылка
            res['name'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[1]/h2')[0].text.replace('\n', '').lstrip().rstrip()            # Название
            res['city'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[3]/p[2]/span')[0].text.replace('\n', '')                       # Город (НЕТ)
            res['company'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[3]/p[1]')[0].text.replace('\n', '').lstrip().rstrip()       # Назвнание компании публикующей вакансию
            res['experience'] = ''                                                                                                                              # Опыт работы (нет замены на jun mid и sin)
            res['schedule'] = ''                                                                                                                                # Тип работы (офис/удаленка и тд) (НЕТ)
            res['employment'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[3]/ul/li[2]/span')[0].text.replace('\n', '')                                      # График работы
            res['skills'] = ''                                                                                                                                  # Ключевые навыки
            res['description'] = re.sub(self.re_html_tag_remove, '', dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[4]')[0].text).replace('\n', '')                                                   # Полное описание (НЕТ)
            if len(dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[2]/span'))>0:
                res['salary'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[1]/div[2]/span')[0].text.replace('\n', '').lstrip().rstrip()        # ЗП
            else:
                res['salary'] = 'Договорная'
            res['time'] = dom.xpath('/html/body/div[1]/main/div[2]/div/div/div[2]/section[3]/ul/li[1]/span')[0].text.replace('\n', '').lstrip().rstrip()        # Дата публикации
        
            return res

        except Exception as e:
            print(f'EEE  _pars_url_other   https://rabota1000.ru/vacancy/{id}')
        
        finally:
            return res

    #& Парсинг FINDER.VC        (use xpath)
    def _pars_url_finder(self, id:str)->list:
        res = {}
        try:
            #! PROXY
            #prox = self.proxy[random.randint(0, len(self.proxy)-1)]
            ua = FakeUserAgent()
            soup = BeautifulSoup(requests.get(f'https://finder.work/vacancies/{id}', proxies={'http':self.main_proxy, 'https':self.main_proxy}, headers={'User-Agent': ua.random}).text, 'html.parser')
            dom = lxml.etree.HTML(str(soup)) 
            res['vac_link'] = f'https://finder.work/vacancies/{id}' # Ссылка
            res['name'] = soup.find('h1', attrs={'class':'vacancy-info-header__title'}).text # Название
            res['city'] = ''              # Город (НЕТ)
            res['company'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/a')[0].text.replace('\n', '')        # Назвнание компании публикующей вакансию
            res['experience'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div[3]/div[1]/div[2]/div')[0].text.replace('\n', '')  # Опыт работы (нет замены на jun mid и sin)
            res['schedule'] = ''     # Тип работы (офис/удаленка и тд) (НЕТ
            res['employment'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[3]/div/div[2]/a')[0].text.replace('\n', '') # График работы
            res['skills'] = [li.text.replace('\n', '') for li in dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[3]/div[1]/div[2]/div[1]/ul')[0]]           # Ключевые навыки
            res['description'] = ''    # Полное описание (НЕТ)
            res['salary'] = dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[2]/div[2]/div[2]/div')[0].text.replace(u'\xa0', '').replace('\n', '')

            if 'сегодня' in dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[1]')[0].text:
                res['time'] = str(date.today())
            elif 'вчера' in dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[1]')[0].text:
                res['time'] = str(date.today() - timedelta(days=1))
            else:
                res['time'] = str(date.today() - timedelta(days=int(re.search(r'Опубликована (\d+)', dom.xpath('/html/body/div[1]/div[2]/div/main/div/div/div[2]/div[1]/div/div/div[1]/div/div[1]')[0].text).group(1))))
        
            return res
        
        except Exception as e:
            print(f'EEE  _pars_url_finder   https://finder.work/vacancies/{id}   {e}')

        finally:
            return res


# Инициализация DAG
dag = DAG(
    'Async_Pars_DAG',
    description='DAG для асинхронного парсинга вакансий из vac_name_list.csv',
    schedule_interval=None,
    start_date=datetime(2022, 10, 31),
    catchup=False
)

parser = Rabota1000_parser_async()

Pars = PythonOperator(
    task_id='call_method_topars',
    python_callable=parser.topars,
    dag=dag
)

def printheaddf():
    print(parser.df.head(5))

PrintRes = PythonOperator(
    task_id='print_dataframe',
    python_callable=printheaddf,
    dag=dag
)

# Определение порядка выполнения задач
Pars >> PrintRes