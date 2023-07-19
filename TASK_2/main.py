from bs4 import BeautifulSoup as BS
import pandas as pd
import numpy as np
import requests
import time
import logging
import asyncio
from aiohttp import ClientSession
import psycopg2


logging.basicConfig(filename='log.csv', filemode='w',
                    encoding='cp1251', level=logging.DEBUG,
                   format="%(asctime)s;%(levelname)s;%(message)s")




# -----------------------------------Сбор ссылок на Python-вакансии  через BeautifulSoap

def grab_vacancy_links(query: str, page_num: int):
	hdrs = {
		"user-agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/85.0.4183.121 Safari/537.36'}
	url = "https://hh.ru/search/vacancy"
	
	params = {"text": query , "search_field": ["name", "description"], "area": "113",               # "area": "113" Россия
	          "page": page_num}
	
	try:
		response = requests.get(url, headers=hdrs, params=params)
		
		if response.status_code == 200:
			soap = BS(response.content, 'html.parser')   #lxml
			href_tags = soap.find_all("a")
			links = [item.attrs.get("href").split("?from=")[0].split("/")[-1] for item in href_tags if
			         "https://" in item.attrs.get("href") and
			         "vacancy" in item.attrs.get("href")]
			logging.info(f"{query}: OK. Status_code=200")
			
			return links
		
		else:
			logging.warning(f"{query}: status_code = {response.status_code}")
			return []
	except:
		logging.error(f"{query}: Couldn't get request")
		return []

vacancy_list = []
query = "middle python developer"

for page in range(5):
	vacancy_list = vacancy_list + grab_vacancy_links(query=query, page_num=page)
	time.sleep(0.7)


# ========================================== END: Сбор ссылок на Python-вакансии  через BeautifulSoap



# ------------------------------------------ Асинхронный парсинг вакансий через API hh.ru

async def get_vacancy(Session, url):
	async with Session.get(url) as response:
		if response.status==200:
			logging.debug(f"{url}: OK. Status_code=200")
			try:
				data = await response.json()
				skills = [el["name"] for el in data['key_skills']]
				return [data['employer']['name'], data['name'] , BS(data["description"], 'html.parser').text, "|".join(skills) ]
			except:
				logging.debug(f"{url}: Can't parse json")
				return[ None,None,None,None]
		else:
			logging.warning(f"{url}: The request failed. Status_code={response.status}")

	
async def main(vacancies):
	async with ClientSession("https://api.hh.ru/") as MySession:
		logging.debug("Session successfully started.")
		tasks = []
		for vacancy_id in vacancies:
			tasks.append(asyncio.create_task(get_vacancy(MySession, url=f"/vacancies/{vacancy_id}")))
		
		logging.debug(f"{len(tasks)} tasks started")
		result = await asyncio.gather(*tasks)
	return result
	
	
start=time.time()
print(f"len(vacancy_list) = {len(vacancy_list)}")
parse_result = asyncio.run(main(vacancy_list))

print("Время выполнения асинхронных запросов через API, с: ", time.time() - start)



#-------------------------Запись результатов в локальную БД:


password = '**********'

with  psycopg2.connect ( dbname= 'hw1',user='postgres', password =password, host= 'localhost') as conn:
	db_cur = conn.cursor()
	query_text = """
	create table if not exists Vacancies
	    (id integer primary key generated always as identity,
	    company_name text not null,
	    position text not null,
	    job_description text,
	    key_skills text
	    )
	"""
	db_cur.execute(query_text)
	conn.commit()
	
	query_text = """
	insert into Vacancies(company_name, position, job_description, key_skills)
	values (%s,%s,%s,%s)
	"""
	
	try:
		db_cur.executemany(query_text, list(np.array(parse_result)))
		conn.commit()
		logging.debug("Данные записаны в БД")
	except:
		logging.error("Ошибка записи результатов парсинга в БД")
		conn.rollback()
		
	
