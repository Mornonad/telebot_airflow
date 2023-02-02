import requests
import telebot
from telebot import types
from airflow.models import Variable

my_token = Variable.get('hh_bot_key')

bot = telebot.TeleBot(my_token)
params = {'text': 'Data Engineer',
          'area': '113',
          'type': 'open',
          'per_page': '5',
          'page': '0',
          'period': 1,
          'experience.id': ['noExperience', 'between1And3']
          }

def start():
    send_mess = f"<b>Привет</b>!\nВакансии на сегодня:"
    bot.send_message(447150849, send_mess, parse_mode='html')

def get_vacancies():
    vacancies = []
    url = 'https://api.hh.ru/vacancies'
    response = requests.get(url, params)
    data = response.json()
    vacancies.append(data)
    return vacancies

def parse_vacancies():
    new_json = []
    vacancies = get_vacancies()

    for i in vacancies:
        for irem in i['items']:
            name = irem['name']
            employer = irem['employer']['name']
            area = irem['area']['name']
            alternate_url = irem['alternate_url']

            if irem['salary'] != None:
                if irem['salary']['from'] != None and irem['salary']['to'] != None:
                    salary_from = irem['salary']['from']
                    salary_to = irem['salary']['to']
                    salary = f"{salary_from} - {salary_to}"
                elif irem['salary']['from'] != None and irem['salary']['to'] == None:
                    salary_from = irem['salary']['from']
                    salary = f"от {salary_from}"
                elif irem['salary']['from'] == None and irem['salary']['to'] != None:
                    salary_to = irem['salary']['to']
                    salary = f"до {salary_to}"
            else:
                salary = 'Зарплата не указана'

            vac = {
                'name': name,
                'salary': salary,
                'employer': employer,
                'area': area,
                'alternate_url': alternate_url
            }

            new_json.append(vac)

    vacancies_prep = []
    for lists in new_json:
        for values in lists.values():
            vacancies_prep.append(values)

    list = []
    for i in range(0, len(vacancies_prep), 5):
        list.append(vacancies_prep[i:i + 5])

    return list

def send_vacancies():
    list = parse_vacancies()
    for item in list[:5]:
        for j in item:
            vac_url = item[4]
            vacan = item[:4]

            vacan2 = '\n'.join(map(str, vacan))

        markup = types.InlineKeyboardMarkup()
        url = types.InlineKeyboardButton(text='Подробнее', url=vac_url)
        markup.add(url)
        bot.send_message(447150849, vacan2, reply_markup=markup)



from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


default_args = {
    'owner': 'mornonad',
    'start_date': datetime(2022, 1, 1),
    'depends_on_past': False,
    'retries': 1
}

dag = DAG('get_vacancies_from_hh',
    default_args=default_args,
    catchup=False,
    schedule_interval='00 11 * * *')


t1 = PythonOperator(
    task_id='get_started',
    python_callable=start,
    dag=dag)

t2 = PythonOperator(
    task_id='get_vacancies',
    python_callable=get_vacancies,
    dag=dag)

t3 = PythonOperator(
    task_id='parse_vacancies',
    python_callable=parse_vacancies,
    dag=dag)

t4 = PythonOperator(
    task_id='send_vacancies',
    python_callable=send_vacancies,
    dag=dag)


t1 >> t2 >> t3 >> t4