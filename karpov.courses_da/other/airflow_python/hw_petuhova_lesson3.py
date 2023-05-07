import pandas as pd
from datetime import timedelta
from datetime import datetime
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.models import Variable

data = '/var/lib/airflow/airflow.git/dags/a.batalov/vgsales.csv'

year = 1994 + hash(f'a-petuhova') % 23

default_args = {
    'owner': 'a-petuhova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 13),
    'schedule_interval': '0 12 * * *'
}


@dag(default_args=default_args, catchup=False)
def a_petuhova_hw3():
    @task()
    # Собираем все данные за необходимый год
    def get_vgsales():
        vgsales = pd.read_csv(data) \
                 .query('Year == @year')
        return vgsales


    @task()
    # Какая игра была самой продаваемой в этом году во всем мире?
    def get_best_game(vgsales):
        best_game=vgsales.query('Year == @year') \
            [['Name', 'Global_Sales']] \
            .sort_values('Global_Sales', ascending=False) \
            .head(1)
        return best_game


    @task()
    # Игры какого жанра были самыми продаваемыми в Европе?
    def get_top_Europe_Genre(vgsales):
        top_Europe_Genre = vgsales.groupby('Genre', as_index=False).agg({'EU_Sales': 'sum'})
        top_Europe_Genre = top_Europe_Genre[top_Europe_Genre.EU_Sales == top_Europe_Genre.EU_Sales.max()]
        return top_Europe_Genre


    @task()
    # На какой платформе было больше всего игр, которые продались более чем миллионным тиражом в Северной Америке?
    def get_top_NA_Platform(vgsales):
        top_NA_Platform = vgsales.query('NA_Sales > 1') \
                              .groupby('Platform', as_index=False) \
                              .agg({'Name': 'count'}) \
                              .rename(columns={'Name': 'Number'})
        top_NA_Platform = top_NA_Platform[top_NA_Platform.Number == top_NA_Platform.Number.max()]
        return top_NA_Platform


    @task()
    # У какого издателя самые высокие средние продажи в Японии?
    def get_top_JP_Publisher(vgsales):
        top_JP_Publisher = vgsales.groupby('Publisher', as_index=False) \
                               .agg({'JP_Sales': 'mean'})
        top_JP_Publisher = top_JP_Publisher[top_JP_Publisher.JP_Sales == top_JP_Publisher.JP_Sales.max()]
        return top_JP_Publisher


    @task()
    # Сколько игр продались лучше в Европе, чем в Японии?
    def get_EU_more_JP(vgsales):
        EU_more_JP =  vgsales.query('EU_Sales > JP_Sales').shape[0]
        return EU_more_JP

    @task()
    # Вывод на печать
    def print_data(best_game, top_Europe_Genre, top_NA_Platform, top_JP_Publisher, EU_more_JP):

        context = get_current_context()
        date = context['ds']

        print(
f'''Sales data for {year} year for {date}.
Top Global sales game:
{best_game}

Top EU sales by genre:
{top_Europe_Genre}

Top NA sales by platform:
{top_NA_Platform}

Top JP sales by publisher:
{top_JP_Publisher}

Quantity of games sold better in EU than JP:
{EU_more_JP}
''')


    vgsales = get_vgsales()

    best_game = get_best_game(vgsales)
    top_Europe_Genre = get_top_Europe_Genre(vgsales)
    top_NA_Platform = get_top_NA_Platform(vgsales)
    top_JP_Publisher = get_top_JP_Publisher(vgsales)
    EU_more_JP = get_EU_more_JP(vgsales)

    print_data(best_game, top_Europe_Genre, top_NA_Platform, top_JP_Publisher, EU_more_JP)

a_petuhova_hw3 = a_petuhova_hw3()