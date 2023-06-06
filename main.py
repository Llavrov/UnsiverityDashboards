import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

from statsmodels.tsa.holtwinters import ExponentialSmoothing


def handlePrepareData():
    data = pd.read_csv('games.csv')

    print('Подготавливаем данные')

    data.columns = data.columns.str.lower()

    print('Преобразование названия столбцов в нижний регистр:' + f'\n{data.columns}\n')
    # Заменим year_of_release на int => nan -> 0

    data['year_of_release'] = data['year_of_release'].fillna(0).astype(int)
    print('В столбце "year_of_release" для проверки преобразования типов:\n' + f'{data["year_of_release"].unique()}\n')

    # В столбце user_score заменим 'tbd' для преобразования и последующей обработки
    data['user_score'] = data['user_score'].replace('tbd', np.nan)

    # В столбце user_score заменим 'tbd' для преобразования и последующей обработки
    # Заменим user_score на float => nan -> 0 (оценка пользователей float)
    data['user_score'] = data['user_score'].fillna(0.0).astype(float)
    print('В столбце "user_score" для проверки преобразования типов:\n' + f'{data["user_score"].unique()}\n')

    # Заменим critic_score на int => nan -> 0 (оценка критиков от 1 до 100)
    data['critic_score'] = data['critic_score'].fillna(0).astype(int)
    print('Уникальные значения в столбце "critic_score" для проверки преобразования типов:\n' + f'{data["critic_score"].unique()}\n')

    # В столбце "Rating Pending" преобразуем пустые значения
    data['rating'] = data['rating'].fillna('RP')

    # сВычисляем Сумму всех продаж
    data['sum_sales'] = data[['na_sales', 'eu_sales', 'jp_sales', 'other_sales']].sum(axis=1)

    # По столбцам name и genre удаляем записи с nan (всего две)
    data = data.dropna(subset=['name'])

    # Итоги подготовки файла к работе
    print('Количество NaN значений столбец "name": ' + f'{data["name"].isnull().sum()}')
    print('Количество NaN значений столбец "platform": ' + f'{data["platform"].isnull().sum()}')
    print('Количество NaN значений столбец "year_of_release": ' + f'{data["year_of_release"].isnull().sum()}')
    print('Количество NaN значений столбец "genre": ' + f'{data["genre"].isnull().sum()}')
    print('Количество NaN значений столбец "na_sales": ' + f'{data["na_sales"].isnull().sum()}')
    print('Количество NaN значений столбец "eu_sales": ' + f'{data["eu_sales"].isnull().sum()}')
    print('Количество NaN значений столбец "jp_sales": ' + f'{data["jp_sales"].isnull().sum()}')
    print('Количество NaN значений столбец "other_sales": ' + f'{data["other_sales"].isnull().sum()}')
    print('Количество NaN значений столбец "critic_score": ' + f'{data["critic_score"].isnull().sum()}')
    print('Количество NaN значений столбец "user_score": ' + f'{data["user_score"].isnull().sum()}')
    print('Количество NaN значений столбец "rating": ' + f'{data["rating"].isnull().sum()}')
    print('Количество NaN значений столбец "sum_sales": ' + f'{data["sum_sales"].isnull().sum()}\n')

    return data


def handleIntroductoryAnalysis(data):
    print('Исследовательский анализ данных')

    print('Количество игр выпущенных в период с 1980 по 1990: ' + f'{len(data[(data["year_of_release"] >= 1980) & (data["year_of_release"] <= 1990)])}')
    print('Количество игр выпущенных в период с 1990 по 2000: ' + f'{len(data[(data["year_of_release"] >= 1990) & (data["year_of_release"] <= 2000)])}')
    print('Количество игр выпущенных в период с 2000 по 2010: ' + f'{len(data[(data["year_of_release"] >= 2000) & (data["year_of_release"] <= 2010)])}')
    print('Количество игр выпущенных в период с 2010 по 2020: ' + f'{len(data[(data["year_of_release"] >= 2010) & (data["year_of_release"] <= 2020)])}\n')

    # Cписок платформ по продажам отсортированный по убыванию
    platformSales = data.groupby('platform')['sum_sales'].sum().sort_values(ascending=False)
    topPlatforms = platformSales.head(5).index

    print('5 платформ по продажам игр: ' + f'{topPlatforms}\n')

    # Выборка из исходных данных по 5 платформам (выбранным)
    filteredData = data[data['platform'].isin(topPlatforms)]

    # Pаспределение по годам
    platformSalesByYear = filteredData.groupby(['platform', 'year_of_release'])['sum_sales'].sum().unstack()

    platformSalesByYear.plot(kind='bar', stacked=True)
    plt.xlabel('Год')
    plt.ylabel('Продажи')
    plt.title('Продажи по платформам по годам')
    plt.legend(loc='upper left')
    plt.show()
    plt.close()

    # Прогноз по продажам на 2017 год (основываясь на данных за 2010-2016 года)
    actualPeriodData = data[(data["year_of_release"] >= 2010) & (data["year_of_release"] <= 2016)]
    salesData = actualPeriodData.groupby('year_of_release')['sum_sales'].sum()

    # Разница в продажах между годами
    salesMas = []
    for counter in range(2010, 2015):
        salesMas.append((salesData[salesData.index == counter + 1].values[0])
                         - (salesData[salesData.index == counter].values[0]))
    salesArray = np.array(salesMas)
    print('Прогноз по продажам на 2017 год, основываясь на предыдущих годов: ' + f'{salesData[salesData.index == 2016].values[0] + salesArray.mean()}\n')

    # Платформ по продажам
    platformSales = data.groupby('platform')['sum_sales'].sum().sort_values(ascending=False)

    print('Продажи по первым 5-ти платформам:')
    for counter in range(0, 5):
        # Берем продажи по платформе в 2015 и 2016 году соответственно
        actual_data_first = data[(data['year_of_release'] == 2016) & (data['platform'] == platformSales.index[counter])]
        actual_data_second = data[(data['year_of_release'] == 2015) & (data['platform'] == platformSales.index[counter])]
        actual_data_first_sum = actual_data_first['sum_sales'].sum()
        actual_data_second_sum = actual_data_second['sum_sales'].sum()

        if actual_data_first_sum > actual_data_second_sum:
            parameter = 'Возрастает'
        else:
            parameter = 'Убывает'

        print(f'{platformSales.index[counter]} - {platformSales[counter]} - {parameter}')

    # График «ящик с усами» по продажам на определенные платформы
    # Выборка по платформам

    platformSales = data.groupby('platform')['sum_sales'].sum()
    sorted_platforms = platformSales.sort_values(ascending=False)
    topPlatforms = sorted_platforms.head(5)
    salesData = [data[data['platform'] == platform]['sum_sales'] for platform in topPlatforms.index]
    # Строим график
    plt.boxplot(salesData, labels=topPlatforms.index)
    plt.xlabel('Платформа')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение глобальных продаж игр по платформам')
    plt.show()
    plt.close()

    # Влияние отзывов критиков и пользователей на продажи игр
    platform_data = data[data['platform'] == 'PS2']
    platform_data = platform_data.dropna(subset=['user_score', 'critic_score'])

    # Строим диаграммы рассеяния
    plt.scatter(platform_data['user_score']*10, platform_data['sum_sales'], label='Отзывы пользователей')
    plt.scatter(platform_data['critic_score'], platform_data['sum_sales'], label='Отзывы критиков')
    plt.xlabel('Отзывы')
    plt.ylabel('Глобальные продажи')
    plt.title('Влияние отзывов на продажи - платформа PS2')
    plt.legend()
    plt.show()
    plt.close()
    # Вывод матрицы корреляции
    correlation = platform_data[['user_score', 'critic_score', 'sum_sales']].corr()
    print('Матрица корреляции по продажам и оценкам:')
    print(correlation)

    # Группируем данные по жанру и суммируем продажи
    genre_sales = data.groupby('genre')['sum_sales'].sum().sort_values(ascending=False)

    # Строим график столбцов для распределения продаж по жанрам
    genre_sales.plot(kind='bar', color='green')
    plt.xlabel('Жанр')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение продаж по жанрам')
    plt.xticks(rotation=45)
    plt.show()
    plt.close()

    # Выделяем самые прибыльные жанры
    top_genres = genre_sales.head(5)
    print('Самые прибыльные жанры:')
    print(top_genres.head())

    print('Жанры с высокими продажами:')
    print(genre_sales.head())

    print('Жанры с низкими продажами:')
    print(genre_sales.tail())

def handleSetCharacterOfUser(data):

    print('Составим портрет пользователя в каждом регионе')

    # Наиболее популярные платформы в регионах
    platformSales = data.groupby('platform')[['na_sales', 'eu_sales', 'jp_sales']].sum()
    print('Наиболее популярные платформы в NA:', platformSales.sort_values(['na_sales'], ascending=False).head())
    print('Наиболее популярные платформы в EU:', platformSales.sort_values(['eu_sales'], ascending=False).head())
    print('Наиболее популярные платформы в JP:', platformSales.sort_values(['jp_sales'], ascending=False).head())

    # Наиболее популярные жанры в регионах
    genre_sales = data.groupby('genre')[['na_sales', 'eu_sales', 'jp_sales']].sum()
    print('Наиболее популярные жанры в NA:', genre_sales.sort_values(['na_sales'], ascending=False).head())
    print('Наиболее популярные жанры в EU:', genre_sales.sort_values(['eu_sales'], ascending=False).head())
    print('Наиболее популярные жанры в JP:', genre_sales.sort_values(['jp_sales'], ascending=False).head())

    # График по продажам по рейтингоу ESRB
    rating_sales = data.groupby('rating')[['na_sales']].sum().sort_values('na_sales', ascending=False)
    rating_sales.plot(kind='bar', color='purple')

    plt.xlabel('Оценка рейтинга')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение продаж по рейтингу ESRB')
    plt.xticks(rotation=45)
    plt.show()
    plt.close()


def hypotesis_testing(data):

    print('Проверяем гипотезы:')

    # Гипотеза 1
    print('Гипотеза 1 (Средние значения рейтинга по платформам XBox One и Pc одинаковые)')

    xboxUserRating = data[data['platform'] == 'XOne']
    print('Средний рейтинг по платформе XBox One: ' + f'{xboxUserRating["user_score"].mean()}')
    pcRating = data[data['platform'] == 'PC']
    print('Средний рейтинг по платформе PC: ' + f'{pcRating["user_score"].mean()}')

    ALPHA = 0.25
    if abs(pcRating["user_score"].mean() - xboxUserRating["user_score"].mean()) < ALPHA:
        print('True')
    else:
        print('False')

    print('Отклонение значения от ALPHA: ' + f'{abs(abs(pcRating["user_score"].mean() - xboxUserRating["user_score"].mean()) - ALPHA)}')

    # Гипотеза 2
    print('Гипотеза 2 (Средние значения рейтинга по жанрам action и sports разные)')
    
    actionRating = data[data['genre'] == 'Action']
    print('Средний рейтинг по жанру Action: ' + f'{actionRating["user_score"].mean()}')
    
    sportsRating = data[data['genre'] == 'Sports']
    print('Средний рейтинг по жанру Sports: ' + f'{sportsRating["user_score"].mean()}')

    if abs(actionRating["user_score"].mean() - sportsRating["user_score"].mean()) > ALPHA:
        print('True')
    else:
        print('False')
    
    print('Отклонение значения от ALPHA: ' + f'{abs(abs(actionRating["user_score"].mean() - sportsRating["user_score"].mean()) - ALPHA)}')

def main():
    data = handlePrepareData()
    handleIntroductoryAnalysis(data)
    handleSetCharacterOfUser(data)
    hypotesis_testing(data)


if __name__ == '__main__':
    main()

main()