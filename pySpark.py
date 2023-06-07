from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, expr
from pyspark.sql.functions import sum as pyspark_sum
import pyspark.sql.functions as F
import matplotlib.pyplot as plt

def handlePrepareData():
    spark = SparkSession.builder.getOrCreate()
    data = spark.read.csv('games.csv', header=True)

    data = data.toDF(*[col_name.lower() for col_name in data.columns])

    print('Преобразование названия столбцов в нижний регистр:' + f'\n{data.columns}\n')

    # Заменим year_of_release на int => nan -> 0
    data = data.withColumn('year_of_release', data['year_of_release'].cast('integer')).fillna(0, subset=['year_of_release'])

    print('столбце "year_of_release" для проверки преобразования типов:\n' + f'{data.select("year_of_release").distinct().rdd.flatMap(lambda x: x).collect()}\n')

    # В столбце user_score заменим 'tbd' для преобразования и последующей обработки
    data = data.withColumn('user_score', col('user_score').cast('float')).fillna(0.0, subset=['user_score'])

    # Заменим critic_score на int => nan -> 0 (оценка критиков от 1 до 100)
    data = data.withColumn('critic_score', data['critic_score'].cast('integer')).fillna(0, subset=['critic_score'])
    print('Уникальные значения в столбце "critic_score" для проверки преобразования типов:\n' + f'{data.select("critic_score").distinct().rdd.flatMap(lambda x: x).collect()}\n')

    # В столбце "Rating Pending" преобразуем пустые значения
    data = data.fillna('RP', subset=['rating'])

    # сВычисляем Сумму всех продаж
    data = data.withColumn('sum_sales', expr('na_sales + eu_sales + jp_sales + other_sales'))

    # По столбцам name и genre удаляем записи с nan (всего две)
    data = data.dropna(subset=['name', 'genre'])

    # Итоги подготовки файла к работе
    print('Количество NaN значений столбец "name": ' + f'{data.filter(col("name").isNull()).count()}')
    print('Количество NaN значений столбец "platform": ' + f'{data.filter(col("platform").isNull()).count()}')
    print('Количество NaN значений столбец "year_of_release": ' + f'{data.filter(col("year_of_release").isNull()).count()}')
    print('Количество NaN значений столбец "genre": ' + f'{data.filter(col("genre").isNull()).count()}')
    print('Количество NaN значений столбец "na_sales": ' + f'{data.filter(col("na_sales").isNull()).count()}')
    print('Количество NaN значений столбец "eu_sales": ' + f'{data.filter(col("eu_sales").isNull()).count()}')
    print('Количество NaN значений столбец "jp_sales": ' + f'{data.filter(col("jp_sales").isNull()).count()}')
    print('Количество NaN значений столбец "other_sales": ' + f'{data.filter(col("other_sales").isNull()).count()}')
    print('Количество NaN значений столбец "critic_score": ' + f'{data.filter(col("critic_score").isNull()).count()}')
    print('Количество NaN значений столбец "user_score": ' + f'{data.filter(col("user_score").isNull()).count()}')
    print('Количество NaN значений столбец "rating": ' + f'{data.filter(col("rating").isNull()).count()}')
    print('Количество NaN значений столбец "sum_sales": ' + f'{data.filter(col("sum_sales").isNull()).count()}\n')

    return data

def handleIntroductoryAnalysis(data):
    Df = data
    print('Исследовательский анализ данных')

    print('Количество игр выпущенных в период 1980 по 1990: ' + f'{Df.filter((Df["year_of_release"] >= 1980) & (Df["year_of_release"] <= 1990)).count()}')
    print('Количество игр выпущенных в период 1990 по 2000: ' + f'{Df.filter((Df["year_of_release"] >= 1990) & (Df["year_of_release"] <= 2000)).count()}')
    print('Количество игр выпущенных в период 2000 по 2010: ' + f'{Df.filter((Df["year_of_release"] >= 2000) & (Df["year_of_release"] <= 2010)).count()}')
    print('Количество игр выпущенных в период 2010 по 2020: ' + f'{Df.filter((Df["year_of_release"] >= 2010) & (Df["year_of_release"] <= 2020)).count()}\n')

    # Cписок платформ по продажам отсортированный по убыванию
    platformSales = Df.groupby('platform').agg(F.sum('sum_sales').alias('total_sales')).sort(F.desc('total_sales'))
    topPlatforms = platformSales.limit(5).select('platform').rdd.flatMap(lambda x: x).collect()

    print('5 платформ по продажам игр: ' + f'{topPlatforms}\n')

    # Выборка из исходных данных по 5 платформам (выбранным)
    filteredData = Df.filter(Df['platform'].isin(topPlatforms))

    # Pаспределение по годам
    platformSalesByYear = filteredData.groupby('platform').pivot('year_of_release').agg(F.sum('sum_sales')).na.fill(0)

    # Выводим полученные данные
    platformSalesByYear.show()

    # Прогноз по продажам на 2017 год (основываясь на данных за 2010-2016 года)
    actualPeriodData = Df.filter((Df["year_of_release"] >= 2010) & (Df["year_of_release"] <= 2016))
    salesData = actualPeriodData.groupby('year_of_release').agg(F.sum('sum_sales').alias('total_sales'))

    # Разница в продажах между годами
    salesDiff = salesData.withColumn('sales_diff', F.col('total_sales') - F.lag('total_sales').over(Window.orderBy('year_of_release')))
    salesDiff = salesDiff.filter(salesDiff['year_of_release'] >= 2011)
    salesArray = salesDiff.select('sales_diff').rdd.flatMap(lambda x: x).collect()
    forecast_2017 = salesData.filter(salesData['year_of_release'] == 2016).select('total_sales').rdd.flatMap(lambda x: x).collect()[0] + sum(salesArray) / len(salesArray)
    print('Прогнозируемые продажи на 2017 год на основе предыдущих годов' + f'{forecast_2017}\n')

    # Платформ по продажам
    platformSales = Df.groupby('platform').agg(F.sum('sum_sales').alias('total_sales')).sort(F.desc('total_sales'))

    print('Продажи по первым 5-ти платформам:')
    topPlatformSales = platformSales.limit(5).toPandas()
    for i, row in topPlatformSales.iterrows():
        platform = row['platform']
        actualDataFirst = Df.filter((Df['year_of_release'] == 2016) & (Df['platform'] == platform)).agg(F.sum('sum_sales').alias('total_sales')).collect()[0]['total_sales']
        actualDataSecond = Df.filter((Df['year_of_release'] == 2015) & (Df['platform'] == platform)).agg(F.sum('sum_sales').alias('total_sales')).collect()[0]['total_sales']
        if actualDataFirst and actualDataSecond:
            if actualDataFirst > actualDataSecond:
                parameter = 'Возрастает'
            else:
                parameter = 'Убывает'
        else:
            parameter = 'Продажи отсутствуют'
        print(f'{platform} - {row["total_sales"]} - {parameter}')

    # График «ящик с усами» по продажам на определенные платформы
    # Выборка по платформам

    platformSales = Df.groupby('platform').agg(F.sum('sum_sales').alias('total_sales')).sort(F.desc('total_sales'))
    topPlatforms = [row['platform'] for row in platformSales.limit(5).collect()]
    salesData = Df.filter(Df['platform'].isin(topPlatforms)).select('platform', 'sum_sales')
    salesData = salesData.withColumn('platform', F.when(salesData['platform'].isin(topPlatforms), salesData['platform']).otherwise('Other'))
    salesData.toPandas().boxplot(column='sum_sales', by='platform')
    # Строим график
    plt.xlabel('Платформа')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение глобальных продаж игр по платформам')
    plt.show()
    plt.close()

    # Влияние отзывов критиков и пользователей на продажи игр
    platformData = Df.filter(Df['platform'] == 'PS2').na.drop(subset=['user_score', 'critic_score'])

    # Строим диаграммы рассеяния
    plt.scatter(platformData.select('user_score').rdd.flatMap(lambda x: [10 * i for i in x]).collect(), platformData.select('sum_sales').rdd.flatMap(lambda x: x).collect(), label='Отзывы пользователей')
    plt.scatter(platformData.select('critic_score').rdd.flatMap(lambda x: x).collect(), platformData.select('sum_sales').rdd.flatMap(lambda x: x).collect(), label='Отзывы критиков')
    plt.xlabel('Отзывы')
    plt.ylabel('Глобальные продажи')
    plt.title('Влияние отзывов на продажи - платформа PS2')
    plt.legend()
    plt.show()
    plt.close()

    # Вывод матрицы корреляции
    correlation = platformData.select('user_score', 'critic_score', 'sum_sales').toPandas().corr()
    print('Матрица корреляции по продажам и оценкам:')
    print(correlation)

    # Группируем данные по жанру и суммируем продажи
    genreSales = data.groupby('genre').agg(F.sum('sum_sales').alias('total_sales')).sort(F.desc('total_sales'))

    # Строим график столбцов для распределения продаж по жанрам
    genreSalesPd = genreSales.toPandas()
    genreSalesPd.plot(kind='bar', x='genre', y='total_sales', color='blue')

    plt.xlabel('Жанр')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение продаж по жанрам')
    plt.xticks(rotation=45)
    plt.show()
    plt.close()

    # Выделяем самые прибыльные жанры
    topGenres = genreSalesPd.head(5)
    print('Самые прибыльные жанры:')
    print(topGenres.head())

    print('Жанры с высокими продажами:')
    print(genreSalesPd.head())

    print('Жанры с низкими продажами:')
    print(genreSalesPd.tail())


def handleSetCharacterOfUser(data):

    print('Составим портрет пользователя в каждом регионе')

    # Наиболее популярные платформы в регионах
    platformSales = data.groupby('platform').agg(pyspark_sum('na_sales').alias('na_sales'), pyspark_sum('eu_sales').alias('eu_sales'), pyspark_sum('jp_sales').alias('jp_sales'))
    print('Наиболее популярные платформы в NA:', platformSales.orderBy('na_sales', ascending=False).limit(5).toPandas())
    print('Наиболее популярные платформы в EU:', platformSales.orderBy('eu_sales', ascending=False).limit(5).toPandas())
    print('Наиболее популярные платформы в JP:', platformSales.orderBy('jp_sales', ascending=False).limit(5).toPandas())

    # Наиболее популярные жанры в регионах
    genreSales = data.groupby('genre').agg(pyspark_sum('na_sales').alias('na_sales'), pyspark_sum('eu_sales').alias('eu_sales'), pyspark_sum('jp_sales').alias('jp_sales'))
    print('Наиболее популярные жанры в NA:', genreSales.orderBy('na_sales', ascending=False).limit(5).toPandas())
    print('Наиболее популярные жанры в EU:', genreSales.orderBy('eu_sales', ascending=False).limit(5).toPandas())
    print('Наиболее популярные жанры в JP:', genreSales.orderBy('jp_sales', ascending=False).limit(5).toPandas())

    # График по продажам по рейтингоу ESRB
    ratingSales = data.groupby('rating').agg(pyspark_sum('na_sales').alias('na_sales')).orderBy('na_sales', ascending=False)
    ratingSales.toPandas().plot(kind='bar', x='rating', y='na_sales', color='blue')
    plt.xlabel('Оценка рейтинга')
    plt.ylabel('Глобальные продажи')
    plt.title('Распределение продаж по рейтингу ESRB')
    plt.xticks(rotation=45)
    plt.show()
    plt.close()


def handleTestingHypotheses(data):

    print('Проверяем гипотезы:')

    # Гипотеза 1
    print('Гипотеза 1 (Средние значения рейтинга по платформам XBox One и Pc одинаковые)')

    xboxUserRating = data.filter(data['platform'] == 'XOne')
    xboxMeanRating = xboxUserRating.agg({'user_score': 'mean'}).first()[0]
    print('Средний рейтинг по платформе XBox One: ' + f'{xboxMeanRating}')

    pcRating = data.filter(data['platform'] == 'PC')
    pcMeanRating = pcRating.agg({'user_score': 'mean'}).first()[0]
    print('Средний рейтинг по платформе PC: ' + f'{pcMeanRating}')

    ALPHA = 0.25
    if abs(xboxMeanRating - pcMeanRating) < ALPHA:
        print('True')
    else:
        print('False')

    print('Отклонение значения от ALPHA: ' + f'{abs(abs(xboxMeanRating - pcMeanRating) - ALPHA)}')

    # Гипотеза 2
    print('Гипотеза 2 (Средние значения рейтинга по жанрам action и sports разные)')
    
    actionRating = data[data['genre'] == 'Action']
    actionRating = data.filter(data['genre'] == 'Action')
    actionMeanRating = actionRating.agg({'user_score': 'mean'}).first()[0]
    print('Средний рейтинг по жанру Action: ' + f'{actionMeanRating}')

    sportsRating = data[data['genre'] == 'Sports']
    sportsRating = data.filter(data['genre'] == 'Sports')
    sportsMeanRating = sportsRating.agg({'user_score': 'mean'}).first()[0]
    print('Средний рейтинг по жанру Sports: ' + f'{sportsMeanRating}')

    if abs(actionMeanRating - sportsMeanRating) > ALPHA:
        print('True')
    else:
        print('False')
    
    print('Отклонение значения от ALPHA: ' + f'{abs(abs(actionMeanRating - sportsMeanRating) - ALPHA)}')


def main():
    data = handlePrepareData()
    handleIntroductoryAnalysis(data)
    handleSetCharacterOfUser(data)
    handleTestingHypotheses(data)


if __name__ == '__main__':
    main()

main()