import pymongo
import pandas as pd
from pymongo import MongoClient


def startConnect():
    client = MongoClient('mongodb://localhost:27017')
    data = pd.read_csv('games.csv').to_dict(orient='records')

    return client['games']['games_collection'].insert_many(data)


def connect():
    client = MongoClient('mongodb://localhost:27017')

    print('Success connect')
    return client['games']['games_collection']


def getDataFromDatabase(collection):
    rows = collection.find()
    for row in range(0, 5):
        print(rows[row])


def getDataFromDatabaseWithFilters(collection, name='Tour de France 2014'):
    for row in collection.find({'Name': name}):
        print(row)


def createObj(collection, name):
    new_row = {
        'Name': name,
        'Platform': 'PS4',
        'Year_of_Release': '2016',
        'Genre': 'Sports',
        'NA_sales': 0.0,
        'EU_sales': 0.0,
        'JP_sales': 0.0,
        'Other_sales': 0.0,
        'Critic_Score': 100.0,
        'User_Score': '5.1',
        'Rating': 'E'
    }

    collection.insert_one(new_row)
    print('Success create')
    print('Записи по фильтру "test-note"')
    for row in collection.find({'Name': name}):
        print(row)


def createObjects(collection, names):
    new_rows = [
        {
        'Name': names[0],
        'Platform': 'PS4',
        'Year_of_Release': '2016',
        'Genre': 'Sports',
        'NA_sales': 0.0,
        'EU_sales': 0.0,
        'JP_sales': 0.0,
        'Other_sales': 0.0,
        'Critic_Score': 100.0,
        'User_Score': '5.1',
        'Rating': 'E'
        },
        {
            'Name': names[1],
            'Platform': 'PS4',
            'Year_of_Release': '2016',
            'Genre': 'Sports',
            'NA_sales': 0.0,
            'EU_sales': 0.0,
            'JP_sales': 0.0,
            'Other_sales': 0.0,
            'Critic_Score': 100.0,
            'User_Score': '5.1',
            'Rating': 'E'
        },
        {
            'Name': names[0],
            'Platform': 'PS4',
            'Year_of_Release': '2016',
            'Genre': 'Sports',
            'NA_sales': 0.0,
            'EU_sales': 0.0,
            'JP_sales': 0.0,
            'Other_sales': 0.0,
            'Critic_Score': 100.0,
            'User_Score': '5.1',
            'Rating': 'E'
        }
    ]
    collection.insert_many(new_rows)

    print('Success create')
    print('Записи по фильтру "2016"')
    for row in collection.find({'Year_of_Release': '2016'}):
        print(row)


def changeObj(collection, name, platform):
    collection.update_one({ 'Name': name }, { '$set': { 'Platform': platform } })

    print('Success edit')
    print('Записи по фильтру "PS4"')
    for row in collection.find({'Platform': platform}):
        print(row)


def changeObjects(collection, name, platform):
    collection.update_many({ 'Name': name }, { '$set': { 'Platform': platform } })

    print('Success edit')
    print('Записи по фильтру "PS4"')
    for row in collection.find({'Platform': platform}):
        print(row)


def deleteObj(collection, name):
    collection.delete_one({ 'Name': name })

    print('Success deleted')
    print('Записи по фильтру "2016"')
    for row in collection.find({ 'Year_of_Release': '2016' }):
        print(row)


def deleteManyObj(collection, name):
    collection.delete_many({'Name': name})

    print('Success deleted')
    print('Записи по фильтру "2016"')
    for row in collection.find({ 'Year_of_Release': '2016' }):
        print(row)


print('Шаг 1 - Подключение к бд')
collection = connect()

print('Шаг 2 - Получение первых 5 записей из бд без фильтров')
getDataFromDatabase(collection)

print('Шаг 3 - Получение записей по фильтру "Tour de France 2014"')
getDataFromDatabaseWithFilters(collection)

print('Шаг 4 - Создание записи с именем "test-note"')
createObj(collection, 'test-note')

print('Шаг 5 - Создание записи с именами "test-note-1" и "test-note-2"')
createObjects(collection, ['test-note-1', 'test-note-2'])

print('Шаг 6 - Редактирование платформы для записи с именем "test-note-2" на "PS4"')
changeObj(collection, 'test-note-2', 'PS4')

print('Шаг 7 - Редактирование платформы для записей с именем "test-note-1" на "PS5"')
changeObjects(collection, 'test-note-1', 'PS5')

print('Шаг 8 - Удаление записи с именем "test-note"')
deleteObj(collection, 'test-note')

print('Шаг 9 - Удаление записей с именем "test-note-1"')
deleteManyObj(collection, 'test-note-1')

print('Шаг 10 - Удаление записи с именем "test-note-2"')
deleteManyObj(collection, 'test-note-2')