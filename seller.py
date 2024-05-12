import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон

    Эта функция отправляет запрос к API магазина Ozon для получения списка товаров.

    Args:
        last_id (str): Идентификатор последнего товара в предыдущем запросе.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Словарь, содержащий результат запроса к API магазина Ozon.

    Пример:
        >>> get_product_list("last_product_id", "client_id_value", "seller_token_value")
        {'result': [...]}

    Некорректный пример:
        >>> get_product_list(123, "client_id_value", "seller_token_value")
        TypeError: ...
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон
        Эта функция использует функцию get_product_list для получения списка товаров и
    извлекает артикулы (offer_id) из этого списка.

    Args:
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        list: Список артикулов товаров магазина Ozon.

    Пример:
        >>> get_offer_ids("client_id_value", "seller_token_value")
        ['offer_id_1', 'offer_id_2', ...]

    Некорректный пример:
        >>> get_offer_ids(123, "seller_token_value")
        TypeError: ...
    """

    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров
    Эта функция отправляет запрос к API магазина Ozon для обновления цен на товары.

    Args:
        prices (list): Список цен на товары для обновления.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Словарь с результатом запроса к API магазина Ozon.

    Пример:
        >>> update_price([{"offer_id": "offer_id_1", "price": "1000"}], "client_id_value", "seller_token_value")
        {'result': [...]}

    Некорректный пример:
        >>> update_price(["offer_id_1", "price_1"], "client_id_value", "seller_token_value")
        TypeError: ...
    """

    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки

        Эта функция отправляет запрос к API магазина Ozon для обновления остатков товаров.

    Args:
        stocks (list): Список остатков товаров для обновления.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        dict: Словарь с результатом запроса к API магазина Ozon.

    Пример:
        >>> update_stocks([{"offer_id": "offer_id_1", "stock": 10}], "client_id_value", "seller_token_value")
        {'result': [...]}

    Некорректный пример:
        >>> update_stocks(["offer_id_1", "stock_1"], "client_id_value", "seller_token_value")
        TypeError: ...
    """

    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio
    Эта функция загружает файл с данными о наличии товаров и их ценах с внешнего источника.

    Returns:
        list: Список словарей, представляющих остатки товаров.

    Пример:
        >>> download_stock()
        [{"Код": "123", "Количество": "10", "Цена": "5000"}, {...}]

    Некорректный пример:
        >>> download_stock("url_to_download")
        TypeError: ...
    """

    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает список остатков товаров для обновления.

    Эта функция создает список остатков товаров, который будет использоваться для обновления
    информации о наличии товаров на платформе Ozon.

    Args:
        watch_remnants (list): Список словарей, содержащих информацию о наличии товаров.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список остатков товаров для обновления.

    Пример:
        >>> create_stocks([{"Код": "123", "Количество": "10"}], ["123"])
        [{"offer_id": "123", "stock": 10}]

    Некорректный пример:
        >>> create_stocks(["123", "10"], ["123"])
        TypeError: ...
    """

    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен на товары для обновления.

    Эта функция создает список цен на товары, который будет использоваться для обновления
    информации о ценах товаров на платформе Ozon.

    Args:
        watch_remnants (list): Список словарей, содержащих информацию о наличии и ценах товаров.
        offer_ids (list): Список артикулов товаров.

    Returns:
        list: Список цен на товары для обновления.

    Пример:
        >>> create_prices([{"Код": "123", "Цена": "5000"}], ["123"])
        [{"auto_action_enabled": "UNKNOWN", "currency_code": "RUB", "offer_id": "123", "old_price": "0", "price": "5000"}]

    Некорректный пример:
        >>> create_prices(["123", "5000"], ["123"])
        TypeError: ...
    """

    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену. Пример: 5'990.00 руб. -> 5990
    Преобразует строку цены в упрощенный формат.

    Эта функция принимает на вход строку цены и удаляет из неё все нечисловые
    символы, возвращая упрощенную версию цены.

    Аргументы:
        price (str): Строка, представляющая цену в любом формате.

    Возвращаемое значение:
        str: Упрощенная версия строки цены, содержащая только числовые символы.

    Пример:
        >>> price_conversion("5'990.00 руб.")
        '5990'

    Некорректный пример:
        >>> price_conversion("Цена составляет 5'990.00 руб.")
        '599000'
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов

    Эта функция разделяет список на подсписки, каждый из которых содержит
    заданное количество элементов.

    Args:
        lst (list): Исходный список.
        n (int): Количество элементов в каждом подсписке.

    Yields:
        list: Подсписок, содержащий n элементов.

    Пример:
        >>> list(divide([1, 2, 3, 4, 5, 6], 2))
        [[1, 2], [3, 4], [5, 6]]

    Некорректный пример:
        >>> list(divide([1, 2, 3, 4, 5, 6], 0))
        ValueError: ...
    """

    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загружает цены на товары на платформу Ozon.

    Эта функция загружает цены на товары на платформу Ozon, используя данные
    о ценах, полученные из внешнего источника.

    Args:
        watch_remnants (list): Список словарей, содержащих информацию о наличии и ценах товаров.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        list: Список цен на товары, загруженных на платформу Ozon.

    Пример:
        >>> upload_prices([{"Код": "123", "Цена": "5000"}], "client_id_value", "seller_token_value")
        [{"auto_action_enabled": "UNKNOWN", "currency_code": "RUB", "offer_id": "123", "old_price": "0", "price": "5000"}]

    Некорректный пример:
        >>> upload_prices(["123", "5000"], "client_id_value", "seller_token_value")
        TypeError: ...
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загружает остатки товаров на платформу Ozon.

    Эта функция загружает остатки товаров на платформу Ozon, используя данные
    о наличии товаров, полученные из внешнего источника.

    Args:
        watch_remnants (list): Список словарей, содержащих информацию о наличии и ценах товаров.
        client_id (str): Идентификатор клиента для аутентификации.
        seller_token (str): Токен продавца для аутентификации.

    Returns:
        tuple: Кортеж, содержащий два списка: список непустых остатков и список всех остатков товаров.

    Пример:
        >>> upload_stocks([{"Код": "123", "Количество": "10"}], "client_id_value", "seller_token_value")
        ([{"offer_id": "123", "stock": 10}], [{"offer_id": "123", "stock": 10}])

    Некорректный пример:
        >>> upload_stocks(["123", "10"], "client_id_value", "seller_token_value")
        TypeError: ...
    """

    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    """Основная функция скрипта.

    Эта функция выполняет основную логику скрипта: загружает данные о товарах и их остатках,
    обновляет информацию на платформе Ozon.

    Примечание:
        Запускает скрипт для обновления данных на платформе Ozon.

    """
    
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
