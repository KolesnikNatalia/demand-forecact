"""
Фикстура выгрузки для тестов: 3 магазина × 5 товаров × 14 дней.

Строки лежат здесь списком словарей — так видно, какой случай проверяется, —
а parquet из них делает сам `clickhouse-local` в точной схеме сырья. Схема важна:
строковые колонки выгрузки объявлены `Nullable`, и грабли с ними должны
воспроизводиться в тесте, а не всплывать на полной выгрузке.

Период фикстуры 2026-05-11 … 2026-05-24, дата когорты 2026-05-19 приходится
на его середину.

Что покрыто (по одной паре на случай, если не сказано иное):

| пара | случай |
|---|---|
| S1 × I1 | обычный ряд `шт`: дефицит при нулевом остатке, возврат, все дни в матрице |
| S1 × I2 | `кг` в той же подподгруппе, что `шт`; остаток 0.05 кг — больше нуля, но ниже порога наличия, то есть дефицит |
| S1 × I3 | спорная подгруппа (`НАПИТКИ`); внутри жизни день без строки и день вне матрицы с продажей |
| S2 × I1 | матричные дни до первой и после последней активности |
| S2 × I4 | пара без матричных дней: продажи только вне матрицы |
| S2 × I5 | пустой ряд: матричные дни есть, активности нет ни одного дня |
| S3 × I1 | пара, появившаяся после даты когорты |
| S3 × I2 | магазин, выбивающийся из общей массы по выручке |

Строки «всё по нулям» ставятся только матричным дням: в выгрузке их не бывает
вне матрицы, и фикстура это правило соблюдает.

Рядом — вторая, маленькая выгрузка `quadrant_rows()`: четыре пары магазина S9,
по одной на квадрант спроса. Она отдельная, чтобы ручные ответы основной
фикстуры не пришлось пересчитывать ради одного класса.
"""

import json
import pathlib
import subprocess


# Схема сырья: как в data/main_data/main_data_YYYY_MM.parquet
RAW_SCHEMA = (
    'TransDate Nullable(Date32), LocationId Nullable(String), ItemId Nullable(String),'
    ' ItemLocationId Nullable(UInt64), isMatrix UInt8, StockStartQty Float64,'
    ' StockEndQty Float64, SalesQty Float64, SalesAmount Float64,'
    ' LocationNetwork Nullable(String), LocationFormatTT Nullable(String),'
    ' ItemMeasure Nullable(String), ItemIdLevel1 Nullable(String),'
    ' ItemIdLevel2 Nullable(String), ItemIdLevel3 Nullable(String),'
    ' SalesByLevel1Qty Float64, SalesByLevel2Qty Float64, SalesByLevel3Qty Float64,'
    ' SalesByLocationAmount Float64'
)

PERIOD = ('2026-05-11', '2026-05-24')
COHORT_DATE = '2026-05-19'
DAYS = [f'2026-05-{day:02d}' for day in range(11, 25)]

GROUP = '100 СОБСТВЕННОЕ ПРОИЗВОДСТВО'
MAIN_SUBGROUP = 'КУЛИНАРИЯ,ГОТОВЫЕ БЛЮДА'
DISPUTED_SUBGROUP = 'НАПИТКИ'  # спорная подгруппа профиля

STORES = {
    'S1': {'network': 'РЕМИ', 'format': 'Формат 4'},
    'S2': {'network': 'РЕМИ', 'format': 'Формат 3'},
    'S3': {'network': 'ЭКОНОМЫЧ', 'format': 'Формат 5'},  # выручка ниже порога профиля
    'S9': {'network': 'РЕМИ', 'format': 'Формат 4'},      # только для фикстуры квадрантов
}

ITEMS = {
    'I1': {'measure': 'шт', 'level2': MAIN_SUBGROUP, 'level3': 'ГОРЯЧЕЕ'},
    'I2': {'measure': 'кг', 'level2': MAIN_SUBGROUP, 'level3': 'ГОРЯЧЕЕ'},  # та же подподгруппа, другая ветка
    'I3': {'measure': 'кг', 'level2': DISPUTED_SUBGROUP, 'level3': 'МОРСЫ'},
    'I4': {'measure': 'шт', 'level2': MAIN_SUBGROUP, 'level3': 'ВЫПЕЧКА'},
    'I5': {'measure': 'кг', 'level2': MAIN_SUBGROUP, 'level3': 'ВЫПЕЧКА'},
    **{item: {'measure': 'шт', 'level2': MAIN_SUBGROUP, 'level3': 'ГОРЯЧЕЕ'}
       for item in ('Q1', 'Q2', 'Q3', 'Q4')},
}

PAIR_IDS = {  # ItemLocationId — в выгрузке он уже есть, здесь задаётся явно
    ('S1', 'I1'): 101, ('S1', 'I2'): 102, ('S1', 'I3'): 103,
    ('S2', 'I1'): 201, ('S2', 'I4'): 204, ('S2', 'I5'): 205,
    ('S3', 'I1'): 301, ('S3', 'I2'): 302,
    ('S9', 'Q1'): 901, ('S9', 'Q2'): 902, ('S9', 'Q3'): 903, ('S9', 'Q4'): 904,
}

# Фикстура квадрантов спроса: своя выгрузка на четыре пары, по одной на квадрант
# Syntetos–Boylan. Отдельно от основной, чтобы не пересчитывать её ручные ответы
# ради одного класса. Все дни в матрице, остаток 10 шт — выше порога наличия,
# поэтому дней дефицита нет и оба варианта ряда совпадают.
#
# | пара | продажи по дням | ADI | CV² | квадрант |
# |---|---|---|---|---|
# | S9 × Q1 | 5 каждый день | 1.0 | 0.00 | гладкий |
# | S9 × Q2 | 1 и 9 через день | 1.0 | 0.69 | неровный |
# | S9 × Q3 | 5 через день | 2.0 | 0.00 | прерывистый |
# | S9 × Q4 | 1 и 9 через день, между ними ноль | 2.0 | 0.93 | комковатый |
QUADRANT_QTY = {
    'Q1': [5.0] * 14,
    'Q2': [1.0, 9.0] * 7,
    'Q3': [5.0, 0.0] * 7,
    'Q4': [1.0, 0.0, 9.0, 0.0] * 3 + [1.0, 0.0],
}

QUADRANT_EXPECTED = {'Q1': 'гладкий', 'Q2': 'неровный',
                     'Q3': 'прерывистый', 'Q4': 'комковатый'}

# Цена штуки — своя у Q4. При равном основании ABC пары разводит выручка,
# и проверить это можно только там, где порядок по выручке расходится
# с порядком по `ItemLocationId`: у Q3 и Q4 одинаковая частота продаж (7 дней),
# но Q4 дороже, хотя её номер больше.
QUADRANT_PRICE = {'Q1': 50.0, 'Q2': 50.0, 'Q3': 50.0, 'Q4': 100.0}


def _row(day, store, item, matrix, stock_start, stock_end, qty, amount):
    return {
        'TransDate': day,
        'LocationId': store,
        'ItemId': item,
        'ItemLocationId': PAIR_IDS[(store, item)],
        'isMatrix': matrix,
        'StockStartQty': stock_start,
        'StockEndQty': stock_end,
        'SalesQty': qty,
        'SalesAmount': amount,
        'LocationNetwork': STORES[store]['network'],
        'LocationFormatTT': STORES[store]['format'],
        'ItemMeasure': ITEMS[item]['measure'],
        'ItemIdLevel1': GROUP,
        'ItemIdLevel2': ITEMS[item]['level2'],
        'ItemIdLevel3': ITEMS[item]['level3'],
        # своды того же дня в анализе этапа 1 не участвуют, но колонки должны быть
        'SalesByLevel1Qty': qty,
        'SalesByLevel2Qty': qty,
        'SalesByLevel3Qty': qty,
        'SalesByLocationAmount': amount,
    }


def rows() -> list:
    """Строки выгрузки фикстуры."""
    data = []

    # S1 × I1 (шт): в матрице все 14 дней, продаётся каждый день.
    # 13 мая распродан к вечеру — дефицит. 15 мая возврат: продажи и выручка
    # отрицательные, остаток при этом верный.
    for index, day in enumerate(DAYS):
        stock_end = 0.0 if day == '2026-05-13' else 4.0
        qty, amount = (-2.0, -100.0) if day == '2026-05-15' else (3.0, 150.0)
        data.append(_row(day, 'S1', 'I1', 1, 5.0, stock_end, qty, amount))

    # S1 × I2 (кг, та же подподгруппа, что у I1): 14 мая к вечеру осталось 0.05 кг —
    # больше нуля, но ниже порога наличия 0.1 кг, то есть день дефицита
    for day in DAYS:
        stock_end = 0.05 if day == '2026-05-14' else 2.5
        data.append(_row(day, 'S1', 'I2', 1, 3.0, stock_end, 1.5, 300.0))

    # S1 × I3 (кг, спорная подгруппа): в середине жизни дыра — 16 мая строки нет
    # вовсе, 17 мая строка есть, но вне матрицы и с продажей. Оба дня — пропуски
    # ряда, и слой массивов должен вернуть на их место NaN, а не ноль
    for day in DAYS:
        if day == '2026-05-16':
            continue
        if day == '2026-05-17':
            data.append(_row(day, 'S1', 'I3', 0, 1.0, 0.8, 0.2, 40.0))
        else:
            data.append(_row(day, 'S1', 'I3', 1, 1.0, 0.8, 0.2, 40.0))

    # S2 × I1: в матрице все дни, но активность (продажа или остаток) — только
    # с 15 по 19 мая. Дни до и после — матричные строки без товара: жизнь пары
    # ими не продлевается, в доле нулей и дефиците они не участвуют
    for day in DAYS:
        active = '2026-05-15' <= day <= '2026-05-19'
        if active:
            data.append(_row(day, 'S2', 'I1', 1, 2.0, 1.0, 1.0, 50.0))
        else:
            data.append(_row(day, 'S2', 'I1', 1, 0.0, 0.0, 0.0, 0.0))

    # S2 × I4 (шт): ни одного матричного дня, но продажи есть — так в выгрузке
    # тоже бывает. Класс спроса и дефицит по такой паре считать не из чего
    for day in ('2026-05-12', '2026-05-13', '2026-05-20'):
        data.append(_row(day, 'S2', 'I4', 0, 0.0, 0.0, 2.0, 200.0))

    # S2 × I5 (кг): пустой ряд — матричные дни есть, продаж и остатков нет
    for day in DAYS:
        data.append(_row(day, 'S2', 'I5', 1, 0.0, 0.0, 0.0, 0.0))

    # S3 × I1: пара появилась 20 мая, то есть после даты когорты
    for day in DAYS[9:]:
        data.append(_row(day, 'S3', 'I1', 1, 1.0, 1.0, 1.0, 45.0))

    # S3 × I2: второй ряд того же магазина. Выручка S3 остаётся ниже порога,
    # по которому магазин помечается как выбивающийся из общей массы
    for day in DAYS[9:]:
        data.append(_row(day, 'S3', 'I2', 1, 0.5, 0.4, 0.1, 25.0))

    return data


def quadrant_rows() -> list:
    """Строки выгрузки фикстуры квадрантов: четыре пары одного магазина."""
    return [_row(day, 'S9', item, 1, 20.0, 10.0, qty, qty * QUADRANT_PRICE[item])
            for item, quantities in QUADRANT_QTY.items()
            for day, qty in zip(DAYS, quantities)]


def _json_lines(data) -> str:
    """JSONEachRow вручную: без pandas и без зависимостей, кроме стандартных."""
    return '\n'.join(json.dumps(row, ensure_ascii=False) for row in data)


def write_parquet(data, save_file) -> pathlib.Path:
    """Записать строки в parquet в схеме сырья через `clickhouse-local`."""
    save_file = pathlib.Path(save_file)
    save_file.parent.mkdir(parents=True, exist_ok=True)
    if save_file.exists():
        save_file.unlink()

    query = (f"select * from format(JSONEachRow, $schema${RAW_SCHEMA}$schema$, $data${_json_lines(data)}$data$)"
             f" into outfile '{save_file}' format Parquet")
    result = subprocess.run(['clickhouse-local', '--query', query],
                            capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"не собралась фикстура {save_file}: {result.stderr}")
    return save_file


def write_main_data(main_data_dir) -> pathlib.Path:
    """Фикстура выгрузки: файл месяца, как в `data/main_data/`."""
    return write_parquet(rows(), pathlib.Path(main_data_dir) / 'main_data_2026_05.parquet')
