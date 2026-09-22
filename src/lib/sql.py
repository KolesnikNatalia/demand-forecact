"""
Хелперы для сборки SQL: экранирование списков, выражения-справочники, источник сырья.

SQL шагов живёт в самих шагах — здесь только то, что нужно нескольким шагам сразу
и что иначе пришлось бы копировать: список значений для `IN`, перевод справочника
из профиля в `multiIf`, чтение выгрузки `main_data`.

    import sql

    sql.quoted(['кг', 'шт'])                 -- 'кг', 'шт'
    sql.case('Branch', {'кг': 0.1, 'шт': 1})  -- multiIf(Branch = 'кг', 0.1, ...)
    sql.source_select(main_glob, start, end, 'zero', ['кг', 'шт'])
"""


# Сегменты ассортимента: спорные подгруппы идут в сводках отдельной строкой
# и в основные итоги не входят. Значения заданы здесь, а не литералами по месту:
# слой их проставляет, а построитель отчёта по ним сортирует и считает итоги,
# и разойтись эти две стороны не должны
SCOPE_MAIN = 'основные'
SCOPE_DISPUTED = 'спорные'


# Выход шага — parquet со сжатием zstd: файлы читаются много раз,
# а место и время чтения экономит один и тот же набор настроек
PARQUET_SETTINGS = "settings output_format_parquet_compression_method = 'zstd'"


def literal(value) -> str:
    """Значение как строковый литерал SQL: кавычки и удвоение апострофа."""
    return "'" + str(value).replace("'", "''") + "'"


def quoted(values) -> str:
    """
    Список значений для условия `IN (...)`.

    Строка вместо списка — ошибка, а не список из её букв: в профиле легко
    написать `disputed_subgroups: НАПИТКИ` вместо `[НАПИТКИ]`, и тогда условие
    молча превратилось бы в перечень отдельных символов.
    """
    if isinstance(values, (str, bytes)):
        raise ValueError(f"ожидается список значений, а не строка {values!r}")
    return ', '.join(literal(v) for v in values)


def case(column: str, mapping: dict, default: str = "'unknown'") -> str:
    """
    Справочник из профиля в виде `multiIf` по значению колонки.

    Ключ словаря — значение колонки, значение — готовое SQL-выражение результата
    (строка в кавычках или число). Значение, которого нет в справочнике, получает
    `default`: молча подставить «первое попавшееся» нельзя.
    """
    parts = [f"{column} = {literal(key)}, {result}" for key, result in mapping.items()]
    if not parts:
        return default
    return "multiIf(" + ', '.join(parts) + f", {default})"


def branch_case(branches: dict, column: str = 'ItemMeasure') -> str:
    """
    Единица измерения → ветка расчёта (`{'кг': ['кг'], 'шт': ['шт']}`).

    Единица, которой нет в профиле, даёт ветку `unknown`. До слоя такие строки
    не доходят — `source_select` берёт только единицы профиля, — но выражение
    обязано быть определено на любом значении, а проверка `days_branch_known`
    держит этот инвариант на случай источника без фильтра.
    """
    parts = []
    for branch, measures in branches.items():
        if measures:
            parts.append(f"{column} in ({quoted(measures)}), {literal(branch)}")
    if not parts:
        raise ValueError('в профиле не задана ни одна ветка: раздел branches пуст')
    return "multiIf(" + ', '.join(parts) + ", 'unknown')"


def scope_case(disputed, column: str = 'ItemIdLevel2') -> str:
    """Подгруппа → `основные` или `спорные` (в основные итоги спорные не входят)."""
    if not disputed:
        return literal(SCOPE_MAIN)
    return f"if({column} in ({quoted(disputed)}), {literal(SCOPE_DISPUTED)}, {literal(SCOPE_MAIN)})"


def source_select(main_glob, start_date, end_date, returns: str, measures) -> str:
    """
    Источник — выгрузка как есть, строка в строку, с одной правкой: возвраты.

    - читается через алиас `src`: иначе алиас из списка выборки перекрывает
      исходную колонку в `WHERE`;
    - `Nullable` снимается сразу: на нём падают функции высшего порядка;
    - берутся только единицы измерения из профиля. Ряд без ветки в расчёт
      не идёт ([data_quality.md](../../docs/data_quality.md), разд. 2 и 3):
      ветку такому товару назначает человек, правкой профиля. Сколько строк
      так отсеяно, показывает проверка сырья;
    - возвраты (`SalesQty < 0`) — это продажи прошлых дней, а не спрос дня.
      При `returns = 'zero'` продажи и выручка обнуляются, строка остаётся:
      остаток и флаг матрицы в ней верные, а удаление дало бы ложный пропуск
      в ряду. Это же правило применяется к сырью в проверке сумм;
    - дубли ключа здесь не склеиваются: по величине не видно, какая строка
      верная. Их ловит отдельная проверка.
    """
    if returns != 'zero':
        raise ValueError(f"режим возвратов '{returns}' не поддерживается, ожидается 'zero'")

    return f"""select
                assumeNotNull(src.TransDate) as TransDate
                , assumeNotNull(src.ItemLocationId) as ItemLocationId
                , ifNull(src.LocationId, '') as LocationId
                , ifNull(src.ItemId, '') as ItemId
                , trim(ifNull(src.ItemMeasure, '')) as ItemMeasure
                , ifNull(src.ItemIdLevel2, '') as ItemIdLevel2
                , ifNull(src.ItemIdLevel3, '') as ItemIdLevel3
                , ifNull(src.LocationNetwork, '') as LocationNetwork
                , ifNull(src.LocationFormatTT, '') as LocationFormatTT
                , toUInt8(ifNull(src.isMatrix, 0)) as IsMatrix
                , toFloat64(ifNull(src.StockStartQty, 0)) as StockStartQty
                , toFloat64(ifNull(src.StockEndQty, 0)) as StockEndQty
                , if(toFloat64(ifNull(src.SalesQty, 0)) < 0, 0.0,
                     toFloat64(ifNull(src.SalesQty, 0))) as SalesQty
                , if(toFloat64(ifNull(src.SalesQty, 0)) < 0, 0.0,
                     toFloat64(ifNull(src.SalesAmount, 0))) as SalesAmount
                , toUInt8(toFloat64(ifNull(src.SalesQty, 0)) < 0) as IsReturn
            from file('{main_glob}') as src
            where src.TransDate between toDate({literal(start_date)}) and toDate({literal(end_date)})
              and trim(ifNull(src.ItemMeasure, '')) in ({quoted(measures)})"""
