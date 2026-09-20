"""
Проверки слоя дневных рядов: ключ, суммы, ветки, слой массивов.

Проверка не чинит данные, а останавливает прогон: расчёт на сломанном слое
дал бы правдоподобные цифры, по которым потом принимали бы решения.

Каждая проверка формулируется одинаково — «значение не больше порога», поэтому
статус считается общим правилом, а строки складываются в один журнал:

| колонка | что |
|---|---|
| `CheckName` | имя проверки |
| `Level` | `error` — останавливает прогон, `warning` — только запись в лог |
| `Value`, `Threshold` | измеренное значение и порог |
| `Status` | `ok`, `error` или `warning` |
| `Details` | числа, по которым видно, что именно разошлось |
| `RunAt` | время прогона, местное, строкой (parquet хранит `DateTime` в UTC) |

Журнал — файл на прогон в `data/checks/`: parquet не дописывают, а история
нужна, поэтому старые файлы остаются и читаются маской
`series_days_checks_*.parquet`.

Суммы сравниваются с допуском: `Float64`, сложенные в другом порядке, расходятся
в последних битах (в сырье есть `0.8260000000000001`).
"""

import datetime
import json
import pathlib
import sys

sys.path.append(f"{pathlib.Path(__file__).resolve().parents[1]}/lib")  # модули src/lib
import clickhouse
import sql
from logger import logger


SUM_TOLERANCE = 1e-9  # относительный допуск сравнения сумм слоя и сырья

EXAMPLES_IN_DETAILS = 10  # сколько позиций показать в замечании, чтобы было с чего начать разбор


def _totals_branch(source_cte: str, branch_case: str, branches, days_file, paths) -> pathlib.Path:
    """
    Суммы слоя и сырья по ветке. Выносится в файл: иначе CTE считается заново.

    Перечень веток берётся из профиля, а не из слоя. Если строить его по слою,
    то у пустого слоя не будет ни одной строки сравнения, и прогон с неверным
    периодом «пройдёт» все проверки, не сверив ни одной суммы.
    """
    save_file = paths.checks / 'series_checks_branch.parquet'
    command = f"""with t_Branch as (
            select arrayJoin([{sql.quoted(branches)}]) as Branch
        )
        , t_Src as (
            {source_cte}
        )
        , t_SrcAgg as (
            select
                {branch_case} as SrcBranch
                , sum(SalesQty) as SrcQty
                , sum(SalesAmount) as SrcAmount
                , toUInt64(count()) as SrcRows
            from t_Src
            group by SrcBranch
        )
        , t_DayAgg as (
            select
                Branch as DayBranch
                , sum(SalesQty) as DayQty
                , sum(SalesAmount) as DayAmount
                , toUInt64(count()) as DayRows
            from file('{days_file}')
            group by DayBranch
        )
        -- join_use_nulls = 0: ветка без строк даёт нули, а не NULL, поэтому
        -- пропавшая в слое ветка видна как расхождение с сырьём
        select
            b.Branch as Branch
            , d.DayQty as Qty, s.SrcQty as SrcQty
            , d.DayAmount as Amount, s.SrcAmount as SrcAmount
            , d.DayRows as Rows, s.SrcRows as SrcRows
        from t_Branch as b
        left join t_DayAgg as d on d.DayBranch = b.Branch
        left join t_SrcAgg as s on s.SrcBranch = b.Branch
        order by Branch
        {sql.PARQUET_SETTINGS}
    """
    clickhouse.exec_local(command, paths.tmp / 'series_checks_branch.sql', save_file, 'Parquet')
    return save_file


def _totals_source(main_glob, period, measures, paths) -> pathlib.Path:
    """
    Единицы измерения сырья против справочника профиля.

    Ряд с незнакомой единицей в слой не попадает: ветку такому товару назначает
    человек, правкой профиля. Поэтому важно видеть, сколько строк так отсеяно —
    иначе товар исчезнет из анализа молча.
    """
    save_file = paths.checks / 'series_checks_source.parquet'
    period_filter = (f"TransDate between toDate({sql.literal(period['start'])})"
                     f" and toDate({sql.literal(period['end'])})")
    unknown = f"trim(ifNull(ItemMeasure, '')) not in ({sql.quoted(measures)})"

    command = f"""select
            toUInt64(count()) as SrcRowsAll
            , toUInt64(countIf({unknown})) as UnknownRows
            , toUInt64(uniqExactIf(ItemId, {unknown})) as UnknownItems
            , arrayStringConcat(arraySort(groupUniqArrayIf(trim(ifNull(ItemMeasure, '')),
                                                           {unknown})), ', ') as UnknownMeasures
            -- по каким позициям это случилось: без примеров замечание нечем разбирать
            , arrayStringConcat(arraySlice(arraySort(groupUniqArrayIf(
                  concat(ifNull(ItemId, ''), ' (', trim(ifNull(ItemMeasure, '')), ')'),
                  {unknown})), 1, {EXAMPLES_IN_DETAILS}), ', ') as UnknownExamples
        from file('{main_glob}')
        where {period_filter}
        {sql.PARQUET_SETTINGS}
    """
    clickhouse.exec_local(command, paths.tmp / 'series_checks_source.sql', save_file, 'Parquet')
    return save_file


def _totals_all(days_file, arrays_file, paths) -> pathlib.Path:
    """Счётчики по всему слою: ключ, ветки, возвраты, длины массивов."""
    save_file = paths.checks / 'series_checks_all.parquet'
    command = f"""select
            toUInt64(count()) as Rows
            , toUInt64(uniqExact((TransDate, ItemLocationId))) as Keys
            , toUInt64(countIf(Branch = 'unknown')) as UnknownBranch
            , toUInt64(countIf(IsReturn = 1)) as ReturnRows
            , (
                select toUInt64(count())
                from (
                    select ItemLocationId
                    from file('{days_file}')
                    group by ItemLocationId
                    having uniqExact(Branch) > 1
                )
            ) as MultiBranchPairs
            , (
                select toUInt64(countIf(length(Values) != dateDiff('day', FirstDay, LastDay) + 1))
                from file('{arrays_file}')
            ) as ArrayBadLength
            , (
                select toUInt64(countIf(length(Values) != length(Deficit)))
                from file('{arrays_file}')
            ) as ArrayBadDeficit
            , (
                -- в варианте «продажи как есть» наблюдённых точек столько же,
                -- сколько у пары матричных дней жизни: остальное в массиве — NaN.
                -- Признак совпадения — отдельная колонка ArrFound: при
                -- join_use_nulls = 0 непришедшая строка даёт нули, а не NULL,
                -- и по самому ArrPoints пару без ряда от пары с нулём не отличить
                select toUInt64(countIf(a.ArrFound = 0 or a.ArrPoints != d.MatrixLifeDays))
                from (
                    select ItemLocationId, toInt64(countIf(IsMatrix = 1 and InLife = 1)) as MatrixLifeDays
                    from file('{days_file}')
                    where IsEmpty = 0
                    group by ItemLocationId
                ) as d
                left join (
                    select
                        ItemLocationId as ArrId
                        , toUInt8(1) as ArrFound
                        , toInt64(arrayCount(v -> not isNaN(v), Values)) as ArrPoints
                    from file('{arrays_file}')
                    where Variant = 'sales'
                ) as a on a.ArrId = d.ItemLocationId
            ) as ArrayBadPoints
        from file('{days_file}')
        {sql.PARQUET_SETTINGS}
    """
    clickhouse.exec_local(command, paths.tmp / 'series_checks_all.sql', save_file, 'Parquet')
    return save_file


def _checks_sql(branch_file, all_file, source_file, run_at) -> str:
    """Строки журнала проверок из посчитанных сумм и счётчиков."""
    tolerance = f"toFloat64({SUM_TOLERANCE})"
    return f"""with t_B as (
            select * from file('{branch_file}')
        )
        , t_A as (
            select * from file('{all_file}')
        )
        , t_S as (
            select * from file('{source_file}')
        )
        select
            CheckName
            , Level
            , Value
            , Threshold
            , if(Value <= Threshold, 'ok', Level) as Status
            , Details
            -- время прогона строкой: parquet хранит DateTime в UTC, и при чтении
            -- журнала время разъезжалось бы с логом и с именем файла
            , {run_at} as RunAt
        from (
            select
                concat('days_sales_qty_', Branch) as CheckName
                , 'error' as Level
                , toFloat64(abs(Qty - SrcQty) / greatest(abs(SrcQty), 1)) as Value
                , {tolerance} as Threshold
                , concat('продажи ветки: слой ', toString(Qty), ', сырьё ', toString(SrcQty)) as Details
            from t_B
            union all
            select
                concat('days_sales_amount_', Branch)
                , 'error'
                , toFloat64(abs(Amount - SrcAmount) / greatest(abs(SrcAmount), 1))
                , {tolerance}
                , concat('выручка ветки: слой ', toString(Amount), ', сырьё ', toString(SrcAmount))
            from t_B
            union all
            select
                concat('days_rows_', Branch)
                , 'error'
                , toFloat64(abs(toInt64(Rows) - toInt64(SrcRows)))
                , toFloat64(0)
                , concat('строк ветки: слой ', toString(Rows), ', сырьё ', toString(SrcRows))
            from t_B
            union all
            select
                -- сверка со всей выгрузкой за период, а не только с той её частью,
                -- из которой строился слой: иначе потерю строк видно не было бы —
                -- сумма сошлась бы сама с собой
                'days_rows_total'
                , 'error'
                , toFloat64(abs(toInt64(Rows) + toInt64(UnknownRows) - toInt64(SrcRowsAll)))
                , toFloat64(0)
                , concat('строк: слой ', toString(Rows), ' + вне профиля ', toString(UnknownRows)
                       , ' против выгрузки за период ', toString(SrcRowsAll))
            from t_A, t_S
            union all
            select
                'source_measures_known'
                , 'warning'
                , toFloat64(UnknownRows)
                , toFloat64(0)
                , concat('строк с единицей вне профиля: ', toString(UnknownRows)
                       , ', товаров: ', toString(UnknownItems)
                       , ', единицы: ', if(UnknownMeasures = '', '—', UnknownMeasures)
                       , if(UnknownExamples = '', '', concat('. Товары: ', UnknownExamples))
                       , '. Такие ряды в слой не попадают: ветку задаёт раздел branches профиля')
            from t_S
            union all
            select
                'days_rows_positive'
                , 'error'
                , toFloat64(Rows = 0)
                , toFloat64(0)
                , concat('строк в слое: ', toString(Rows),
                         '. Пустой слой — это период профиля мимо данных или пустой каталог выгрузки')
            from t_A
            union all
            select
                'days_key_unique'
                , 'error'
                , toFloat64(toInt64(Rows) - toInt64(Keys))
                , toFloat64(0)
                , concat('строк ', toString(Rows), ', ключей «день × пара» ', toString(Keys))
            from t_A
            union all
            select
                -- инвариант слоя, а не фильтр: строки с единицей вне профиля
                -- отсекает источник, и сюда они дойти не должны. Проверка держит
                -- это свойство на случай, если источник перестанет фильтровать
                'days_branch_known'
                , 'error'
                , toFloat64(UnknownBranch)
                , toFloat64(0)
                , concat('строк без ветки в слое: ', toString(UnknownBranch)
                       , '. Сколько строк отсеял источник, показывает source_measures_known')
            from t_A
            union all
            select
                'days_pair_one_branch'
                , 'error'
                , toFloat64(MultiBranchPairs)
                , toFloat64(0)
                , concat('пар, у которых больше одной ветки: ', toString(MultiBranchPairs))
            from t_A
            union all
            select
                'arrays_length_life'
                , 'error'
                , toFloat64(ArrayBadLength)
                , toFloat64(0)
                , concat('рядов, где длина массива не равна числу дней жизни: ', toString(ArrayBadLength))
            from t_A
            union all
            select
                'arrays_deficit_length'
                , 'error'
                , toFloat64(ArrayBadDeficit)
                , toFloat64(0)
                , concat('рядов, где массив дефицита другой длины: ', toString(ArrayBadDeficit))
            from t_A
            union all
            select
                'arrays_points_match_days'
                , 'error'
                , toFloat64(ArrayBadPoints)
                , toFloat64(0)
                , concat('пар, где наблюдённых точек не столько, сколько матричных дней жизни: ',
                         toString(ArrayBadPoints))
            from t_A
            union all
            select
                'days_returns_zeroed'
                , 'warning'
                , toFloat64(ReturnRows)
                , toFloat64(0)
                , concat('строк с обнулённым возвратом: ', toString(ReturnRows))
            from t_A
        )
        -- сначала то, из-за чего прогон встал, потом предупреждения, потом ok
        order by multiIf(Status = 'error', 0, Status = 'warning', 1, 2), CheckName
        {sql.PARQUET_SETTINGS}
    """


def check_days(files: dict, cfg: dict, paths) -> list:
    """
    Проверить слой дневных рядов и слой массивов.

    `files` — пути результатов шага, `cfg` — разобранный профиль: тот же источник,
    из которого слой построен, ветки и единицы измерения. Ветки берутся из профиля,
    а не из слоя, чтобы проверка не исчезала вместе с пропавшими данными.

    Возвращает строки журнала. На `error` бросает исключение: следующие шаги
    не должны получить сломанный слой.
    """
    branches = list(cfg['branches'])
    branch_case = sql.branch_case(cfg['branches'], 'ItemMeasure')

    branch_file = _totals_branch(cfg['source_cte'], branch_case, branches, files['days'], paths)
    all_file = _totals_all(files['days'], files['arrays'], paths)
    source_file = _totals_source(cfg['main_glob'], cfg['period'], cfg['measures'], paths)

    # микросекунды в имени: два прогона подряд не должны затирать друг друга,
    # журнал — это история проверок
    started = datetime.datetime.now()
    stamp = started.strftime('%Y%m%d_%H%M%S_%f')
    journal_file = paths.checks / f'series_days_checks_{stamp}.parquet'
    clickhouse.exec_local(_checks_sql(branch_file, all_file, source_file,
                                      sql.literal(started.strftime('%Y-%m-%d %H:%M:%S'))),
                          paths.tmp / 'series_checks.sql', journal_file, 'Parquet')

    rows = _read(journal_file, paths)
    report_file = _write_report(rows, journal_file, paths)
    _report(rows, report_file)
    return rows


def _write_report(rows: list, journal_file, paths) -> pathlib.Path:
    """
    Отчёт прогона рядом с журналом: замечания вверху, полная таблица ниже.

    Отдельный файл нужен потому, что в логе замечание про одну позицию теряется
    среди сообщений шага, а разбирать его будет человек — возможно, назавтра.
    Таблицу готовит сам ClickHouse (`FORMAT Markdown`) из журнала прогона.
    """
    report_file = journal_file.with_suffix('.md')
    table = clickhouse.exec_local(
        f"""select CheckName as `Проверка`, Status as `Статус`, Value as `Значение`
                 , Threshold as `Порог`, Details as `Подробности`
            from file('{journal_file}') format Markdown""",
        paths.tmp / 'series_checks_report.sql', return_result=True)

    issues = [row for row in rows if row['Status'] != 'ok']
    lines = [f"# Проверки слоя дневных рядов — {rows[0]['RunAt'] if rows else ''}", '',
             f"Проверок {len(rows)}: ошибок {sum(r['Status'] == 'error' for r in rows)}, "
             f"замечаний {sum(r['Status'] == 'warning' for r in rows)}.", '']

    if issues:
        lines.append('## Замечания и ошибки')
        lines.append('')
        lines += [f"- **{row['CheckName']}** ({row['Status']}): {row['Details']}" for row in issues]
        lines.append('')
    else:
        lines += ['Замечаний нет.', '']

    lines += ['## Все проверки', '', table or '', '',
              f"Журнал прогона: `{journal_file}`", '']
    report_file.write_text('\n'.join(lines), encoding='utf-8')
    return report_file


def _read(journal_file, paths) -> list:
    """Прочитать журнал обратно: строк десяток, файл ради этого не нужен."""
    # порядок файла сохраняется: в нём сначала то, из-за чего прогон встал
    text = clickhouse.exec_local(
        f"select * from file('{journal_file}') format JSONEachRow",
        paths.tmp / 'series_checks_read.sql', return_result=True)
    return [json.loads(line) for line in (text or '').splitlines() if line.strip()]


def _report(rows: list, report_file):
    """
    Подвести итог прогона и остановить его, если есть error.

    Замечания собираются в один блок в конце, а не только идут построчно по ходу
    дела: отдельная позиция с незнакомой единицей расчёт не останавливает — иначе
    одна SKU оставила бы без прогноза всю сеть, — но и потеряться среди сообщений
    шага она не должна.
    """
    errors = [r for r in rows if r['Status'] == 'error']
    warnings = [r for r in rows if r['Status'] == 'warning']

    for row in rows:
        message = f"проверка {row['CheckName']}: {row['Status']} — {row['Details']}"
        (logger.error if row['Status'] == 'error' else
         logger.warning if row['Status'] == 'warning' else logger.info)(message)

    summary = [f"итог проверок: всего {len(rows)}, ошибок {len(errors)}, замечаний {len(warnings)}"]
    summary += [f"  [{row['Status']}] {row['CheckName']}: {row['Details']}"
                for row in errors + warnings]
    summary.append(f"  отчёт: {report_file}")
    # уровень warning, чтобы итог был виден в консоли, а не только в файле лога
    (logger.warning if errors or warnings else logger.info)('\n'.join(summary))

    if errors:
        details = '; '.join(f"{r['CheckName']}: {r['Details']}" for r in errors)
        raise ValueError(f"слой дневных рядов не прошёл проверки ({len(errors)}): {details}. "
                         f"Отчёт: {report_file}")
