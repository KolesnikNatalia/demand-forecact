"""
Проверки профиля рядов: ключ, полнота, суммы, «неприменимо».

Профиль — это то, по чему дальше фильтруют, сегментируют и строят все сводки
отчёта. Ошибка в нём не видна глазами: цифры останутся правдоподобными.
Поэтому проверки уровня error останавливают прогон.

Что держится проверками:

- **ключ** «пара × вариант» уникален, и вариантов ровно два на пару;
- **все пары выгрузки на месте**, а продажи и выручка по ветке равны сырью —
  сверка идёт с тем же источником, из которого построен слой этапа 1;
- **заполненность**: у непустой пары есть ветка, подгруппа, когорта, класс
  спроса, доля дефицита и шесть ABC-классов;
- **«неприменимо» стоит там, где нечего считать**: у пустого ряда — везде,
  у пары без матричных дней — в классе спроса и в ABC по частоте. Обратное
  тоже проверяется: «неприменимо» не должно появляться там, где данные есть;
- **жизнь пары** в профиле совпадает с `series_pairs.parquet` этапа 1: профиль
  выводит её из флага `InLife`, и это тождество должно выполняться, а не
  подразумеваться.

Журнал, отчёт и остановку прогона делает общий модуль `checks_journal`.
"""

import pathlib
import sys

sys.path.append(f"{pathlib.Path(__file__).resolve().parents[1]}/lib")  # модули src/lib
import checks_journal
import clickhouse
import sql


SUM_TOLERANCE = 1e-9  # относительный допуск сравнения сумм профиля и сырья

def _required(cfg: dict) -> dict:
    """
    Колонки, которые у непустой пары обязаны быть заполнены.

    Значение — тройка «условие «не заполнено», название для отчёта, зерно».
    Зерно важно для текста замечания: в профиле две строки на пару, и счётчик
    без фильтра по варианту назвал бы вдвое больше пар, чем их есть. Поэтому
    свойства пары считаются по одному варианту (`pair`), а то, что различается
    между вариантами, — по строкам (`row`).

    Ожидаемые значения берутся из `cfg`, а не пишутся здесь литералами: слово
    «неприменимо» и список квадрантов задаёт шаг, и копия здесь молча разошлась
    бы с ним — проверка продолжила бы говорить «ок», перестав что-либо ловить.
    """
    not_applicable = sql.literal(cfg['not_applicable'])
    return {
        'branch': ("Branch = '' or Branch = 'unknown'", 'ветка', 'pair'),
        'subgroup': ("ItemIdLevel2 = ''", 'подгруппа', 'pair'),
        'cohort': (f"Cohort = '' or Cohort = {not_applicable}", 'когорта', 'pair'),
        # класс спроса считается по дням варианта, поэтому проверяется построчно.
        # Условие — «значение не из перечня», а не «пустая строка»: multiIf всегда
        # возвращает непустой литерал, и проверка на '' не ловила бы ничего
        'demand_class': (f"Quadrant not in ({sql.quoted(cfg['quadrants'])})",
                         'класс спроса', 'row'),
        'deficit': ("MatrixDays > 0 and isNull(DeficitDayShare)", 'доля дней дефицита', 'pair'),
    }


# Зерно счётчика → условие отбора строк и слово для отчёта
GRAIN = {'pair': ("Variant = 'sales'", 'пар'), 'row': ('1 = 1', 'строк')}


def _totals_branch(source_cte: str, branch_case: str, branches, base_file, paths) -> pathlib.Path:
    """
    Суммы и число пар: профиль против сырья, по ветке.

    Перечень веток берётся из профиля параметров, а не из данных: иначе
    у пустого результата не было бы ни одной строки сравнения, и прогон
    с неверным периодом «прошёл» бы, не сверив ни одной суммы.

    Из профиля берётся один вариант ряда: выручка и количество — свойства пары,
    и в обеих её строках они одинаковы.
    """
    save_file = paths.checks / 'profile_checks_branch.parquet'
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
                , toUInt64(uniqExact(ItemLocationId)) as SrcPairs
            from t_Src
            group by SrcBranch
        )
        , t_ProfAgg as (
            select
                Branch as ProfBranch
                , sum(SalesQtyTotal) as ProfQty
                , sum(Revenue) as ProfAmount
                , toUInt64(uniqExact(ItemLocationId)) as ProfPairs
            from file('{base_file}')
            where Variant = 'sales'
            group by ProfBranch
        )
        -- join_use_nulls = 0: ветка без строк даёт нули, а не NULL, поэтому
        -- пропавшая в профиле ветка видна как расхождение с сырьём
        select
            b.Branch as Branch
            , p.ProfQty as Qty, s.SrcQty as SrcQty
            , p.ProfAmount as Amount, s.SrcAmount as SrcAmount
            , p.ProfPairs as Pairs, s.SrcPairs as SrcPairs
        from t_Branch as b
        left join t_ProfAgg as p on p.ProfBranch = b.Branch
        left join t_SrcAgg as s on s.SrcBranch = b.Branch
        order by Branch
        {sql.PARQUET_SETTINGS}
    """
    clickhouse.exec_local(command, paths.tmp / 'profile_checks_branch.sql', save_file, 'Parquet')
    return save_file


def _totals_all(base_file, series_pairs_file, cfg: dict, paths) -> pathlib.Path:
    """Счётчики по всему профилю: ключ, заполненность, «неприменимо», жизнь пары."""
    save_file = paths.checks / 'profile_checks_all.parquet'
    abc = cfg['abc_columns']
    not_applicable = sql.literal(cfg['not_applicable'])

    # «не заполнен хотя бы один из шести» и «хоть один класс проставлен»
    abc_missing = ' or '.join(f"{name} = ''" for name in abc)
    abc_applied = ' or '.join(f"{name} != {not_applicable}" for name in abc)
    # частота неприменима без матричных дней, остальные основания — применимы
    abc_freq_applied = ' or '.join(f"{name} != {not_applicable}"
                                   for name in abc if name.startswith('AbcFreq'))
    missing = '\n            '.join(
        f", toUInt64(countIf({GRAIN[grain][0]} and IsEmpty = 0 and ({expr}))) as Missing_{key}"
        for key, (expr, _, grain) in _required(cfg).items())

    command = f"""select
            toUInt64(count()) as Rows
            , toUInt64(uniqExact((ItemLocationId, Variant))) as Keys
            , toUInt64(uniqExact(ItemLocationId)) as Pairs
            , toUInt64(uniqExact(Variant)) as Variants
            {missing}
            -- шесть ABC — свойство пары, поэтому считаются по одному варианту
            , toUInt64(countIf(Variant = 'sales' and IsEmpty = 0
                               and ({abc_missing}))) as MissingAbc
            -- пустой ряд: истории нет, значит нет ни класса спроса, ни ABC
            , toUInt64(countIf(IsEmpty = 1
                               and (Quadrant != {not_applicable} or ({abc_applied})))) as EmptyApplied
            -- пара без матричных дней: долю нулей считать не из чего
            , toUInt64(countIf(IsEmpty = 0 and MatrixDays = 0
                               and (Quadrant != {not_applicable}
                                    or ({abc_freq_applied})))) as NoMatrixApplied
            -- обратная сторона: «неприменимо» там, где дни варианта есть
            , toUInt64(countIf(VariantDays > 1 and SalesDays > 1
                               and Quadrant = {not_applicable})) as ApplicableSkipped
            , toUInt64(countIf(Variant = 'sales' and IsEmpty = 1)) as EmptyPairs
            , toUInt64(countIf(Variant = 'sales' and IsEmpty = 0
                               and MatrixDays = 0)) as NoMatrixPairs
            , toUInt64(countIf(Quadrant = {not_applicable} and IsEmpty = 0)) as NotApplicableRows
            , (
                -- профиль выводит жизнь пары из флага InLife, а слой этапа 1
                -- считал её по первой и последней активности. Признак совпадения —
                -- отдельная колонка PairFound: при join_use_nulls = 0 непришедшая
                -- строка даёт нули, а не NULL, и отличить её иначе нельзя
                select toUInt64(countIf(sp.PairFound = 0
                                        or pr.FirstActive != sp.PairFirst
                                        or pr.LastActive != sp.PairLast))
                from (
                    select ItemLocationId, FirstActive, LastActive
                    from file('{base_file}')
                    where Variant = 'sales' and IsEmpty = 0
                ) as pr
                left join (
                    select
                        ItemLocationId as PairId
                        , toUInt8(1) as PairFound
                        , FirstActive as PairFirst
                        , LastActive as PairLast
                    from file('{series_pairs_file}')
                    where IsEmpty = 0
                ) as sp on sp.PairId = pr.ItemLocationId
            ) as LifeMismatch
        from file('{base_file}')
        {sql.PARQUET_SETTINGS}
    """
    clickhouse.exec_local(command, paths.tmp / 'profile_checks_all.sql', save_file, 'Parquet')
    return save_file


def _preamble(branch_file, all_file) -> str:
    """Посчитанные суммы и счётчики как CTE: журнал выбирает из них строки проверок."""
    return f"""t_B as (
            select * from file('{branch_file}')
        )
        , t_A as (
            select * from file('{all_file}')
        )
    """


def _body(cfg: dict) -> str:
    """Строки журнала проверок из посчитанных сумм и счётчиков."""
    tolerance = f"toFloat64({SUM_TOLERANCE})"
    parts = [f"""
            select
                concat('profile_sales_qty_', Branch) as CheckName
                , 'error' as Level
                , toFloat64(abs(Qty - SrcQty) / greatest(abs(SrcQty), 1)) as Value
                , {tolerance} as Threshold
                , concat('продажи ветки: профиль ', toString(Qty), ', сырьё ', toString(SrcQty)) as Details
            from t_B
            union all
            select
                concat('profile_sales_amount_', Branch)
                , 'error'
                , toFloat64(abs(Amount - SrcAmount) / greatest(abs(SrcAmount), 1))
                , {tolerance}
                , concat('выручка ветки: профиль ', toString(Amount), ', сырьё ', toString(SrcAmount))
            from t_B
            union all
            select
                concat('profile_pairs_', Branch)
                , 'error'
                , toFloat64(abs(toInt64(Pairs) - toInt64(SrcPairs)))
                , toFloat64(0)
                , concat('пар ветки: профиль ', toString(Pairs), ', сырьё ', toString(SrcPairs))
            from t_B"""]

    def row(name, level, value, details):
        return f"""
            select
                '{name}'
                , '{level}'
                , toFloat64({value})
                , toFloat64(0)
                , {details}
            from t_A"""

    parts.append(row('profile_rows_positive', 'error', 'Rows = 0',
                     """concat('строк в профиле: ', toString(Rows),
                         '. Пустой профиль — это период профиля мимо данных или непосчитанный этап 1')"""))
    parts.append(row('profile_key_unique', 'error', 'toInt64(Rows) - toInt64(Keys)',
                     """concat('строк ', toString(Rows), ', ключей «пара × вариант» ', toString(Keys))"""))
    parts.append(row('profile_two_variants', 'error',
                     'abs(toInt64(Rows) - toInt64(Pairs) * 2) + abs(toInt64(Variants) - 2)',
                     """concat('строк ', toString(Rows), ' при ', toString(Pairs), ' парах и ',
                         toString(Variants), ' вариантах: нужна ровно одна строка на пару и вариант')"""))

    for key, (_, title, grain) in _required(cfg).items():
        unit = GRAIN[grain][1]
        parts.append(row(f'profile_filled_{key}', 'error', f'Missing_{key}',
                         f"""concat('непустых {unit} без значения «{title}»: ', toString(Missing_{key}))"""))

    parts.append(row('profile_filled_abc', 'error', 'MissingAbc',
                     """concat('непустых пар, у которых пуст хотя бы один из шести ABC-классов: ',
                         toString(MissingAbc))"""))
    parts.append(row('profile_empty_not_applicable', 'error', 'EmptyApplied',
                     """concat('строк пустых рядов с проставленным классом: ', toString(EmptyApplied),
                         '. У пустого ряда нет истории, значит нет ни класса спроса, ни ABC')"""))
    parts.append(row('profile_no_matrix_not_applicable', 'error', 'NoMatrixApplied',
                     """concat('строк пар без матричных дней с проставленным классом: ',
                         toString(NoMatrixApplied), '. Долю нулей и частоту продаж считать не из чего')"""))
    parts.append(row('profile_applicable_not_skipped', 'error', 'ApplicableSkipped',
                     """concat('рядов с днями варианта, у которых класс спроса «неприменимо»: ',
                         toString(ApplicableSkipped))"""))
    parts.append(row('profile_life_matches_pairs', 'error', 'LifeMismatch',
                     """concat('пар, у которых жизнь в профиле разошлась со слоем этапа 1: ',
                         toString(LifeMismatch))"""))
    parts.append(row('profile_empty_pairs', 'warning', 'EmptyPairs',
                     """concat('пустых рядов: ', toString(EmptyPairs),
                         '. Они помечены, в ABC и сводках не участвуют')"""))
    parts.append(row('profile_no_matrix_pairs', 'warning', 'NoMatrixPairs',
                     """concat('непустых пар без единого матричного дня: ', toString(NoMatrixPairs),
                         '. Класс спроса и ABC по частоте у них «неприменимо»')"""))
    parts.append(row('profile_demand_class_not_applicable', 'warning', 'NotApplicableRows',
                     """concat('строк непустых пар без класса спроса: ', toString(NotApplicableRows),
                         '. Чаще всего это пары без матричных дней и ряды с единственной продажей')"""))

    return '\n            union all'.join(parts)


def check_profile(files: dict, cfg: dict, paths) -> list:
    """
    Проверить профиль рядов.

    `files` — пути результатов шага, `cfg` — разобранный профиль параметров:
    тот же источник, из которого построен слой этапа 1, и его ветки.

    Возвращает строки журнала. На `error` бросает исключение: следующие этапы
    не должны получить сломанный профиль.
    """
    branches = list(cfg['branches'])
    branch_case = sql.branch_case(cfg['branches'], 'ItemMeasure')

    branch_file = _totals_branch(cfg['source_cte'], branch_case, branches, files['base'], paths)
    all_file = _totals_all(files['base'], files['series_pairs'], cfg, paths)

    return checks_journal.run(
        'series_profile', 'Проверки профиля рядов',
        _preamble(branch_file, all_file), _body(cfg),
        paths, 'профиль рядов не прошёл проверки')
