"""
Дневные ряды «магазин × товар»: разметка, на которой держится весь анализ.

Этап 1 плана prd/plan-store-item-time-series-analysis.md. Шаг читает сырьё
`data/main_data/main_data_*.parquet` и пишет два файла слоя `prepared`:

- `series_days.parquet` — строка сырья плюс разметка: ветка `кг` / `шт`, матричный
  день, день дефицита, жизнь пары, когорта, значение ряда в двух вариантах;
- `series_arrays.parquet` — тот же ряд массивом по дням жизни пары, пропуск — `NaN`.
  Самому этапу 1 он не нужен, но его читают этапы 4, 7, 8 и 11, поэтому строится здесь.

Что здесь решено (подробности — docs/data.md, разд. 6, и prd/research-...md, разд. 8):

- **возвраты** (`SalesQty < 0`) обнуляются вместе с выручкой, строка остаётся;
- **дефицит** — матричный день жизни пары с вечерним остатком ниже порога наличия
  ветки, а не «ровно ноль»: у `кг` сотни тысяч строк с остатком меньше порции;
- **пустой ряд** — пара, у которой за весь период ни продажи, ни ненулевого остатка.
  Строки остаются в слое (чтобы сходились суммы), но помечены `IsEmpty`, и дальше
  в анализе такие пары не участвуют;
- **жизнь пары** — от первой до последней активности. Матричные дни вне этого
  отрезка — пропуски, как дни вне матрицы: в долю нулей, дефицит и доступность
  они не входят;
- **сетка не уплотняется.** Строки «всё по нулям» есть только при `isMatrix = 1`,
  поэтому отсутствие строки — это внематричный день, то есть пропуск. Позиции
  пропусков внутри жизни пары восстанавливает слой массивов (`WITH FILL`).

Запуск:

    uv run python src/preprocessing/series_days.py [--checks | --checks-only]
"""

import argparse
import pathlib
import sys
import time

_SRC = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(f"{_SRC}/lib")     # модули src/lib
sys.path.append(f"{_SRC}/checks")  # проверки слоёв
import clickhouse
import params
import series_checks
import settings
import sql
from logger import logger


PROFILE_FILE = settings.paths.root / 'profiles' / 'series_analysis.yaml'

MAIN_DATA_GLOB = 'main_data_*.parquet'  # маску задаёт шаг, а не settings.py


def _exec(command: str, name: str, save_file, paths) -> pathlib.Path:
    """Выполнить запрос в `clickhouse-local` и записать parquet.

    Файл SQL-команды остаётся в `data/tmp/`: по нему потом разбирают, что именно
    считал подшаг. Результат `exec_local` удаляет перед запуском, поэтому упавший
    шаг оставляет отсутствие файла, а не старые данные.
    """
    started = time.time()
    clickhouse.exec_local(command, paths.tmp / f'{name}.sql', save_file, 'Parquet')
    logger.info(f"series_days: {name} — {time.time() - started:.1f} c → {save_file}")
    return save_file


def _step_stores(source_cte: str, revenue_limit: float, paths) -> pathlib.Path:
    """
    Подшаг 1. Выручка магазина за период и флаг «выбивается из общей массы».

    В задачу входят все магазины, но у части из них выручка собственного
    производства — единицы процентов от обычной. В сводках по магазинам, сетям
    и форматам они должны быть видны отдельной строкой, поэтому флаг едет
    в каждой строке дневного слоя.
    """
    command = f"""with t_Src as (
            {source_cte}
        )
        select
            LocationId
            , any(LocationNetwork) as LocationNetwork
            , any(LocationFormatTT) as LocationFormatTT
            , sum(SalesAmount) as Revenue
            , toUInt8(sum(SalesAmount) < {revenue_limit}) as OutlierStore
        from t_Src
        group by LocationId
        order by LocationId
        {sql.PARQUET_SETTINGS}
    """
    return _exec(command, 'series_stores', paths.prepared / 'series_stores.parquet', paths)


def _step_pairs(source_cte: str, cohort_date, paths) -> pathlib.Path:
    """
    Подшаг 2. Пара «магазин × товар»: атрибуты, жизнь, пустой ряд, когорта.

    Активность — день, когда была продажа или ненулевой остаток. Первая и последняя
    активность задают жизнь пары; пара без единой активности — пустой ряд, у неё
    жизни нет и когорта «неприменимо».
    """
    active = 'SalesQty > 0 or StockStartQty > 0 or StockEndQty > 0'

    command = f"""with t_Src as (
            {source_cte}
        )
        select
            ItemLocationId
            , any(LocationId) as LocationId
            , any(ItemId) as ItemId
            , any(ItemIdLevel2) as ItemIdLevel2
            , any(ItemIdLevel3) as ItemIdLevel3
            , any(LocationNetwork) as LocationNetwork
            , any(LocationFormatTT) as LocationFormatTT
            , toUInt8(countIf({active}) = 0) as IsEmpty
            , if(IsEmpty = 1, null, minIf(TransDate, {active})) as FirstActive
            , if(IsEmpty = 1, null, maxIf(TransDate, {active})) as LastActive
            , multiIf(IsEmpty = 1, 'неприменимо'
                    , FirstActive < toDate('{cohort_date}'), 'до'
                    , 'после') as Cohort
            , toUInt32(count()) as SrcRows
            , toUInt32(countIf(IsMatrix = 1)) as MatrixDays
            , toUInt32(countIf(IsReturn = 1)) as ReturnRows
        from t_Src
        group by ItemLocationId
        order by ItemLocationId
        {sql.PARQUET_SETTINGS}
    """
    return _exec(command, 'series_pairs', paths.prepared / 'series_pairs.parquet', paths)


def _step_days(source_cte: str, pairs_file, stores_file, branch_case: str,
               scope_case: str, threshold_case: str, save_file, paths) -> pathlib.Path:
    """
    Подшаг 3. Дневной слой: строка сырья плюс разметка.

    Присоединяемые CTE отдают колонки под своими именами (`Pair*`, `Store*`):
    одноимённые колонки справа дают `AMBIGUOUS_IDENTIFIER` на `ON`, даже когда
    сам `ON` верен.
    """
    command = f"""with t_Src as (
            {source_cte}
        )
        , t_Pairs as (
            select
                ItemLocationId as PairId
                , ItemIdLevel2 as PairLevel2
                , ItemIdLevel3 as PairLevel3
                , LocationNetwork as PairNetwork
                , LocationFormatTT as PairFormat
                , IsEmpty as PairIsEmpty
                , FirstActive as PairFirstActive
                , LastActive as PairLastActive
                , Cohort as PairCohort
            from file('{pairs_file}')
        )
        , t_Stores as (
            select
                LocationId as StoreId
                , OutlierStore as StoreOutlier
            from file('{stores_file}')
        )
        select
            s.TransDate as TransDate
            , s.ItemLocationId as ItemLocationId
            , s.LocationId as LocationId
            , s.ItemId as ItemId
            , s.ItemMeasure as ItemMeasure
            , {branch_case} as Branch
            , p.PairLevel2 as ItemIdLevel2
            , p.PairLevel3 as ItemIdLevel3
            , p.PairNetwork as LocationNetwork
            , p.PairFormat as LocationFormatTT
            , {scope_case} as Scope
            , p.PairCohort as Cohort
            , toUInt8(st.StoreOutlier) as OutlierStore
            , toUInt8(p.PairIsEmpty) as IsEmpty
            , s.IsMatrix as IsMatrix
            -- жизнь пары: матричные дни вне неё — такие же пропуски, как дни вне матрицы
            , toUInt8(ifNull(p.PairIsEmpty = 0
                             and s.TransDate >= p.PairFirstActive
                             and s.TransDate <= p.PairLastActive, 0)) as InLife
            -- дефицит: матричный день жизни пары с остатком ниже порога наличия ветки.
            -- Ветка без порога в профиле роняет запрос (см. threshold_case): «порога
            -- нет» — это не «дефицита не было», и молча подставить ноль нельзя
            , toUInt8(s.IsMatrix = 1 and InLife = 1
                      and s.StockEndQty < {threshold_case}) as IsDeficit
            , s.StockStartQty as StockStartQty
            , s.StockEndQty as StockEndQty
            , s.SalesQty as SalesQty
            , s.SalesAmount as SalesAmount
            , s.IsReturn as IsReturn
            -- два варианта ряда. Пропуск — NULL: агрегаты ClickHouse его пропускают,
            -- поэтому «только матричные дни» и «без дней дефицита» не требуют
            -- отдельных веток кода дальше по анализу
            , if(s.IsMatrix = 1 and InLife = 1, s.SalesQty, null) as ValueSales
            , if(s.IsMatrix = 1 and InLife = 1 and IsDeficit = 0, s.SalesQty, null) as ValueDemand
        from t_Src as s
        left join t_Pairs as p on p.PairId = s.ItemLocationId
        left join t_Stores as st on st.StoreId = s.LocationId
        order by ItemLocationId, TransDate
        {sql.PARQUET_SETTINGS}
    """
    return _exec(command, 'series_days', save_file, paths)


def _step_arrays(days_file, save_file, paths) -> pathlib.Path:
    """
    Подшаг 4. Ряд массивом по дням жизни пары, пропуск — `NaN`.

    Дни внутри жизни, у которых нет строки в сырье, восстанавливает
    `ORDER BY ... WITH FILL`: заполнение идёт по префиксу сортировки, то есть
    внутри каждой пары. У заполненной строки `HasRow = 0`.

    `NaN`, а не `NULL`: массив читает Python, и `NaN` он берёт без маски.
    Внутри SQL этот массив агрегатами не считают — там пропуск остаётся `NULL`.
    `groupArray` порядок не гарантирует, поэтому массив сортируется явно.
    """
    command = f"""with t_Fill as (
            select
                ItemLocationId
                , TransDate
                , toUInt8(1) as HasRow
                , IsMatrix
                , IsDeficit
                , SalesQty
            from file('{days_file}')
            where IsEmpty = 0 and InLife = 1
            order by ItemLocationId, TransDate with fill step 1
        )
        select
            ItemLocationId
            , Variant
            , min(TransDate) as FirstDay
            , max(TransDate) as LastDay
            , arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((TransDate,
                  if(HasRow = 1 and IsMatrix = 1
                     and not (Variant = 'demand' and IsDeficit = 1), SalesQty, nan))))) as Values
            , arrayMap(x -> x.2, arraySort(x -> x.1, groupArray((TransDate, IsDeficit)))) as Deficit
        from t_Fill
        array join ['sales', 'demand'] as Variant
        group by ItemLocationId, Variant
        order by ItemLocationId, Variant
        {sql.PARQUET_SETTINGS}
    """
    return _exec(command, 'series_arrays', save_file, paths)


def run_settings(profile, paths) -> dict:
    """
    Разобрать профиль: то, что нужно и расчёту, и проверкам.

    Собирается в одном месте, чтобы проверки сверяли слой с тем же источником,
    из которого он построен, а не с похожим. По той же причине функция открытая:
    её зовут следующие шаги анализа — иначе у каждого был бы свой «почти такой же»
    источник, и расхождение сумм списали бы на него.
    """
    period = profile.section('period', {'start', 'end'})
    branches = profile.value('branches')
    measures = [measure for measure_list in branches.values() for measure in measure_list]

    life = profile.value('life')
    if life != 'active_span':
        raise ValueError(f"{profile.file}: life = '{life}' не поддерживается, ожидается 'active_span'")

    main_glob = paths.main_data / MAIN_DATA_GLOB
    return {
        'period': period,
        'branches': branches,
        'measures': measures,
        'main_glob': main_glob,
        'thresholds': profile.section('stock_threshold', set(branches)),
        'outlier': profile.section('scope.outlier_stores', {'revenue_below_mln'}),
        'disputed': profile.value('scope.disputed_subgroups'),
        'cohort_date': profile.value('cohort.date'),
        'source_cte': sql.source_select(main_glob, period['start'], period['end'],
                                        profile.value('returns'), measures),
    }


def _result_files(paths) -> dict:
    """
    Пути результатов шага. Их же читают проверки, запущенные отдельно.

    Все parquet — в своём слое (`prepared`), включая промежуточные: по ним
    разбирают, что получилось на подшаге. В `paths.tmp` лежат только файлы
    SQL-команд.
    """
    return {'days': paths.prepared / 'series_days.parquet',
            'arrays': paths.prepared / 'series_arrays.parquet',
            'pairs': paths.prepared / 'series_pairs.parquet',
            'stores': paths.prepared / 'series_stores.parquet'}


def run(profile, paths=None, checks=False) -> dict:
    """
    Собрать дневные ряды по профилю параметров.

    `checks` — прогнать проверки слоя сразу после расчёта. По умолчанию их нет:
    регулярный расчёт и эксперименты гоняют шаг часто, а проверки читают выгрузку
    ещё раз. Их запускают отдельно — `check()` или ключ `--checks`.

    Возвращает пути к результатам: `days`, `arrays`, а также промежуточные
    `pairs` и `stores` — их читают проверки и следующие шаги.
    """
    paths = paths or settings.paths
    paths.ensure()

    cfg = run_settings(profile, paths)
    files = _result_files(paths)

    branch_case = sql.branch_case(cfg['branches'], 's.ItemMeasure')
    scope_case = sql.scope_case(cfg['disputed'], 'p.PairLevel2')
    # порога нет только у ветки вне профиля, а таких строк в слое не бывает:
    # источник берёт лишь единицы профиля. Вместо подстановки «дефицита не было»
    # запрос падает с внятным текстом — multiIf считает ветку default лениво
    threshold_case = sql.case('Branch', cfg['thresholds'],
                              default="throwIf(1, 'нет порога наличия для ветки')")

    logger.info(f"series_days: период {cfg['period']['start']}..{cfg['period']['end']}, "
                f"когорта по {cfg['cohort_date']}, пороги наличия {cfg['thresholds']}")

    _step_stores(cfg['source_cte'], float(cfg['outlier']['revenue_below_mln']) * 1e6, paths)
    _step_pairs(cfg['source_cte'], cfg['cohort_date'], paths)
    _step_days(cfg['source_cte'], files['pairs'], files['stores'], branch_case, scope_case,
               threshold_case, files['days'], paths)
    _step_arrays(files['days'], files['arrays'], paths)

    if checks:
        check(profile, paths=paths)
    return files


def check(profile, paths=None) -> list:
    """
    Проверить уже посчитанный слой. Уровень error бросает исключение.

    Отдельная функция, а не часть расчёта: в Prefect это будет отдельная задача,
    а вручную проверки гоняют по готовым файлам, не пересчитывая слой.
    """
    paths = paths or settings.paths
    paths.ensure()  # каталог проверок может ещё не существовать: INTO OUTFILE его не создаёт

    files = _result_files(paths)
    missing = [str(path) for path in files.values() if not pathlib.Path(path).is_file()]
    if missing:
        raise FileNotFoundError(
            f"нечего проверять, слой не посчитан: нет файлов {', '.join(missing)}. "
            f"Сначала запустите шаг без --checks-only")

    return series_checks.check_days(files, run_settings(profile, paths), paths)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Дневные ряды «магазин × товар» (этап 1 анализа)')
    parser.add_argument('--profile', default=PROFILE_FILE, help='профиль параметров анализа')
    parser.add_argument('--checks', action='store_true', help='прогнать проверки слоя после расчёта')
    parser.add_argument('--checks-only', action='store_true',
                        help='только проверки по уже посчитанному слою, без пересчёта')
    args = parser.parse_args()

    profile = params.load(args.profile)
    if args.checks_only:
        check(profile)
    else:
        run(profile, checks=args.checks)
