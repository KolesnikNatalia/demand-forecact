"""
Профиль рядов «магазин × товар»: история, класс спроса, дефицит, ABC.

Этап 2 плана prd/plan-store-item-time-series-analysis.md. Шаг читает слой
дневных рядов (`prepared/series_days.parquet`, этап 1) и пишет в слой `analysis`:

- `profile_pairs.parquet` — агрегаты пары в двух вариантах ряда;
- `profile_abc.parquet` — шесть ABC-классов пары (3 основания × 2 уровня);
- `profile_base.parquet` — профиль: строка на пару и вариант ряда.

Профиль собирается из частей, а не переписывается: этапы 7 и 10 добавят к нему
меры и классы прогнозируемости своими файлами, а `profile_base.parquet` — это
соединение по `(ItemLocationId, Variant)`.

## Что где считается

**Вариант ряда** задаёт население дней: «продажи как есть» (`sales`) — матричные
дни жизни пары, «спрос без дефицита» (`demand`) — те же дни без дней дефицита.
По этому населению считается всё, что опирается на нули: доля дней с продажами,
доля нулей, ADI, CV², квадрант, уровень продаж.

**Свойства пары** в обеих строках одинаковы — их незачем мерить дважды:

- выручка, количество и средняя цена берутся по всем дням пары, включая дни вне
  матрицы: продажа вне матрицы — это деньги, хотя в ряд она не входит (PRD,
  «Границы и допущения»);
- **дефицит** меряется по матричным дням жизни. В варианте `demand` эти дни
  из ряда убраны, поэтому пересчёт по дням варианта дал бы тождественный ноль;
- **ABC** считается один раз на пару, по фактической истории (решение 2026-09-20).
  Класс — это сегмент ассортимента, а не свойство способа измерить ряд: иначе
  разрез «ABC × вариант» на этапе 4 сравнивал бы разные наборы пар.

**«Неприменимо»** — не «ноль». У пары без матричных дней жизни считать долю нулей
не из чего, у пустого ряда нет и истории. В числах там `NULL`, в классах — строка
`неприменимо`. В ABC и сводках такие пары не участвуют.

Запуск:

    uv run python src/analysis/series_profile.py [--checks | --checks-only]
"""

import argparse
import pathlib
import sys
import time

_SRC = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(f"{_SRC}/lib")            # модули src/lib
sys.path.append(f"{_SRC}/checks")         # проверки слоёв
sys.path.append(f"{_SRC}/preprocessing")  # источник слоя дневных рядов
import clickhouse
import params
import profile_checks
import series_days
import settings
import sql
from logger import logger


PROFILE_FILE = settings.paths.root / 'profiles' / 'series_analysis.yaml'

NOT_APPLICABLE = 'неприменимо'

# Квадрант Syntetos–Boylan: ADI — сколько дней ряда приходится на день с продажей,
# CV² — квадрат коэффициента вариации ненулевых продаж. Границы — в профиле.
QUADRANTS = {(False, False): 'гладкий',      # спрос частый, объём ровный
             (False, True): 'неровный',      # частый, но объём скачет
             (True, False): 'прерывистый',   # редкий, но ровный
             (True, True): 'комковатый'}     # редкий и скачет

# Шесть правил ABC: три основания × два уровня (PRD, «ABC-классы»).
#
# - выручка и частота — без `Branch` в разбиении: деньги складываются, а частота
#   продаж — доля, а не объём;
# - количество — только внутри ветки: `кг` со `шт` не складываются;
# - частоте нужны матричные дни: у пары без них частоты нет, а не ноль.
ABC_RULES = (
    {'name': 'AbcRevenueStore', 'metric': 'Revenue', 'partition': ['LocationId']},
    {'name': 'AbcRevenueSubgroup', 'metric': 'Revenue', 'partition': ['LocationId', 'ItemIdLevel2']},
    {'name': 'AbcQtyStore', 'metric': 'SalesQtyTotal', 'partition': ['LocationId', 'Branch']},
    {'name': 'AbcQtySubgroup', 'metric': 'SalesQtyTotal',
     'partition': ['LocationId', 'ItemIdLevel2', 'Branch']},
    {'name': 'AbcFreqStore', 'metric': 'SalesDays', 'partition': ['LocationId'],
     'needs_matrix': True},
    {'name': 'AbcFreqSubgroup', 'metric': 'SalesDays', 'partition': ['LocationId', 'ItemIdLevel2'],
     'needs_matrix': True},
)

ABC_COLUMNS = [rule['name'] for rule in ABC_RULES]


def _exec(command: str, name: str, save_file, paths) -> pathlib.Path:
    """Выполнить запрос в `clickhouse-local` и записать parquet.

    Файл SQL-команды остаётся в `data/tmp/`: по нему потом разбирают, что именно
    считал подшаг. Результат `exec_local` удаляет перед запуском, поэтому упавший
    шаг оставляет отсутствие файла, а не старые данные.
    """
    started = time.time()
    clickhouse.exec_local(command, paths.tmp / f'{name}.sql', save_file, 'Parquet')
    logger.info(f"series_profile: {name} — {time.time() - started:.1f} c → {save_file}")
    return save_file


def _columns(expressions, indent: int) -> str:
    """Список выражений в столбик: запятая спереди, как в остальном SQL проекта."""
    return ('\n' + ' ' * indent + ', ').join(expressions)


def _quadrant_case(adi: float, cv2: float) -> str:
    """
    Квадрант спроса из ADI и CV².

    Считать его не из чего, если нет ни одного дня варианта (тогда `Adi` пуст)
    или день с продажей всего один (тогда пуст `Cv2`: дисперсия по одной точке
    не определена). Такой ряд получает «неприменимо», а не «гладкий».
    """
    rare, jumpy = f"Adi >= {adi}", f"Cv2 >= {cv2}"
    return ("multiIf("
            f"isNull(Adi) or isNull(Cv2), {sql.literal(NOT_APPLICABLE)}"
            f", isNaN(Cv2), {sql.literal(NOT_APPLICABLE)}"
            f", not {rare} and not {jumpy}, {sql.literal(QUADRANTS[(False, False)])}"
            f", not {rare}, {sql.literal(QUADRANTS[(False, True)])}"
            f", not {jumpy}, {sql.literal(QUADRANTS[(True, False)])}"
            f", {sql.literal(QUADRANTS[(True, True)])})")


def _step_pairs(days_file, quadrant_case: str, save_file, paths) -> pathlib.Path:
    """
    Подшаг 1. Агрегаты пары в двух вариантах ряда — один проход по дневному слою.

    Варианты разворачиваются `ARRAY JOIN` по литеральному массиву: строк
    становится вдвое больше, зато «только матричные дни» и «без дней дефицита»
    задаются одним условием `InVariant`, а не двумя ветками кода.

    Первый и последний день жизни берутся как `minIf` / `maxIf` по `InLife`:
    по построению этапа 1 `InLife` — это в точности отрезок между первой
    и последней активностью. Что так и есть, держит проверка
    `profile_life_matches_pairs`.
    """
    command = f"""with t_Var as (
            select
                ItemLocationId
                , Variant
                , TransDate
                , LocationId, ItemId, ItemMeasure, Branch, ItemIdLevel2, ItemIdLevel3
                , LocationNetwork, LocationFormatTT, Scope, Cohort, OutlierStore, IsEmpty
                , IsMatrix, InLife, IsDeficit, SalesQty, SalesAmount
                -- население анализа: матричный день внутри жизни пары
                , toUInt8(IsMatrix = 1 and InLife = 1) as InMatrixLife
                -- население варианта: у «спроса без дефицита» дни распродажи вырезаны
                , toUInt8(InMatrixLife = 1 and (Variant = 'sales' or IsDeficit = 0)) as InVariant
            from file('{days_file}')
            array join ['sales', 'demand'] as Variant
        )
        select
            ItemLocationId
            , Variant
            -- атрибуты пары: в слое они одинаковы во всех её строках
            , any(LocationId) as LocationId
            , any(ItemId) as ItemId
            , any(ItemMeasure) as ItemMeasure
            , any(Branch) as Branch
            , any(ItemIdLevel2) as ItemIdLevel2
            , any(ItemIdLevel3) as ItemIdLevel3
            , any(LocationNetwork) as LocationNetwork
            , any(LocationFormatTT) as LocationFormatTT
            , any(Scope) as Scope
            , any(Cohort) as Cohort
            , toUInt8(max(OutlierStore)) as OutlierStore
            , toUInt8(max(IsEmpty)) as IsEmpty

            -- история пары
            , if(IsEmpty = 1, null, minIf(TransDate, InLife = 1)) as FirstActive
            , if(IsEmpty = 1, null, maxIf(TransDate, InLife = 1)) as LastActive
            , if(IsEmpty = 1, null,
                 toUInt32(dateDiff('day', minIf(TransDate, InLife = 1),
                                          maxIf(TransDate, InLife = 1)) + 1)) as LifeDays
            , toUInt32(countIf(InMatrixLife = 1)) as MatrixDays
            -- матричные дни вне жизни: пара в матрице, но товар туда так и не встал.
            -- В доступность они не идут, но в отчёте показываются отдельной строкой.
            -- У пустого ряда таких дней формально все, но он помечен и в статистики
            -- не входит, поэтому здесь ноль: иначе сумма по слою удвоилась бы
            -- (решение 2026-09-20)
            , toUInt32(if(IsEmpty = 1, 0, countIf(IsMatrix = 1 and InLife = 0))) as MatrixDaysOutLife
            , toUInt32(count()) as SrcRows

            -- деньги и объём — по всем дням пары, включая дни вне матрицы
            , sum(SalesQty) as SalesQtyTotal
            , sum(SalesAmount) as Revenue
            , if(Revenue = 0, null, sumIf(SalesAmount, IsMatrix = 0) / Revenue) as RevenueOutMatrixShare
            -- средняя цена взвешенная (решение 2026-09-20): у `кг` среднее дневных
            -- цен тянул бы за собой день, когда продали двадцать граммов
            , if(sumIf(SalesQty, SalesQty > 0) = 0, null,
                 sumIf(SalesAmount, SalesQty > 0) / sumIf(SalesQty, SalesQty > 0)) as Price

            -- класс спроса: только по дням варианта
            , toUInt32(countIf(InVariant = 1)) as VariantDays
            , toUInt32(countIf(InVariant = 1 and SalesQty > 0)) as SalesDays
            , if(VariantDays = 0, null, sumIf(SalesQty, InVariant = 1)) as VariantQty
            , if(VariantDays = 0, null, sumIf(SalesQty, InVariant = 1) / VariantDays) as SalesLevel
            , if(VariantDays = 0, null, SalesDays / VariantDays) as SalesDayShare
            , if(VariantDays = 0, null, 1 - SalesDays / VariantDays) as ZeroShare
            , if(SalesDays = 0, null, VariantDays / SalesDays) as Adi
            , if(SalesDays < 2, null,
                 varSampIf(SalesQty, InVariant = 1 and SalesQty > 0)
                 / pow(avgIf(SalesQty, InVariant = 1 and SalesQty > 0), 2)) as Cv2
            , {quadrant_case} as Quadrant

            -- дефицит — свойство пары: он меряется по матричным дням жизни
            , toUInt32(countIf(InMatrixLife = 1 and IsDeficit = 1)) as DeficitDays
            , if(MatrixDays = 0, null, DeficitDays / MatrixDays) as DeficitDayShare
            , if(sumIf(SalesQty, InMatrixLife = 1) = 0, null,
                 sumIf(SalesQty, InMatrixLife = 1 and IsDeficit = 1)
                 / sumIf(SalesQty, InMatrixLife = 1)) as DeficitSalesShare
        from t_Var
        group by ItemLocationId, Variant
        order by ItemLocationId, Variant
        {sql.PARQUET_SETTINGS}
    """
    return _exec(command, 'profile_pairs', save_file, paths)


def _abc_expressions(thresholds) -> tuple:
    """
    Оконные выражения ABC и разбор их в классы.

    Правило одно на все шесть: позиции сортируются по основанию вниз, и класс
    определяется накопленной долей **до** самой позиции. Поэтому позиция,
    которая пересекает границу 80%, попадает в A, и в каждом магазине есть
    хотя бы одна A.

    Пара, к которой правило неприменимо (пустой ряд; для частоты — ещё и пара
    без матричных дней), получает основание 0: она уходит в конец сортировки,
    ничего не добавляет к итогу разбиения и помечается «неприменимо».
    Пара с нулевым основанием — сразу C: делить хвост из нулей не на что.

    **Равные основания разводит выручка** (решение 2026-09-20). У частоты
    основание — целое число дней, и на магазин приходятся десятки пар с одним
    и тем же значением. Без второго ключа класс тех из них, на которых проходит
    граница, решал бы порядок `ItemLocationId`, то есть ничего не значащий номер.
    Сортировка по выручке внутри равных делает выбор осмысленным: из пар
    с одинаковой частотой класс повыше достаётся той, что принесла больше денег.
    `ItemLocationId` остаётся последним ключом — ради воспроизводимости.
    """
    inner, outer = [], []
    for rule in ABC_RULES:
        name, partition = rule['name'], ', '.join(rule['partition'])
        applicable = 'IsEmpty = 0' + (' and MatrixDays > 0' if rule.get('needs_matrix') else '')
        # выражение основания подставляется целиком и в сортировку, и в суммы:
        # ссылаться в окне на алиас из того же списка выборки ненадёжно
        metric = f"if({applicable}, toFloat64({rule['metric']}), 0)"
        order = f"{metric} desc, Revenue desc, ItemLocationId"

        inner += [
            f"{metric} as Met_{name}",
            f"sum({metric}) over (partition by {partition} order by {order}"
            f" rows between unbounded preceding and current row) as Cum_{name}",
            f"sum({metric}) over (partition by {partition}) as Tot_{name}",
            f"toUInt8({applicable}) as Apply_{name}",
        ]
        outer.append(
            f"multiIf(Apply_{name} = 0, {sql.literal(NOT_APPLICABLE)}"
            f", Tot_{name} <= 0, 'C'"
            f", Met_{name} <= 0, 'C'"
            f", (Cum_{name} - Met_{name}) / Tot_{name} < {thresholds[0]}, 'A'"
            f", (Cum_{name} - Met_{name}) / Tot_{name} < {thresholds[1]}, 'B'"
            f", 'C') as {name}")
    return inner, outer


def _step_abc(pairs_file, thresholds, save_file, paths) -> pathlib.Path:
    """
    Подшаг 2. Шесть ABC-классов пары.

    Основания берутся из строки варианта `sales`: это фактическая история пары,
    одна строка на пару. Класс от варианта ряда не зависит (см. шапку модуля).
    """
    inner, outer = _abc_expressions(thresholds)
    inner_sql = _columns(inner, 16)
    outer_sql = _columns(outer, 12)

    command = f"""with t_Pairs as (
            select
                ItemLocationId
                , LocationId, ItemIdLevel2, Branch, IsEmpty, MatrixDays
                , Revenue, SalesQtyTotal, SalesDays
            from file('{pairs_file}')
            where Variant = 'sales'
        )
        select
            ItemLocationId
            , {outer_sql}
        from (
            select
                ItemLocationId
                , {inner_sql}
            from t_Pairs
        )
        order by ItemLocationId
        {sql.PARQUET_SETTINGS}
    """
    return _exec(command, 'profile_abc', save_file, paths)


def _step_base(pairs_file, abc_file, save_file, paths) -> pathlib.Path:
    """
    Подшаг 3. Профиль: агрегаты варианта плюс ABC-классы пары.

    Присоединяемый CTE отдаёт ключ под своим именем (`AbcPairId`): одноимённые
    колонки справа дают `AMBIGUOUS_IDENTIFIER` на `ON`, даже когда сам `ON` верен.
    """
    command = f"""with t_Abc as (
            select
                ItemLocationId as AbcPairId
                , {_columns(ABC_COLUMNS, 16)}
            from file('{abc_file}')
        )
        select
            p.*
            , {_columns([f"a.{name} as {name}" for name in ABC_COLUMNS], 12)}
        from file('{pairs_file}') as p
        left join t_Abc as a on a.AbcPairId = p.ItemLocationId
        order by ItemLocationId, Variant
        {sql.PARQUET_SETTINGS}
    """
    return _exec(command, 'profile_base', save_file, paths)


def run_settings(profile, paths) -> dict:
    """
    Разобрать профиль: параметры этапа 2 плюс источник слоя этапа 1.

    Источник берётся у шага дневных рядов, а не собирается заново: проверка сумм
    должна сверять профиль с тем же сырьём, из которого построен слой.
    """
    cfg = series_days.run_settings(profile, paths)

    # границы подставляются прямо в SQL, поэтому проверяются здесь: `adi: 1,32`
    # или пустое значение иначе всплыли бы ошибкой разбора запроса, где искать
    # причину пришлось бы в дампе SQL, а не в имени раздела профиля
    demand_class = profile.section('demand_class', {'adi', 'cv2'})
    for key, value in demand_class.items():
        if not isinstance(value, (int, float)) or isinstance(value, bool) or not value > 0:
            raise ValueError(f"{profile.file}, раздел 'demand_class': граница '{key}' — "
                             f"положительное число, а не {value!r}")

    thresholds = profile.value('abc.thresholds')
    if len(thresholds) != 2 or not all(isinstance(t, (int, float)) for t in thresholds) \
            or not 0 < thresholds[0] < thresholds[1] < 1:
        raise ValueError(f"{profile.file}, раздел 'abc': ожидаются две границы 0 < A < B < 1, "
                         f"а не {thresholds!r}")

    # состав ABC и слово «неприменимо» нужны и расчёту, и проверкам. Они едут
    # через cfg, а не импортом: иначе модуль проверок и шаг ссылались бы друг
    # на друга, и порядок импорта стал бы важен
    cfg.update({'demand_class': demand_class, 'abc_thresholds': thresholds,
                'abc_columns': ABC_COLUMNS, 'not_applicable': NOT_APPLICABLE,
                'quadrants': sorted(QUADRANTS.values()) + [NOT_APPLICABLE]})
    return cfg


def _result_files(paths) -> dict:
    """
    Пути результатов шага. Их же читают проверки, запущенные отдельно.

    Все parquet — в слое `analysis`, включая промежуточные: по ним разбирают,
    что получилось на подшаге. В `paths.tmp` лежат только файлы SQL-команд.
    """
    return {'base': paths.analysis / 'profile_base.parquet',
            'pairs': paths.analysis / 'profile_pairs.parquet',
            'abc': paths.analysis / 'profile_abc.parquet',
            'days': paths.prepared / 'series_days.parquet',
            'series_pairs': paths.prepared / 'series_pairs.parquet'}


def run(profile, paths=None, checks=False) -> dict:
    """
    Собрать профиль рядов по профилю параметров.

    `checks` — прогнать проверки сразу после расчёта. По умолчанию их нет:
    они читают выгрузку ещё раз, а гоняют профиль часто. Их запускают отдельно —
    `check()` или ключ `--checks`.
    """
    paths = paths or settings.paths
    paths.ensure()

    cfg = run_settings(profile, paths)
    files = _result_files(paths)
    if not pathlib.Path(files['days']).is_file():
        raise FileNotFoundError(
            f"нет слоя дневных рядов: {files['days']}. Сначала посчитайте этап 1 — "
            f"uv run python src/preprocessing/series_days.py")

    demand_class = cfg['demand_class']
    logger.info(f"series_profile: границы класса спроса ADI {demand_class['adi']}, "
                f"CV² {demand_class['cv2']}; границы ABC {cfg['abc_thresholds']}")

    _step_pairs(files['days'], _quadrant_case(demand_class['adi'], demand_class['cv2']),
                files['pairs'], paths)
    _step_abc(files['pairs'], cfg['abc_thresholds'], files['abc'], paths)
    _step_base(files['pairs'], files['abc'], files['base'], paths)

    if checks:
        check(profile, paths=paths)
    return files


def check(profile, paths=None) -> list:
    """Проверить уже посчитанный профиль. Уровень error бросает исключение."""
    paths = paths or settings.paths
    paths.ensure()  # каталог проверок может ещё не существовать: INTO OUTFILE его не создаёт

    files = _result_files(paths)
    missing = [str(path) for path in files.values() if not pathlib.Path(path).is_file()]
    if missing:
        raise FileNotFoundError(
            f"нечего проверять, профиль не посчитан: нет файлов {', '.join(missing)}. "
            f"Сначала запустите шаг без --checks-only")

    return profile_checks.check_profile(files, run_settings(profile, paths), paths)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Профиль рядов «магазин × товар» (этап 2 анализа)')
    parser.add_argument('--profile', default=PROFILE_FILE, help='профиль параметров анализа')
    parser.add_argument('--checks', action='store_true', help='прогнать проверки после расчёта')
    parser.add_argument('--checks-only', action='store_true',
                        help='только проверки по уже посчитанному профилю, без пересчёта')
    args = parser.parse_args()

    profile = params.load(args.profile)
    if args.checks_only:
        check(profile)
    else:
        run(profile, checks=args.checks)
