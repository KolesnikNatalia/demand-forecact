"""
Отчёт по анализу рядов «магазин × товар»: шапка, отпечаток данных, разделы.

Этап 3 плана prd/plan-store-item-time-series-analysis.md. Шаг читает профиль
рядов (`analysis/profile_base.parquet`, этап 2) и дневной слой (`prepared/series_days.parquet`,
этап 1) и собирает `docs/series_analysis.md`. Рядом, в слое `analysis`, остаётся
манифест `report_tables.json`: имя, разрез и меры каждой сводной таблицы.

## Что отчёт даёт и чего не трогает

**Шапка пересчитывается каждый прогон**: дата расчёта и отпечаток данных —
период, файлы выгрузки, число строк и хеш содержимого. По отпечатку видно,
на какой выгрузке посчитаны цифры ниже: выгрузка обновляется, а отчёт датирован
([PRD](../../prd/prd-store-item-time-series-analysis.md), «Критерии готовности»).

**Выводы, написанные руками, пересборку переживают.** Автоматические куски стоят
между маркерами `<!-- auto:имя -->` и `<!-- /auto -->`, и сборщик заменяет только
их. Всё, что вне маркеров, — текст человека, и шаг его не трогает. Нет файла —
он создаётся из шаблона; пропал блок целиком — раздел дописывается в конец,
а не теряется молча; сломана разметка (нет закрывающего маркера, маркер
задвоен) — шаг падает, не тронув файл.

**Ни одна таблица не складывает `кг` и `шт` и не подмешивает спорные подгруппы
в итоги.** Это держит построитель `report_tables.py`, а не внимательность: таблица без
ветки в ключе не строится.

Запуск:

    uv run python src/report/series_report.py [--checks]
"""

import argparse
import datetime
import json
import pathlib
import sys
import time

_SRC = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(f"{_SRC}/lib")            # модули src/lib
sys.path.append(f"{_SRC}/preprocessing")  # источник слоя дневных рядов
sys.path.append(f"{_SRC}/analysis")       # источник профиля рядов
sys.path.append(f"{_SRC}/report")         # построитель сводных таблиц
import clickhouse
import params
import series_days
import series_profile
import settings
import sql
import report_tables
from logger import logger


PROFILE_FILE = settings.paths.root / 'profiles' / 'series_analysis.yaml'

REPORT_FILE = 'series_analysis.md'  # имя задаёт шаг, каталог — paths.docs

# Рабочий отчёт. Умолчание живёт только здесь и в разборе командной строки,
# а не в `run()`: см. её описание
REPORT_PATH = settings.paths.docs / REPORT_FILE

MARK_START = '<!-- auto:{name} -->'
MARK_END = '<!-- /auto -->'

# Строка шапки с периодом: по ней проверка сверяет отчёт с профилем
PERIOD_TITLE = 'период профиля параметров'

NOT_APPLICABLE = series_profile.NOT_APPLICABLE

# Варианты ряда: «продажи как есть» и «спрос без дефицита» (этап 1).
# Порядок в сводках — тот, в котором их читают, а не алфавитный
VARIANT_ORDER = "Variant = 'demand'"

# Шаблон нового отчёта. Заголовки и маркеры — здесь, содержимое разделов
# подставляет сборщик. Текст вне маркеров пишет человек, и пересборка его
# не трогает: поэтому в шаблоне под каждым разделом есть место под выводы
TEMPLATE = f"""# Анализ рядов «магазин × товар»

Отчёт собирается шагом `src/report/series_report.py` из профиля рядов (этап 2).
Автоматические части стоят между маркерами `{MARK_START.format(name='имя')}`
и `{MARK_END}` и пересчитываются каждый прогон. Всё, что вне маркеров, — выводы
и замечания человека: пересборка их не трогает.

{MARK_START.format(name='header')}
{MARK_END}

## Популяция

{MARK_START.format(name='population')}
{MARK_END}

### Выводы по популяции

<!-- сюда пишутся выводы руками -->
"""


def _fingerprint(cfg: dict, files: dict, paths) -> dict:
    """
    Отпечаток данных: на чём посчитаны цифры отчёта.

    Период и маска выгрузки берутся из того же разбора профиля, что и у шага
    этапа 1: отпечаток должен описывать ровно тот источник, из которого построен
    слой, а не похожий.

    Хеш `sum(cityHash64(*))` не зависит от порядка строк и считается за доли
    секунды на всей выгрузке. Размер файла — это размер файла целиком, а строки
    считаются в границах периода: файл месяца может выходить за период.
    """
    period = cfg['period']
    where = (f"where TransDate between toDate({sql.literal(period['start'])})"
             f" and toDate({sql.literal(period['end'])})")

    command = f"""with t_Files as (
            select _file as SrcFile, any(_size) as Bytes, count() as SrcRows
            from file('{cfg['main_glob']}')
            {where}
            group by SrcFile
        )
        , t_Src as (
            select
                toUInt64(count()) as Files
                , toUInt64(sum(Bytes)) as Bytes
                , toUInt64(sum(SrcRows)) as Rows
            from t_Files
        )
        , t_Hash as (
            select
                toInt64(sum(cityHash64(*))) as Hash
                , toUInt64(uniqExact(ItemLocationId)) as Pairs
            from file('{cfg['main_glob']}')
            {where}
        )
        , t_Days as (
            select
                toUInt64(count()) as Rows
                , toUInt64(uniqExact(ItemLocationId)) as Pairs
                , min(TransDate) as FirstDay
                , max(TransDate) as LastDay
            from file('{files['days']}')
        )
        , t_Base as (
            select
                toUInt64(count()) as Rows
                , toUInt64(uniqExact(ItemLocationId)) as Pairs
                , toUInt64(countIf(Variant = 'sales' and IsEmpty = 1)) as EmptyPairs
            from file('{files['base']}')
        )
        select
            t_Src.Files as SrcFiles
            , t_Src.Bytes as SrcBytes
            , t_Src.Rows as SrcRows
            , t_Hash.Hash as SrcHash
            , t_Hash.Pairs as SrcPairs
            , t_Days.Rows as DayRows
            , t_Days.Pairs as DayPairs
            , toString(t_Days.FirstDay) as DayFirst
            , toString(t_Days.LastDay) as DayLast
            , t_Base.Rows as BaseRows
            , t_Base.Pairs as BasePairs
            , t_Base.EmptyPairs as BaseEmptyPairs
        from t_Src, t_Hash, t_Days, t_Base
        format JSONEachRow
    """
    text = clickhouse.exec_local(command, paths.tmp / 'report_fingerprint.sql',
                                 return_result=True)
    lines = [line for line in (text or '').splitlines() if line.strip()]
    if not lines:
        raise ValueError(f"отпечаток данных не посчитан: {paths.tmp / 'report_fingerprint.sql'}")
    return json.loads(lines[0])


def _period_row(period: dict) -> str:
    """Строка шапки с периодом — одна для сборки и для проверки отчёта."""
    return f"| {PERIOD_TITLE} | {period['start']} … {period['end']} |"


def _header(cfg: dict, mark: dict, profile_file, paths) -> str:
    """Шапка отчёта: дата расчёта и отпечаток данных."""
    period = cfg['period']
    rows = [
        ('файлов выгрузки', f"{mark['SrcFiles']} ({mark['SrcBytes'] / 2**20:.0f} МБ)"),
        ('строк выгрузки в периоде', f"{mark['SrcRows']:,}".replace(',', ' ')),
        ('пар «магазин × товар» в выгрузке', f"{mark['SrcPairs']:,}".replace(',', ' ')),
        ('хеш содержимого выгрузки', f"`{mark['SrcHash']}`"),
        ('строк в слое дневных рядов', f"{mark['DayRows']:,}".replace(',', ' ')),
        ('дни слоя', f"{mark['DayFirst']} … {mark['DayLast']}"),
        ('строк в профиле рядов', f"{mark['BaseRows']:,}".replace(',', ' ')),
        ('пар в профиле, из них пустых рядов',
         f"{mark['BasePairs']:,}".replace(',', ' ') + ' / '
         + f"{mark['BaseEmptyPairs']:,}".replace(',', ' ')),
    ]

    # профиль показывается путём от корня проекта, но лежать он может и вне его
    # (свой профиль теста, прогон с чужим конфигом) — тогда печатается как есть
    profile_file = pathlib.Path(profile_file)
    shown = profile_file.relative_to(paths.root) if profile_file.is_relative_to(paths.root) \
        else profile_file

    lines = [f"**Дата расчёта:** {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}", '',
             f"**Профиль параметров:** `{shown}`", '',
             '### Отпечаток данных', '',
             '| показатель | значение |', '|:-|-:|']
    lines.append(_period_row(period))
    lines += [f"| {title} | {value} |" for title, value in rows]
    lines += ['', 'Цифры отчёта посчитаны на выгрузке с этим отпечатком. Выгрузка обновляется, '
              'поэтому сравнивать их с цифрами прошлых прогонов можно только при том же хеше.']
    return '\n'.join(lines)


def _population(files: dict, paths) -> tuple:
    """
    Раздел «Популяция»: кто вообще попал в анализ и с каким классом спроса.

    Три таблицы, потому что у них разное зерно строки:

    - **пары и выручка** — свойства пары, поэтому берётся один вариант ряда:
      в профиле их два, и без фильтра выручка удвоилась бы;
    - **класс спроса и дефицит** — свойства варианта, поэтому вариант в разрезе.
      Пустые ряды отсюда убраны: истории у них нет, и в классах они только
      заслоняли бы остальных. Сколько их, видно в первой таблице и в шапке;
    - **почему нет класса** — «неприменимо» не одно: пустой ряд, пара без
      матричных дней, вариант, у которого дефицит съел все дни, и ряд
      с единственной продажей. Разные причины — разные выводы, поэтому они
      разведены, а не свалены в одну строку.

    Доля дефицита — отношение сумм (`дни дефицита / матричные дни`), а не среднее
    долей по парам: иначе пара с тремя днями весила бы столько же, сколько пара
    с четырьмя сотнями.
    """
    base = files['base']
    branch = report_tables.Key('Branch', 'ветка')
    variant = report_tables.Key('Variant', 'вариант ряда', order=VARIANT_ORDER)
    pairs = report_tables.Measure('пар', 'toUInt64(count())', share=True)
    revenue = report_tables.Measure('выручка, руб.', 'sum(Revenue)', digits=0, share=True)

    built = [
        report_tables.pivot(
            'population_pairs', 'Пары и выручка по когортам', base, paths,
            keys=[branch, report_tables.Key('Cohort', 'когорта')],
            measures=[pairs,
                      report_tables.Measure('пустых рядов', 'toUInt64(countIf(IsEmpty = 1))'),
                      revenue],
            where="Variant = 'sales'"),
        report_tables.pivot(
            'population_demand_class', 'Класс спроса и дефицит по вариантам ряда', base, paths,
            keys=[branch, variant, report_tables.Key('Quadrant', 'класс спроса')],
            measures=[pairs, revenue,
                      report_tables.Measure('дней дефицита, %',
                                     '100 * sum(DeficitDays) / nullIf(sum(MatrixDays), 0)',
                                     digits=1)],
            where='IsEmpty = 0'),
        report_tables.pivot(
            'population_not_applicable', 'Пары без класса спроса', base, paths,
            keys=[branch, variant,
                  report_tables.Key('Reason', 'почему нет класса',
                             expr=f"multiIf(IsEmpty = 1, 'пустой ряд'"
                                  f", MatrixDays = 0, 'без матричных дней'"
                                  f", VariantDays = 0, 'нет дней варианта'"
                                  f", Quadrant = {sql.literal(NOT_APPLICABLE)},"
                                  f" 'меньше двух дней с продажами'"
                                  f", 'класс посчитан')",
                             order="Reason = 'класс посчитан'")],
            measures=[pairs, revenue]),
    ]

    intro = (
        'Кто попал в анализ. Вариант ряда `sales` — «продажи как есть» (матричные дни жизни '
        'пары), `demand` — «спрос без дефицита» (те же дни без дней распродажи). Спорные '
        'подгруппы идут отдельной строкой и в основные итоги не входят; строка `итого` — '
        'итог своей ветки и своего сегмента.')

    text = [intro, '']
    for table in built:
        text += [f"#### {table.title}", '', table.markdown, '']
    return '\n'.join(text).rstrip(), built


def _replace_block(text: str, name: str, content: str) -> str:
    """
    Заменить автоматический кусок между маркерами, не трогая остальное.

    Нет маркера — раздел дописывается в конец вместе с маркерами: текст человека
    важнее аккуратности, и молча потерять раздел хуже, чем поставить его не туда.

    Сломанная разметка — ошибка, а не догадка. Если закрывающий маркер пропал,
    ближайший `<!-- /auto -->` принадлежит уже следующему блоку, и замена до него
    стёрла бы заголовки, чужой блок и выводы между ними. Поэтому закрывающий
    маркер должен стоять раньше, чем начнётся следующий блок, а открывающий —
    встречаться один раз. Ошибка бросается до записи: файл остаётся как был.
    """
    start, end = MARK_START.format(name=name), MARK_END
    block = f"{start}\n{content}\n{end}"

    at = text.find(start)
    if at < 0:
        logger.warning(f"report: в отчёте нет маркера {start} — раздел дописан в конец")
        return text.rstrip() + f"\n\n{block}\n"
    if text.count(start) > 1:
        raise ValueError(f"в отчёте маркер {start} встречается {text.count(start)} раза: "
                         f"неясно, какой блок заменять. Уберите лишний руками")

    inside = at + len(start)
    finish = text.find(end, inside)
    next_block = text.find(MARK_START.split('{')[0], inside)  # начало любого блока
    if finish < 0 or 0 <= next_block < finish:
        raise ValueError(f"в отчёте у {start} нет своего закрывающего {end}: замена "
                         f"стёрла бы текст до конца следующего блока. Верните маркер руками")
    return text[:at] + block + text[finish + len(end):]


def run_settings(profile, paths) -> dict:
    """
    Разобрать профиль: то же, что у этапов 1 и 2.

    Свои параметры у отчёта пока не появились, но разбор идёт через шаг этапа 2:
    так отчёт падает на той же опечатке в профиле, что и расчёт, а не собирает
    таблицы по слою, посчитанному с другими порогами.
    """
    return series_profile.run_settings(profile, paths)


def _result_files(paths) -> dict:
    """Пути результатов шага и источников, из которых он собирается."""
    return {'manifest': paths.analysis / report_tables.MANIFEST_FILE,
            'base': paths.analysis / 'profile_base.parquet',
            'days': paths.prepared / 'series_days.parquet'}


def run(profile, paths=None, checks=False, *, report_file) -> dict:
    """
    Собрать отчёт по профилю параметров.

    `report_file` — куда писать отчёт, и **умолчания у него нет намеренно**.
    Каталог `docs` лежит в репозитории, а не в корне данных, и `DF_DATA_DIR` его
    не уводит. С умолчанием любой вызов с чужими `paths` — новый тест, отладка
    на фикстуре — молча переписал бы закоммиченный `docs/series_analysis.md`
    чужими цифрами. Без умолчания такой вызов падает `TypeError`, а рабочий путь
    `REPORT_PATH` подставляет только запуск из командной строки.
    """
    paths = paths or settings.paths
    paths.ensure()

    cfg = run_settings(profile, paths)
    files = _result_files(paths)
    files['report'] = pathlib.Path(report_file)

    missing = [str(files[key]) for key in ('base', 'days') if not pathlib.Path(files[key]).is_file()]
    if missing:
        raise FileNotFoundError(
            f"нечего собирать, нет источников отчёта: {', '.join(missing)}. Сначала посчитайте "
            f"этапы 1 и 2 — uv run python main.py")

    started = time.time()
    mark = _fingerprint(cfg, files, paths)
    logger.info(f"report: отпечаток — строк выгрузки {mark['SrcRows']}, хеш {mark['SrcHash']}")

    population, built = _population(files, paths)
    report_tables.write_manifest(built, files['manifest'])

    text = files['report'].read_text(encoding='utf-8') if files['report'].is_file() else TEMPLATE
    text = _replace_block(text, 'header', _header(cfg, mark, profile.file, paths))
    text = _replace_block(text, 'population', population)
    files['report'].parent.mkdir(parents=True, exist_ok=True)
    files['report'].write_text(text, encoding='utf-8')

    logger.info(f"report: {len(built)} таблиц за {time.time() - started:.1f} c → {files['report']}")

    if checks:
        check(profile, paths=paths, report_file=files['report'])
    return files


def check(profile, paths=None, *, report_file) -> list:
    """
    Проверить собранный отчёт по манифесту таблиц и профилю параметров.

    Проверка ловит то, что построитель поймать не может: таблицу, собранную
    в обход него, потерянный раздел и отчёт, собранный по другому профилю —
    период в шапке сверяется с профилем, а сам профиль разбирается так же,
    как при расчёте, и опечатка в нём роняет проверку.

    Журнала проверок у отчёта нет, в отличие от слоёв данных: сверять суммы
    не с чем, правила таблиц держит построитель. Почему так — docs/data_quality.md,
    разд. 3, «Отчёт анализа». Полную проверку таблиц отчёта делает этап 14.
    """
    paths = paths or settings.paths
    cfg = run_settings(profile, paths)
    files = _result_files(paths)
    report = pathlib.Path(report_file)

    if not files['manifest'].is_file():
        raise FileNotFoundError(f"нет манифеста таблиц: {files['manifest']}. "
                                f"Сначала соберите отчёт без --checks")
    manifest = json.loads(files['manifest'].read_text(encoding='utf-8'))

    problems = []
    for entry in manifest['tables']:
        if report_tables.BRANCH not in entry['keys']:
            problems.append(f"таблица '{entry['name']}': в разрезе нет ветки")
        if report_tables.SCOPE not in entry['keys']:
            problems.append(f"таблица '{entry['name']}': в разрезе нет сегмента, "
                            f"спорные подгруппы попали бы в основные итоги")

    text = report.read_text(encoding='utf-8') if report.is_file() else ''
    if not text:
        problems.append(f"нет отчёта: {report}")
    for name in ('header', 'population'):
        if MARK_START.format(name=name) not in text:
            problems.append(f"в отчёте нет раздела '{name}'")
    # отчёт собран по другому профилю — цифры в нём не про этот прогон
    if text and _period_row(cfg['period']) not in text:
        problems.append(f"период в шапке отчёта не совпадает с профилем {profile.file} "
                        f"({cfg['period']['start']} … {cfg['period']['end']}): отчёт собран "
                        f"по другому профилю")
    for entry in manifest['tables']:
        if entry['title'] not in text:
            problems.append(f"таблица '{entry['name']}' есть в манифесте, но не в отчёте")

    for problem in problems:
        logger.error(f"проверка отчёта: {problem}")
    if problems:
        raise ValueError(f"отчёт не прошёл проверки ({len(problems)}): {'; '.join(problems)}")

    logger.info(f"проверка отчёта: таблиц {len(manifest['tables'])}, замечаний нет")
    return manifest['tables']


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Отчёт по анализу рядов (этап 3 анализа)')
    parser.add_argument('--profile', default=PROFILE_FILE, help='профиль параметров анализа')
    parser.add_argument('--checks', action='store_true', help='проверить отчёт после сборки')
    parser.add_argument('--checks-only', action='store_true',
                        help='только проверки по уже собранному отчёту, без пересборки')
    parser.add_argument('--report', default=REPORT_PATH, help='куда писать отчёт')
    args = parser.parse_args()

    profile = params.load(args.profile)
    if args.checks_only:
        check(profile, report_file=args.report)
    else:
        run(profile, checks=args.checks, report_file=args.report)
