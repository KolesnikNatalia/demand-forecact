"""
Построитель сводных таблиц отчёта: разрез, итоги и доли одним запросом.

Этап 3 плана prd/plan-store-item-time-series-analysis.md. Модуль назван
`report_tables`, а не `tables`: `sys.path.append` кладёт каталог проекта в конец
пути поиска, и установленный пакет PyTables (`tables`) перекрыл бы наш модуль —
та же ловушка, из-за которой `profile` в `src/lib` назван `params`.

На этом построителе собираются все разделы отчёта, поэтому правила отчёта
он держит сам, а не надеется на внимательность того, кто пишет очередной раздел:

- **ветка всегда в ключе разреза.** `кг` и `шт` — разные распределения, складывать
  их в одну цифру нельзя (`CLAUDE.md`). Таблица без `Branch` в ключе не строится,
  а падает с ошибкой;
- **спорные подгруппы идут отдельной строкой и не входят в основные итоги.**
  `Scope` добавляется в ключ сам, поэтому итог основного ассортимента и итог
  спорных подгрупп — это две разные строки, и сложить их нельзя даже случайно.
  Отфильтровать `Scope` условием тоже нельзя: тогда спорные исчезли бы из отчёта;
- **два варианта ряда — это одни и те же пары дважды.** Если `Variant` не в ключе
  и не ограничен условием, выручка и число пар удвоились бы, а таблица выглядела
  бы правдоподобно. Такая таблица тоже не строится;
- **магазины вне общей массы видны отдельно.** В разрезе по магазину, сети или
  формату флаг `OutlierStore` добавляется в ключ сам (решение 2026-09-19): в задачу
  такие магазины входят, но в сводке они не должны растворяться в общей строке;
- **доли считаются из сумм на каждом уровне группировки**, а не усреднением
  строк: итог — это не среднее средних.

## Как устроена таблица

Разрез считается `GROUPING SETS`: один набор — все ключи, второй — только ключи
блока (`Branch`, `Scope` и `Variant`, если он в разрезе). Второй набор и даёт
строку «итого», от которой считаются доли. Блок — это то, внутри чего доли
складываются в 100%: население пар задают ветка и сегмент, а вариант ряда берёт
те же пары ещё раз, поэтому он тоже в блоке.

    report_tables.pivot(
        'population_pairs', 'Пары и выручка по когортам', profile_base, paths,
        keys=[report_tables.Key('Branch', 'ветка'), report_tables.Key('Cohort', 'когорта')],
        measures=[report_tables.Measure('пар', 'toUInt64(count())', share=True)],
        where="Variant = 'sales'")

Возвращается `Table`: готовая markdown-таблица (её делает сам ClickHouse,
`FORMAT Markdown`) и запись для манифеста `report_tables.json`. Манифест читает
проверка этапа 14: у каждой таблицы отчёта должна быть ветка в ключе.
"""

import dataclasses
import datetime
import json
import pathlib
import re
import sys

_SRC = pathlib.Path(__file__).resolve().parents[1]
sys.path.append(f"{_SRC}/lib")  # модули src/lib
import clickhouse
import sql
from logger import logger


BRANCH = 'Branch'      # ветка расчёта: обязательный ключ любой таблицы
SCOPE = 'Scope'        # основные / спорные подгруппы: добавляется в ключ сам
VARIANT = 'Variant'    # вариант ряда: одни и те же пары, посчитанные дважды
OUTLIER = 'OutlierStore'  # магазин вне общей массы

TOTAL = 'итого'  # значение ключа в строке итога блока

OUTLIER_NO = 'обычный'          # значения ключа «тип магазина» в таблице
OUTLIER_YES = 'вне общей массы'

# Ключи, за которыми стоит магазин: в таком разрезе к ключу добавляется OutlierStore
STORE_KEYS = ('LocationId', 'LocationNetwork', 'LocationFormatTT')

MANIFEST_FILE = 'report_tables.json'  # имя задаёт шаг, каталог — paths.analysis


@dataclasses.dataclass(frozen=True)
class Key:
    """
    Колонка разреза.

    `name` — имя колонки в источнике и в манифесте; по нему проверка этапа 14
    находит ветку. `expr` задаётся, когда ключа в источнике нет и его надо
    вычислить (тогда колонка источника с этим именем заменяется вычисленной).
    `order` — выражение сортировки, если алфавитный порядок значений не годится.
    """

    name: str
    title: str = ''
    expr: str = ''
    order: str = ''

    def header(self) -> str:
        return self.title or self.name

    def order_by(self) -> list:
        """Сортировка ключа: заданная, а следом — само значение, чтобы не плавал
        порядок строк, у которых заданное выражение совпало."""
        return [self.order, self.name] if self.order else [self.name]


@dataclasses.dataclass(frozen=True)
class Measure:
    """
    Мера: агрегат по строкам источника.

    `share` добавляет рядом колонку доли от итога блока. Доля считается из сумм
    (`мера строки / мера итога`), поэтому итог — не среднее средних. `digits` —
    округление: без него ClickHouse печатает float целиком, и таблицу
    не прочитать.
    """

    title: str
    expr: str
    digits: int | None = None
    share: bool = False

    def share_header(self) -> str:
        return f"{self.title}, %"


@dataclasses.dataclass(frozen=True)
class Table:
    """Посчитанная таблица: markdown для отчёта и запись для манифеста."""

    name: str
    title: str
    source: str
    where: str
    keys: tuple
    block: tuple
    measures: tuple
    markdown: str

    def entry(self) -> dict:
        """Запись манифеста: по ней этап 14 проверяет правила отчёта."""
        return {'name': self.name,
                'title': self.title,
                'source': pathlib.Path(self.source).name,
                'where': self.where,
                'keys': [key.name for key in self.keys],
                'block': list(self.block),
                'measures': [measure.title for measure in self.measures]}


def _check_keys(keys, columns, source) -> list:
    """
    Проверить разрез и дополнить его ключами, которые обязаны в нём быть.

    Ошибки здесь — это ошибки отчёта, а не данных: таблица, где сложены `кг`
    и `шт` или две строки одной пары, выглядит правдоподобно, и глазами её
    не поймать. Поэтому такая таблица не строится вовсе.
    """
    names = [key.name for key in keys]
    if len(set(names)) != len(names):
        raise ValueError(f"в разрезе таблицы повторяются ключи: {names}")
    if BRANCH not in names:
        raise ValueError(
            f"в разрезе таблицы нет ключа '{BRANCH}': ветки `кг` и `шт` — разные "
            f"распределения, и складывать их в одну цифру нельзя. Ключи: {names}")

    keys = list(keys)
    if SCOPE not in names:
        # спорные подгруппы — отдельная строка и отдельный итог
        keys.append(Key(SCOPE, 'сегмент', order=f"{SCOPE} = {sql.literal(sql.SCOPE_DISPUTED)}"))
    if OUTLIER not in names and any(name in STORE_KEYS for name in names):
        # порядок — как у сегмента: сначала обычное, исключения ниже
        keys.append(Key(OUTLIER, 'тип магазина',
                        expr=f"if({OUTLIER} = 1, {sql.literal(OUTLIER_YES)}, {sql.literal(OUTLIER_NO)})",
                        order=f"{OUTLIER} = {sql.literal(OUTLIER_YES)}"))

    missing = [key.name for key in keys if not key.expr and key.name not in columns]
    if missing:
        raise ValueError(f"в источнике {source} нет колонок разреза {missing}. "
                         f"Есть: {sorted(columns)}")
    return keys


def _mentions(where: str, column: str) -> bool:
    """
    Упомянута ли в условии колонка — целым словом, а не частью другого имени.

    Подстрока здесь не годится: в профиле рядом с `Variant` лежит `VariantDays`,
    и условие `VariantDays > 0` проходило бы как «вариант ограничен», хотя
    в выборке оставались бы обе строки пары.
    """
    return re.search(rf"(?<![\w`]){column}(?![\w`])|`{column}`", where) is not None


def _check_where(where: str, columns, keys, source: str):
    """
    Проверить условие отбора строк.

    Два случая, которые условие обязано закрыть или не имеет права закрывать:

    - `Scope` в условии — это отфильтрованные спорные подгруппы. Они должны быть
      видны отдельной строкой, а не исчезать из отчёта;
    - источник с двумя вариантами ряда хранит две строки на пару. Если `Variant`
      не в разрезе и не ограничен условием, выручка и число пар удвоятся.

    Условие разбирается по упоминанию колонки — этого хватает, чтобы поймать
    забытый фильтр, и не хватает, чтобы гарантировать верный. Разбирать SQL
    здесь незачем: проверка ставит вопрос, а отвечает на него человек.
    """
    if _mentions(where, SCOPE):
        raise ValueError(
            f"условие отбора трогает '{SCOPE}': {where!r}. Спорные подгруппы идут в сводке "
            f"отдельной строкой и в основные итоги не входят — отфильтровывать их нельзя")

    if VARIANT in columns and VARIANT not in [key.name for key in keys] \
            and not _mentions(where, VARIANT):
        raise ValueError(
            f"источник {source} хранит по две строки на пару (варианты ряда), а '{VARIANT}' "
            f"не в разрезе и не в условии отбора: выручка и число пар удвоились бы. "
            f"Добавьте Key('{VARIANT}') в разрез или условие вида \"{VARIANT} = 'sales'\"")


def _columns(source, paths) -> set:
    """
    Колонки источника: по ним проверяются ключи, условие и зерно строки.

    `DESCRIBE` подзапросом ClickHouse не принимает, поэтому имена берутся
    из шапки пустой выборки: `LIMIT 0` читает только схему parquet.
    """
    text = clickhouse.exec_local(
        f"select * from file('{source}') limit 0 format TSVWithNames",
        paths.tmp / f'report_describe_{pathlib.Path(source).stem}.sql', return_result=True)
    return set((text or '').strip().split('\t')) - {''}


def _sql(source, keys, block, extra, measures, where, columns) -> str:
    """
    Собрать запрос разреза.

    Ключи считаются в отдельном подзапросе, а группировка идёт по готовым
    колонкам: группировать по алиасу из того же списка выборки ненадёжно.
    Вычисленный ключ заменяет одноимённую колонку источника через
    `* EXCEPT (...)` — иначе в выборке оказались бы две колонки с одним именем.
    """
    computed = [key for key in keys if key.expr]
    shadowed = [key.name for key in computed if key.name in columns]
    source_columns = '*' if not shadowed else f"* except ({', '.join(shadowed)})"
    source_columns += ''.join(f"\n                , {key.expr} as {key.name}" for key in computed)

    key_names = [key.name for key in keys]
    block_names = ', '.join(block)
    # первый ключ вне блока отличает строку итога: в ней он «сгруппирован»,
    # то есть grouping() = 1, а значения у него нет
    is_total = f"toUInt8(grouping({extra[0].name}))"

    aggregates = ''.join(f"\n                , {measure.expr} as M{i}"
                         for i, measure in enumerate(measures))
    shares = ''.join(
        f"\n                , sum(if(IsTotal = 1, M{i}, 0)) over (partition by {block_names}) as T{i}"
        for i, measure in enumerate(measures) if measure.share)

    out = []
    for key in keys:
        value = f"toString({key.name})" if key.name in block else \
            f"if(IsTotal = 1, {sql.literal(TOTAL)}, toString({key.name}))"
        out.append(f"{value} as `{key.header()}`")
    for i, measure in enumerate(measures):
        value = f"M{i}" if measure.digits is None else f"round(M{i}, {measure.digits})"
        out.append(f"{value} as `{measure.title}`")
        if measure.share:
            # nullIf: пустой блок даёт не ноль в знаменателе, а пустую ячейку
            out.append(f"round(100 * M{i} / nullIf(T{i}, 0), 1) as `{measure.share_header()}`")

    # итог блока — под своими строками: сначала разрез, потом черта под ним.
    # Сортировка берётся у самих ключей: у `Scope` и `Variant` алфавит даёт
    # не тот порядок, в котором их читают.
    #
    # Ключи сортировки считаются колонками `SortN` в `t_Share`, а не пишутся
    # прямо в `ORDER BY` итоговой выборки. Там у ключа без заголовка алиас
    # совпадает с именем колонки, и `ORDER BY` взял бы алиас — строку
    # `toString(...)` со словом «итого», — а не значение: числовой ключ
    # встал бы как 10, 2, 9
    order = [expr for key in keys if key.name in block for expr in key.order_by()] \
        + ['IsTotal'] + [expr for key in extra for expr in key.order_by()]
    sorts = ''.join(f"\n                , {expr} as Sort{i}" for i, expr in enumerate(order))

    return f"""with t_Rows as (
            select
                {source_columns}
            from file('{source}')
            {f'where {where}' if where else ''}
        )
        , t_Grouped as (
            select
                {', '.join(key_names)}
                , {is_total} as IsTotal{aggregates}
            from t_Rows
            group by grouping sets (({', '.join(key_names)}), ({block_names}))
        )
        , t_Share as (
            select *{shares}{sorts}
            from t_Grouped
        )
        select
            {(chr(10) + '            , ').join(out)}
        from t_Share
        order by {', '.join(f'Sort{i}' for i in range(len(order)))}
        format Markdown
    """


def pivot(name: str, title: str, source, paths, keys, measures, where: str = '') -> Table:
    """
    Посчитать сводную таблицу отчёта.

    `name` — имя в манифесте, `title` — заголовок в отчёте, `source` — parquet
    слоя, `where` — условие отбора строк источника (не `Scope`: см. `_check_where`).

    Правила отчёта проверяются до запроса: таблица, которая их нарушает,
    не считается. Возвращается `Table` с markdown и записью манифеста.
    """
    source = str(source)
    if not pathlib.Path(source).is_file():
        raise FileNotFoundError(f"нет источника таблицы '{name}': {source}")
    if not measures:
        raise ValueError(f"таблица '{name}' без мер: считать нечего")

    columns = _columns(source, paths)
    keys = _check_keys(keys, columns, source)
    _check_where(where, columns, keys, source)

    # заголовки колонок — это алиасы запроса, и два одинаковых роняют его
    # невнятной ошибкой ClickHouse. Чаще всего так сталкиваются заголовок автора
    # таблицы и заголовок ключа, который построитель добавил сам
    headers = [key.header() for key in keys] + [measure.title for measure in measures] \
        + [measure.share_header() for measure in measures if measure.share]
    doubled = sorted({header for header in headers if headers.count(header) > 1})
    if doubled:
        raise ValueError(f"в таблице '{name}' повторяются заголовки колонок {doubled}. "
                         f"Часть ключей построитель добавляет сам — {SCOPE} и {OUTLIER}")

    key_names = [key.name for key in keys]
    # блок — то, внутри чего доли складываются в 100%: население задают ветка
    # и сегмент, а вариант ряда берёт те же пары ещё раз
    block = tuple(n for n in (BRANCH, SCOPE, VARIANT) if n in key_names)
    # порядок колонок таблицы: ветка, сегмент, дальше — разрез в том порядке,
    # в каком его задал автор раздела, и добавленные построителем ключи в конце
    ordered = [key for key in keys if key.name == BRANCH] + \
              [key for key in keys if key.name == SCOPE] + \
              [key for key in keys if key.name not in (BRANCH, SCOPE)]
    extra = [key for key in ordered if key.name not in block]
    if not extra:
        raise ValueError(
            f"в разрезе таблицы '{name}' нет ни одного ключа кроме {list(block)}: "
            f"сводить нечего, а итог по ветке есть в любой другой таблице")

    markdown = clickhouse.exec_local(
        _sql(source, ordered, block, extra, measures, where, columns),
        paths.tmp / f'report_{name}.sql', return_result=True)
    if not markdown or '|' not in markdown:
        raise ValueError(f"таблица '{name}' не посчиталась: {markdown!r}. "
                         f"Запрос — {paths.tmp / f'report_{name}.sql'}")

    logger.info(f"report: таблица {name} — разрез {key_names}, итог по {list(block)}")
    return Table(name=name, title=title, source=source, where=where, keys=tuple(ordered),
                 block=block, measures=tuple(measures), markdown=markdown.strip())


def write_manifest(tables, save_file) -> pathlib.Path:
    """
    Манифест таблиц отчёта: имя, разрез, источник и меры каждой таблицы.

    Его читает проверка этапа 14: обойти построитель и собрать таблицу руками
    можно, но тогда её не будет в манифесте — и это видно.
    """
    save_file = pathlib.Path(save_file)
    save_file.write_text(json.dumps(
        {'built_at': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
         'tables': [table.entry() for table in tables]},
        ensure_ascii=False, indent=2), encoding='utf-8')
    return save_file
