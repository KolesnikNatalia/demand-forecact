"""
Тесты построителя сводных таблиц и каркаса отчёта.

Построитель держит два правила отчёта, которые глазами в готовой таблице
не поймать: ветка всегда в ключе разреза, а спорные подгруппы идут отдельной
строкой и в основные итоги не входят. Поэтому проверяется и то, что нарушающая
их таблица не строится, и то, что у правильной таблицы цифры сходятся с ручным
расчётом по фикстуре.

Пары фикстуры (tests/fixtures.py) разложены по веткам и сегментам так:

| ветка | сегмент | пары | выручка |
|---|---|---|---|
| кг | основные | 102, 205 (пустой ряд), 302 | 4200 + 0 + 125 = 4325 |
| кг | спорные | 103 (`НАПИТКИ`) | 13 × 40 = 520 |
| шт | основные | 101, 201, 204, 301 | 1950 + 250 + 600 + 225 = 3025 |

Отсюда видно, что итог основного ассортимента `кг` — это 3 пары и 4325 руб.,
а не 4 пары и 4845 руб. Если спорная подгруппа подмешается в итог, тест упадёт.
"""

import json

import pytest

import fixtures
import params
import report_tables
import series_report
import sql


# выручка пар фикстуры по ветке и сегменту: считана руками из tests/fixtures.py
KG_MAIN = {'pairs': 3, 'revenue': 4200.0 + 0.0 + 125.0}
KG_DISPUTED = {'pairs': 1, 'revenue': 13 * 40.0}
PCS_MAIN = {'pairs': 4, 'revenue': 1950.0 + 250.0 + 600.0 + 225.0}

BRANCH_TITLE, SCOPE_TITLE = 'ветка', 'сегмент'
PAIRS, REVENUE = 'пар', 'выручка'


def _rows(markdown: str) -> list:
    """Markdown-таблица ClickHouse обратно в строки-словари."""
    lines = [line.strip() for line in markdown.splitlines() if line.strip().startswith('|')]
    assert len(lines) >= 3, f"это не таблица: {markdown!r}"

    def cells(line):
        return [cell.strip() for cell in line.strip('|').split('|')]

    header = cells(lines[0])
    # вторая строка — разделитель markdown («|:-|-:|»), данных в ней нет
    return [dict(zip(header, cells(line))) for line in lines[2:]]


def _pick(rows: list, **where) -> dict:
    found = [row for row in rows if all(row[key] == value for key, value in where.items())]
    assert len(found) == 1, f"ждали одну строку {where}, нашли {len(found)}"
    return found[0]


def _pairs_table(files, paths, keys=None, measures=None, where="Variant = 'sales'"):
    """Разрез по ветке и когорте — на нём проверяются правила построителя."""
    return report_tables.pivot(
        'test_pairs', 'Пары и выручка', files['base'], paths,
        keys=keys or [report_tables.Key('Branch', BRANCH_TITLE), report_tables.Key('Cohort', 'когорта')],
        measures=measures or [report_tables.Measure(PAIRS, 'toUInt64(count())', share=True),
                              report_tables.Measure(REVENUE, 'sum(Revenue)', digits=2, share=True)],
        where=where)


# --- правила построителя -----------------------------------------------------

def test_table_without_branch_fails(profile_layer):
    """Таблица без ветки в ключе разреза не строится: `кг` и `шт` не складываются."""
    files, paths, _ = profile_layer

    with pytest.raises(ValueError, match='Branch'):
        report_tables.pivot('no_branch', 'Без ветки', files['base'], paths,
                     keys=[report_tables.Key('Cohort')],
                     measures=[report_tables.Measure(PAIRS, 'toUInt64(count())')],
                     where="Variant = 'sales'")


def test_disputed_subgroups_are_separate_row(profile_layer):
    """
    Спорные подгруппы — своя строка и свой итог, даже если о них не просили.

    `Scope` в разрезе не задан: построитель добавляет его сам.
    """
    files, paths, _ = profile_layer
    rows = _rows(_pairs_table(files, paths).markdown)

    scopes = {row[SCOPE_TITLE] for row in rows}
    assert scopes == {sql.SCOPE_MAIN, sql.SCOPE_DISPUTED}

    disputed = _pick(rows, **{BRANCH_TITLE: 'кг', SCOPE_TITLE: sql.SCOPE_DISPUTED,
                              'когорта': report_tables.TOTAL})
    assert int(disputed[PAIRS]) == KG_DISPUTED['pairs']
    assert float(disputed[REVENUE]) == pytest.approx(KG_DISPUTED['revenue'])


def test_disputed_not_in_main_total(profile_layer):
    """
    В основной итог спорные подгруппы не входят.

    Итог `кг` по основному ассортименту — 3 пары и 4325 руб. Строка `НАПИТКИ`
    добавила бы четвёртую пару и ещё 520 руб., и заметить это в готовой таблице
    было бы нечем.
    """
    files, paths, _ = profile_layer
    rows = _rows(_pairs_table(files, paths).markdown)

    total = _pick(rows, **{BRANCH_TITLE: 'кг', SCOPE_TITLE: sql.SCOPE_MAIN,
                           'когорта': report_tables.TOTAL})
    assert int(total[PAIRS]) == KG_MAIN['pairs']
    assert float(total[REVENUE]) == pytest.approx(KG_MAIN['revenue'])

    # и итог ветки не собран из двух сегментов сразу
    both = KG_MAIN['revenue'] + KG_DISPUTED['revenue']
    assert float(total[REVENUE]) != pytest.approx(both)

    pcs = _pick(rows, **{BRANCH_TITLE: 'шт', SCOPE_TITLE: sql.SCOPE_MAIN,
                         'когорта': report_tables.TOTAL})
    assert int(pcs[PAIRS]) == PCS_MAIN['pairs']
    assert float(pcs[REVENUE]) == pytest.approx(PCS_MAIN['revenue'])


def test_disputed_cannot_be_filtered_out(profile_layer):
    """Отфильтровать спорные условием нельзя: они должны быть видны в сводке."""
    files, paths, _ = profile_layer

    with pytest.raises(ValueError, match='Scope'):
        _pairs_table(files, paths,
                     where=f"Variant = 'sales' and Scope = {sql.literal(sql.SCOPE_MAIN)}")


def test_variant_must_be_in_key_or_filter(profile_layer):
    """
    Источник с двумя вариантами ряда без фильтра удвоил бы выручку и число пар.

    Это не ошибка данных: в профиле по две строки на пару по построению.
    Поэтому вариант обязан быть либо в разрезе, либо в условии отбора.
    """
    files, paths, _ = profile_layer

    with pytest.raises(ValueError, match='Variant'):
        _pairs_table(files, paths, where='')

    # колонка с похожим именем вариант не ограничивает: `VariantDays > 0` оставляет
    # обе строки пары, и подстрочная проверка пропускала такую таблицу с удвоенными
    # цифрами (ревью этапа 3, 2026-09-22)
    with pytest.raises(ValueError, match='Variant'):
        _pairs_table(files, paths, where='VariantDays > 0')

    # вариант в разрезе — та же таблица считается
    rows = _rows(_pairs_table(
        files, paths,
        keys=[report_tables.Key('Branch', BRANCH_TITLE), report_tables.Key('Cohort', 'когорта'),
              report_tables.Key('Variant', 'вариант')],
        where='').markdown)
    total = _pick(rows, **{BRANCH_TITLE: 'кг', SCOPE_TITLE: sql.SCOPE_MAIN,
                           'вариант': 'sales', 'когорта': report_tables.TOTAL})
    assert float(total[REVENUE]) == pytest.approx(KG_MAIN['revenue'])


def test_shares_are_counted_from_sums(profile_layer):
    """
    Доля — это мера строки, делённая на меру итога своего блока, а не среднее долей.

    Проверяется на выручке `кг`: доли строк складываются в 100%, и каждая равна
    своему отношению к итогу.
    """
    files, paths, _ = profile_layer
    rows = _rows(_pairs_table(files, paths).markdown)

    block = [row for row in rows
             if row[BRANCH_TITLE] == 'кг' and row[SCOPE_TITLE] == sql.SCOPE_MAIN]
    detail = [row for row in block if row['когорта'] != report_tables.TOTAL]
    total = _pick(block, **{'когорта': report_tables.TOTAL})

    assert float(total[f"{REVENUE}, %"]) == pytest.approx(100.0)
    assert sum(float(row[f"{PAIRS}, %"]) for row in detail) == pytest.approx(100.0, abs=0.2)
    for row in detail:
        assert float(row[f"{REVENUE}, %"]) == pytest.approx(
            100 * float(row[REVENUE]) / float(total[REVENUE]), abs=0.05)


def test_outlier_store_added_to_store_cut(profile_layer):
    """
    В разрезе по магазину флаг «вне общей массы» добавляется в ключ сам.

    В задачу такие магазины входят, но растворяться в общей строке не должны.
    В фикстуре это S3: его выручка ниже порога профиля.
    """
    files, paths, _ = profile_layer
    table = _pairs_table(files, paths,
                         keys=[report_tables.Key('Branch', BRANCH_TITLE),
                               report_tables.Key('LocationId', 'магазин')])

    assert report_tables.OUTLIER in [key.name for key in table.keys]
    rows = _rows(table.markdown)
    outliers = {row['магазин'] for row in rows
                if row['тип магазина'] == 'вне общей массы' and row['магазин'] != report_tables.TOTAL}
    assert outliers == {'S3'}


def test_doubled_column_header_fails(profile_layer):
    """Заголовок, совпавший с заголовком ключа, который добавлен сам, — ошибка."""
    files, paths, _ = profile_layer

    with pytest.raises(ValueError, match='заголовки'):
        _pairs_table(files, paths,
                     keys=[report_tables.Key('Branch', BRANCH_TITLE),
                           report_tables.Key('Cohort', SCOPE_TITLE)])


def test_numeric_key_sorts_as_number(profile_layer):
    """
    Числовой ключ без заголовка сортируется как число, а не как строка.

    У такого ключа алиас в итоговой выборке совпадает с именем колонки, и прямой
    `ORDER BY` взял бы строку `toString(...)`: 0, 14, 5 вместо 0, 5, 14.
    В фикстуре у пар `кг` основного ассортимента 14, 0 (пустой ряд) и 5 матричных дней.
    """
    files, paths, _ = profile_layer
    rows = _rows(_pairs_table(files, paths,
                              keys=[report_tables.Key('Branch', BRANCH_TITLE),
                                    report_tables.Key('MatrixDays')]).markdown)

    days = [row['MatrixDays'] for row in rows
            if row[BRANCH_TITLE] == 'кг' and row[SCOPE_TITLE] == sql.SCOPE_MAIN
            and row['MatrixDays'] != report_tables.TOTAL]
    assert days == ['0', '5', '14']


def test_ordinary_stores_before_outliers(profile_layer):
    """
    Обычные магазины идут раньше магазинов вне общей массы — как основной
    ассортимент раньше спорного.

    Сеть здесь одна на все магазины (вычисленный ключ), поэтому внутри неё флаг
    делит строки надвое, и порядок видно. В ветке `шт` S1 и S2 обычные, S3 — нет.
    """
    files, paths, _ = profile_layer
    rows = _rows(_pairs_table(files, paths,
                              keys=[report_tables.Key('Branch', BRANCH_TITLE),
                                    report_tables.Key('LocationNetwork', 'сеть',
                                                      expr="'все сети'")]).markdown)

    kinds = [row['тип магазина'] for row in rows
             if row[BRANCH_TITLE] == 'шт' and row[SCOPE_TITLE] == sql.SCOPE_MAIN
             and row['сеть'] != report_tables.TOTAL]
    assert kinds == [report_tables.OUTLIER_NO, report_tables.OUTLIER_YES]


def test_manifest_records_every_table(report):
    """Каждая таблица отчёта попала в манифест, и ветка у неё в ключе."""
    files, _, _ = report
    manifest = json.loads(files['manifest'].read_text(encoding='utf-8'))

    assert manifest['tables'], 'манифест пуст'
    for entry in manifest['tables']:
        assert report_tables.BRANCH in entry['keys'], entry
        assert report_tables.SCOPE in entry['keys'], entry


# --- каркас отчёта -----------------------------------------------------------

def test_report_header_has_date_and_fingerprint(report):
    """Шапка: дата расчёта и отпечаток данных — период, число строк, хеш."""
    files, _, profile = report
    text = files['report'].read_text(encoding='utf-8')

    assert '**Дата расчёта:**' in text
    assert str(profile.value('period.start')) in text
    assert 'хеш содержимого выгрузки' in text
    # строк выгрузки в периоде столько же, сколько строк в фикстуре
    import fixtures
    assert f"| строк выгрузки в периоде | {len(fixtures.rows())} |" in text


def test_report_has_population_section(report):
    """Раздел «Популяция» собран из профиля и попал в отчёт."""
    files, _, _ = report
    text = files['report'].read_text(encoding='utf-8')
    manifest = json.loads(files['manifest'].read_text(encoding='utf-8'))

    assert '## Популяция' in text
    for entry in manifest['tables']:
        assert entry['title'] in text


def test_report_keeps_manual_text(report):
    """
    Выводы, написанные руками, пересборку переживают, а шапка пересчитывается.
    """
    files, paths, profile = report
    manual = 'Вывод человека: хвост `кг` держит меньше процента выручки.'

    text = files['report'].read_text(encoding='utf-8')
    files['report'].write_text(text.replace('<!-- сюда пишутся выводы руками -->', manual),
                               encoding='utf-8')

    series_report.run(profile, paths=paths, report_file=files['report'])
    again = files['report'].read_text(encoding='utf-8')

    assert manual in again
    assert '**Дата расчёта:**' in again
    # шапка не задвоилась: маркер по-прежнему один
    assert again.count(series_report.MARK_START.format(name='header')) == 1


def test_report_file_has_no_default(profile_layer):
    """
    Без явного пути отчёт не собирается.

    Каталог `docs` лежит в репозитории, и `DF_DATA_DIR` его не уводит. С умолчанием
    вызов на фикстуре молча переписал бы рабочий `docs/series_analysis.md`.
    """
    _, paths, profile = profile_layer

    with pytest.raises(TypeError, match='report_file'):
        series_report.run(profile, paths=paths)


@pytest.mark.parametrize('damage', ['closing_marker', 'doubled_marker'])
def test_broken_markers_stop_without_touching_report(report, damage):
    """
    Сломанная разметка останавливает сборку, а файл остаётся как был.

    Без закрывающего маркера шапки ближайший `<!-- /auto -->` — уже от раздела
    «Популяция», и замена до него стёрла бы заголовок раздела вместе с чужим
    блоком. Задвоенный маркер — та же неясность: какой блок заменять.
    """
    files, paths, profile = report
    header = series_report.MARK_START.format(name='header')
    text = files['report'].read_text(encoding='utf-8')

    if damage == 'closing_marker':
        at = text.index(header)
        end = text.index(series_report.MARK_END, at)
        broken = text[:end] + text[end + len(series_report.MARK_END):]
    else:
        broken = text + f"\n{header}\nлишний блок\n{series_report.MARK_END}\n"
    files['report'].write_text(broken, encoding='utf-8')

    with pytest.raises(ValueError, match='маркер'):
        series_report.run(profile, paths=paths, report_file=files['report'])

    assert files['report'].read_text(encoding='utf-8') == broken


def test_check_catches_report_of_another_profile(report):
    """
    Проверка отчёта сверяет его с профилем: отчёт, собранный по другому периоду,
    не проходит. Раньше профиль в `check()` принимался и не использовался,
    и `--checks-only --profile другой.yaml` ничего с ним не сверял.
    """
    files, paths, profile = report
    other = profile.file.with_name('other_period.yaml')
    other.write_text(profile.file.read_text(encoding='utf-8')
                     .replace(fixtures.PERIOD[1], '2026-05-23'), encoding='utf-8')

    with pytest.raises(ValueError, match='период'):
        series_report.check(params.load(other), paths=paths, report_file=files['report'])

    # тот же отчёт с его собственным профилем проходит
    series_report.check(profile, paths=paths, report_file=files['report'])
