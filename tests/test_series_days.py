"""
Тесты шага дневных рядов: разметка совпадает с ручной, поломки останавливают шаг.

Ручной расчёт описан в самих проверках: фикстура собрана так, что для каждого
случая ответ виден глазами (tests/fixtures.py).
"""

import pandas as pd
import pytest

import clickhouse
import fixtures
import params
import series_checks
import series_days
import sql


# пары фикстуры
NORMAL_UNIT = 101      # S1 × I1, шт
WEIGHT_SAME_L3 = 102   # S1 × I2, кг в той же подподгруппе
DISPUTED = 103         # S1 × I3, спорная подгруппа, дыра внутри жизни
DEAD_TAILS = 201       # S2 × I1, матричные дни до и после активности
NO_MATRIX = 204        # S2 × I4, ни одного матричного дня
EMPTY = 205            # S2 × I5, пустой ряд
AFTER_COHORT = 301     # S3 × I1, появилась после даты когорты
OUTLIER_STORE = 302    # S3 × I2, магазин вне общей массы


def _days(files) -> pd.DataFrame:
    days = pd.read_parquet(files['days'])
    days['TransDate'] = pd.to_datetime(days['TransDate'])
    return days


def _pair(days: pd.DataFrame, pair_id: int) -> pd.DataFrame:
    return days[days.ItemLocationId == pair_id].set_index('TransDate').sort_index()


def _cell(days: pd.DataFrame, pair_id: int, date: str, column: str):
    return _pair(days, pair_id).loc[pd.Timestamp(date), column]


def _arrays(files) -> pd.DataFrame:
    return pd.read_parquet(files['arrays'])


def _schema(parquet_file, paths) -> list:
    """Типы колонок parquet глазами ClickHouse: pandas их приводит к своим."""
    text = clickhouse.exec_local(f"describe table file('{parquet_file}') format TSV",
                                 paths.tmp / 'describe.sql', return_result=True)
    return [line.split('\t')[:2] for line in text.splitlines() if line.strip()]


def _series(arrays: pd.DataFrame, pair_id: int, variant: str):
    row = arrays[(arrays.ItemLocationId == pair_id) & (arrays.Variant == variant)]
    assert len(row) == 1, f"ряда {pair_id} / {variant} в слое массивов нет"
    return row.iloc[0]


def test_matrix_and_life(layer):
    """Матричные дни вне жизни пары — такие же пропуски, как дни вне матрицы."""
    files, _, _ = layer
    days = _days(files)
    pair = _pair(days, DEAD_TAILS)

    assert (pair.IsMatrix == 1).all()                       # в матрице все 14 дней
    assert pair.InLife.sum() == 5                           # активность только 15–19 мая
    assert _cell(days, DEAD_TAILS, '2026-05-15', 'InLife') == 1
    assert _cell(days, DEAD_TAILS, '2026-05-14', 'InLife') == 0
    assert _cell(days, DEAD_TAILS, '2026-05-20', 'InLife') == 0

    # вне жизни ряда нет: ни значения, ни дефицита, хотя остаток нулевой
    assert pd.isna(_cell(days, DEAD_TAILS, '2026-05-14', 'ValueSales'))
    assert _cell(days, DEAD_TAILS, '2026-05-14', 'IsDeficit') == 0
    assert pair.ValueSales.count() == 5


def test_deficit_threshold(layer):
    """Дефицит — остаток ниже порога наличия ветки, а не «ровно ноль»."""
    files, _, _ = layer
    days = _days(files)

    # шт: порог 1, вечером 13 мая остаток 0
    assert _cell(days, NORMAL_UNIT, '2026-05-13', 'IsDeficit') == 1
    assert _cell(days, NORMAL_UNIT, '2026-05-12', 'IsDeficit') == 0
    assert _pair(days, NORMAL_UNIT).IsDeficit.sum() == 1

    # кг: порог 0.1, вечером 14 мая остаток 0.05 — больше нуля, но это дефицит
    assert _cell(days, WEIGHT_SAME_L3, '2026-05-14', 'StockEndQty') == pytest.approx(0.05)
    assert _cell(days, WEIGHT_SAME_L3, '2026-05-14', 'IsDeficit') == 1
    assert _cell(days, WEIGHT_SAME_L3, '2026-05-13', 'IsDeficit') == 0
    assert _pair(days, WEIGHT_SAME_L3).IsDeficit.sum() == 1


def test_series_variants(layer):
    """«Продажи как есть» и «спрос без дефицита»: день дефицита во втором пуст."""
    files, _, _ = layer
    days = _days(files)

    assert _cell(days, NORMAL_UNIT, '2026-05-13', 'ValueSales') == pytest.approx(3.0)
    assert pd.isna(_cell(days, NORMAL_UNIT, '2026-05-13', 'ValueDemand'))
    assert _cell(days, NORMAL_UNIT, '2026-05-12', 'ValueDemand') == pytest.approx(3.0)

    pair = _pair(days, NORMAL_UNIT)
    assert pair.ValueSales.count() == 14      # все дни в матрице и в жизни
    assert pair.ValueDemand.count() == 13     # кроме дня дефицита


def test_returns_zeroed(layer):
    """Возврат обнуляет продажи и выручку, строка остаётся."""
    files, _, _ = layer
    days = _days(files)

    assert _cell(days, NORMAL_UNIT, '2026-05-15', 'IsReturn') == 1
    assert _cell(days, NORMAL_UNIT, '2026-05-15', 'SalesQty') == 0.0
    assert _cell(days, NORMAL_UNIT, '2026-05-15', 'SalesAmount') == 0.0
    assert _cell(days, NORMAL_UNIT, '2026-05-15', 'ValueSales') == 0.0
    assert len(_pair(days, NORMAL_UNIT)) == 14


def test_out_of_matrix_sales(layer):
    """Продажа вне матрицы в ряд не попадает, а в выручку пары — да."""
    files, _, _ = layer
    days = _days(files)

    assert _cell(days, DISPUTED, '2026-05-17', 'IsMatrix') == 0
    assert pd.isna(_cell(days, DISPUTED, '2026-05-17', 'ValueSales'))
    assert _cell(days, DISPUTED, '2026-05-17', 'SalesQty') == pytest.approx(0.2)

    # пара без единого матричного дня: ряда нет, но пара живая и не пустая
    pair = _pair(days, NO_MATRIX)
    assert (pair.IsMatrix == 0).all()
    assert pair.ValueSales.count() == 0
    assert (pair.IsEmpty == 0).all()
    assert (pair.InLife == 1).all()


def test_empty_series(layer):
    """Пустой ряд помечен, жизни у него нет, в слой массивов он не попадает."""
    files, _, _ = layer
    days = _days(files)
    pair = _pair(days, EMPTY)

    assert (pair.IsEmpty == 1).all()
    assert (pair.InLife == 0).all()
    assert pair.ValueSales.count() == 0
    assert (pair.Cohort == 'неприменимо').all()

    arrays = _arrays(files)
    assert arrays[arrays.ItemLocationId == EMPTY].empty


def test_cohort(layer):
    """Когорта — по дате первой активности, а не по первой строке."""
    files, _, _ = layer
    days = _days(files)

    assert (_pair(days, NORMAL_UNIT).Cohort == 'до').all()
    assert (_pair(days, AFTER_COHORT).Cohort == 'после').all()
    # пара в матрице с 11 мая, но ожила 15-го — это всё равно когорта «до»
    assert (_pair(days, DEAD_TAILS).Cohort == 'до').all()


def test_branch_scope_and_store(layer):
    """Ветка, спорная подгруппа и магазин вне общей массы."""
    files, _, _ = layer
    days = _days(files)

    assert (_pair(days, NORMAL_UNIT).Branch == 'шт').all()
    assert (_pair(days, WEIGHT_SAME_L3).Branch == 'кг').all()
    # кг и шт в одной подподгруппе — ветки не смешиваются
    assert _cell(days, NORMAL_UNIT, '2026-05-12', 'ItemIdLevel3') == \
           _cell(days, WEIGHT_SAME_L3, '2026-05-12', 'ItemIdLevel3')

    assert (_pair(days, DISPUTED).Scope == 'спорные').all()
    assert (_pair(days, NORMAL_UNIT).Scope == 'основные').all()

    assert (_pair(days, OUTLIER_STORE).OutlierStore == 1).all()
    assert (_pair(days, NORMAL_UNIT).OutlierStore == 0).all()


def test_arrays_gaps(layer):
    """Ряд массивом: длина — дни жизни, пропуск — NaN, а не ноль."""
    files, _, _ = layer
    arrays = _arrays(files)

    # дыра внутри жизни: 16 мая строки нет вовсе, 17 мая строка вне матрицы
    disputed = _series(arrays, DISPUTED, 'sales')
    values = list(disputed.Values)
    assert len(values) == 14
    assert pd.isna(values[5]) and pd.isna(values[6])
    assert values[4] == pytest.approx(0.2) and values[7] == pytest.approx(0.2)

    # день дефицита пуст только в варианте «спрос без дефицита»
    assert not pd.isna(list(_series(arrays, NORMAL_UNIT, 'sales').Values)[2])
    assert pd.isna(list(_series(arrays, NORMAL_UNIT, 'demand').Values)[2])

    # массив строится по дням жизни, а не по всему периоду
    tails = _series(arrays, DEAD_TAILS, 'sales')
    assert len(tails.Values) == 5
    assert str(tails.FirstDay) == '2026-05-15'


def test_checks_pass_on_fixture(layer):
    """На целой фикстуре проверки проходят, возвраты дают warning."""
    files, paths, _ = layer
    journals = sorted(paths.checks.glob('series_days_checks_*.parquet'))
    assert journals, 'журнал проверок не записан'

    rows = pd.read_parquet(journals[-1])
    assert not (rows.Status == 'error').any()
    returns = rows[rows.CheckName == 'days_returns_zeroed'].iloc[0]
    assert returns.Status == 'warning' and returns.Value == 1


def test_report_collects_issues(layer):
    """
    Замечания собираются в отчёт прогона, а не только идут строкой в лог.

    Отдельная позиция расчёт не останавливает, поэтому потерять её нельзя:
    разбирать замечание будет человек, возможно назавтра.
    """
    _, paths, _ = layer
    reports = sorted(paths.checks.glob('series_days_checks_*.md'))
    assert reports, 'отчёт проверок не записан'

    report = reports[-1].read_text(encoding='utf-8')
    assert '## Замечания и ошибки' in report
    assert 'days_returns_zeroed' in report          # замечание вынесено наверх
    assert '| days_key_unique |' in report          # и полная таблица тоже на месте


def test_duplicate_key_stops(workspace):
    """Дубль ключа «день × пара» останавливает шаг."""
    paths, profile = workspace
    rows = fixtures.rows()
    fixtures.write_parquet(rows + [rows[0]], paths.main_data / 'main_data_2026_05.parquet')

    with pytest.raises(ValueError, match='days_key_unique'):
        series_days.run(profile, paths=paths, checks=True)


def test_unknown_measure_is_dropped_with_warning(workspace):
    """
    Ряд с единицей вне профиля в слой не идёт, но виден в проверках.

    Ветку такому товару назначает человек, правкой профиля, поэтому расчёт
    не останавливается (docs/data_quality.md, разд. 2 и 3). Молча пропасть
    товар не должен: число отсеянных строк уходит в warning.
    """
    paths, profile = workspace
    rows = fixtures.rows()
    rows[0] = dict(rows[0], ItemMeasure='уп')
    fixtures.write_parquet(rows, paths.main_data / 'main_data_2026_05.parquet')

    files = series_days.run(profile, paths=paths)
    days = _days(files)
    assert (days.Branch != 'unknown').all()
    assert len(_pair(days, NORMAL_UNIT)) == 13  # одна строка пары отсеяна

    journal = pd.DataFrame(series_days.check(profile, paths=paths))
    measures = journal[journal.CheckName == 'source_measures_known'].iloc[0]
    assert measures.Status == 'warning' and measures.Value == 1
    assert 'уп' in measures.Details


def test_sum_mismatch_stops(layer):
    """Расхождение сумм слоя и сырья останавливает расчёт."""
    _, paths, profile = layer

    # в выгрузке появился ещё один файл, слой посчитан без него
    extra = dict(fixtures.rows()[0], TransDate='2026-05-12', ItemLocationId=999,
                 SalesQty=7.0, SalesAmount=700.0)
    fixtures.write_parquet([extra], paths.main_data / 'main_data_2026_05_extra.parquet')

    with pytest.raises(ValueError, match='days_sales_qty_шт|days_rows_шт'):
        series_days.check(profile, paths=paths)


def test_checks_run_only_on_demand(layer_no_checks):
    """Регулярный расчёт проверки не гоняет: их запускают отдельно."""
    files, paths, profile = layer_no_checks

    assert files['days'].exists()
    assert not list(paths.checks.glob('series_days_checks_*.parquet'))

    series_days.check(profile, paths=paths)
    assert list(paths.checks.glob('series_days_checks_*.parquet'))


def test_empty_period_stops(workspace):
    """Период мимо данных не должен «пройти» проверки: сверять было бы нечего."""
    paths, profile = workspace
    fixtures.write_main_data(paths.main_data)

    profile_file = profile.file.with_name('empty_period.yaml')
    profile_file.write_text(
        profile.file.read_text(encoding='utf-8')
        .replace(fixtures.PERIOD[0], '2027-01-01').replace(fixtures.PERIOD[1], '2027-01-31'),
        encoding='utf-8')

    with pytest.raises(ValueError, match='days_rows_positive'):
        series_days.run(params.load(profile_file), paths=paths, checks=True)


def test_journal_lists_failures_first(layer):
    """В журнале сначала то, из-за чего прогон встал, потом предупреждения."""
    files, paths, _ = layer
    journal = sorted(paths.checks.glob('series_days_checks_*.parquet'))[-1]
    rows = pd.read_parquet(journal)

    order = {'error': 0, 'warning': 1, 'ok': 2}
    ranks = [order[status] for status in rows.Status]
    assert ranks == sorted(ranks)


def test_layer_types_are_not_nullable(layer):
    """Флаги слоёв не должны быть Nullable: на них падают функции по массивам."""
    files, paths, _ = layer
    types = dict(_schema(files['days'], paths))

    assert types['IsDeficit'] == 'UInt8'
    assert types['InLife'] == 'UInt8'
    # пропуск ряда — это NULL, и здесь Nullable как раз нужен
    assert types['ValueSales'].startswith('Nullable')

    assert dict(_schema(files['arrays'], paths))['Deficit'] == 'Array(UInt8)'


def test_disputed_subgroups_must_be_a_list():
    """Строка вместо списка в профиле не должна превращаться в перечень букв."""
    with pytest.raises(ValueError, match='список значений'):
        sql.scope_case('НАПИТКИ')
