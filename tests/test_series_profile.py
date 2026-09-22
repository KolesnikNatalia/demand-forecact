"""
Тесты профиля рядов: цифры совпадают с ручным расчётом, поломки останавливают шаг.

Ручной расчёт описан в самих проверках: фикстура собрана так, что для каждого
случая ответ виден глазами (tests/fixtures.py).
"""

import pandas as pd
import pytest

import checks_journal
import clickhouse
import fixtures
import params
import series_profile


# пары фикстуры
NORMAL_UNIT = 101      # S1 × I1, шт: 14 дней в матрице, один возврат, один дефицит
WEIGHT_SAME_L3 = 102   # S1 × I2, кг в той же подподгруппе
DISPUTED = 103         # S1 × I3, спорная подгруппа, дыра внутри жизни
DEAD_TAILS = 201       # S2 × I1, матричные дни до и после активности
NO_MATRIX = 204        # S2 × I4, ни одного матричного дня
EMPTY = 205            # S2 × I5, пустой ряд

NOT_APPLICABLE = series_profile.NOT_APPLICABLE


def _base(files) -> pd.DataFrame:
    return pd.read_parquet(files['base'])


def _row(base: pd.DataFrame, pair_id: int, variant: str = 'sales') -> pd.Series:
    rows = base[(base.ItemLocationId == pair_id) & (base.Variant == variant)]
    assert len(rows) == 1, f"в профиле нет строки {pair_id} / {variant}"
    return rows.iloc[0]


def test_key_is_pair_and_variant(profile_layer):
    """Строка на пару и вариант ряда: ключ уникален, вариантов ровно два."""
    files, _, _ = profile_layer
    base = _base(files)

    assert set(base.Variant) == {'sales', 'demand'}
    assert not base.duplicated(['ItemLocationId', 'Variant']).any()
    assert len(base) == base.ItemLocationId.nunique() * 2
    # все пары основной фикстуры на месте: 8 пар × 2 варианта
    assert sorted(base.ItemLocationId.unique()) == sorted(
        {row['ItemLocationId'] for row in fixtures.rows()})


def test_history_and_attributes(profile_layer):
    """Длина жизни, дни в матрице, выручка, доля выручки вне матрицы, цена."""
    files, _, _ = profile_layer
    base = _base(files)

    # S1 × I1: 14 дней в матрице и в жизни, продажи 3 шт по 150 руб.,
    # 15 мая возврат обнулён — значит 13 дней с деньгами
    pair = _row(base, NORMAL_UNIT)
    assert pair.LifeDays == 14 and pair.MatrixDays == 14 and pair.MatrixDaysOutLife == 0
    assert pair.Revenue == pytest.approx(13 * 150.0)
    assert pair.SalesQtyTotal == pytest.approx(13 * 3.0)
    assert pair.Price == pytest.approx(150.0 / 3.0)       # взвешенная: Σ выручки / Σ количества
    assert pair.RevenueOutMatrixShare == pytest.approx(0.0)

    # S2 × I1: в матрице все 14 дней, но жизнь — только 15–19 мая.
    # Матричные дни вне жизни считаются отдельно и в MatrixDays не входят
    tails = _row(base, DEAD_TAILS)
    assert tails.LifeDays == 5 and tails.MatrixDays == 5 and tails.MatrixDaysOutLife == 9

    # S1 × I3: 13 строк, из них одна вне матрицы — её выручка в долю и попадает
    disputed = _row(base, DISPUTED)
    assert disputed.MatrixDays == 12
    assert disputed.Revenue == pytest.approx(13 * 40.0)
    assert disputed.RevenueOutMatrixShare == pytest.approx(40.0 / (13 * 40.0))

    # S2 × I4: продажи есть, матричных дней нет — вся выручка вне матрицы
    no_matrix = _row(base, NO_MATRIX)
    assert no_matrix.MatrixDays == 0
    assert no_matrix.RevenueOutMatrixShare == pytest.approx(1.0)


def test_demand_class_by_matrix_days(profile_layer):
    """ADI, CV² и доля нулей — по дням варианта, а вариант вырезает дни дефицита."""
    files, _, _ = profile_layer
    base = _base(files)

    # «продажи как есть»: 14 матричных дней жизни, продажи в 13 из них
    # (15 мая — обнулённый возврат)
    sales = _row(base, NORMAL_UNIT, 'sales')
    assert sales.VariantDays == 14 and sales.SalesDays == 13
    assert sales.ZeroShare == pytest.approx(1 / 14)
    assert sales.SalesDayShare == pytest.approx(13 / 14)
    assert sales.Adi == pytest.approx(14 / 13)
    assert sales.Cv2 == pytest.approx(0.0)          # все ненулевые продажи по 3 шт
    assert sales.SalesLevel == pytest.approx(39 / 14)

    # «спрос без дефицита»: 13 мая распродан, этот день из ряда убран
    demand = _row(base, NORMAL_UNIT, 'demand')
    assert demand.VariantDays == 13 and demand.SalesDays == 12
    assert demand.Adi == pytest.approx(13 / 12)
    assert demand.ZeroShare == pytest.approx(1 / 13)


def test_zero_share_counted_only_on_matrix_days(profile_layer):
    """
    Пересчёт по всем строкам даёт другое число — значит считалось по матричным дням.

    У S2 × I1 в матрице все 14 дней, но жизнь — только 5. Девять матричных дней
    без товара в долю нулей не идут: это не «спроса не было», а «позиции не было».
    """
    files, _, _ = profile_layer
    base = _base(files)
    pair = _row(base, DEAD_TAILS)

    days = pd.read_parquet(files['days'])
    all_rows = days[days.ItemLocationId == DEAD_TAILS]
    over_all = 1 - (all_rows.SalesQty > 0).sum() / len(all_rows)
    adi_over_all = len(all_rows) / (all_rows.SalesQty > 0).sum()

    assert pair.ZeroShare == pytest.approx(0.0)      # по дням жизни продажи каждый день
    assert over_all == pytest.approx(9 / 14)         # по всем строкам — совсем другое
    assert pair.Adi == pytest.approx(1.0)
    assert adi_over_all == pytest.approx(14 / 5)


def test_deficit_shares(profile_layer):
    """Дефицит меряется по матричным дням жизни и одинаков в обоих вариантах."""
    files, _, _ = profile_layer
    base = _base(files)

    unit = _row(base, NORMAL_UNIT)
    assert unit.DeficitDays == 1
    assert unit.DeficitDayShare == pytest.approx(1 / 14)
    assert unit.DeficitSalesShare == pytest.approx(3.0 / 39.0)

    # в варианте «спрос без дефицита» дни дефицита из ряда убраны, но доля —
    # свойство пары: пересчёт по дням варианта дал бы тождественный ноль
    assert _row(base, NORMAL_UNIT, 'demand').DeficitDayShare == pytest.approx(1 / 14)

    # кг: остаток 0.05 кг — больше нуля, но ниже порога наличия
    weight = _row(base, WEIGHT_SAME_L3)
    assert weight.DeficitDays == 1 and weight.DeficitDayShare == pytest.approx(1 / 14)


def test_quadrants(quadrant_profile):
    """Каждый квадрант Syntetos–Boylan находится там, где заложен в фикстуре."""
    files, _, _ = quadrant_profile
    base = _base(files)

    for item, expected in fixtures.QUADRANT_EXPECTED.items():
        pair = _row(base, fixtures.PAIR_IDS[('S9', item)])
        assert pair.Quadrant == expected, f"{item}: ждали «{expected}», получили «{pair.Quadrant}»"

    # ручной расчёт для крайних случаев
    smooth = _row(base, fixtures.PAIR_IDS[('S9', 'Q1')])
    assert smooth.Adi == pytest.approx(1.0) and smooth.Cv2 == pytest.approx(0.0)

    lumpy = _row(base, fixtures.PAIR_IDS[('S9', 'Q4')])
    assert lumpy.Adi == pytest.approx(14 / 7)
    # продажи 1 (четырежды) и 9 (трижды): среднее 31/7, дисперсия по выборке
    values = pd.Series([1.0] * 4 + [9.0] * 3)
    assert lumpy.Cv2 == pytest.approx(values.var() / values.mean() ** 2)


def test_no_matrix_days_is_not_applicable(profile_layer):
    """
    У пары без матричных дней класс спроса и ABC по частоте — «неприменимо».

    В выручку и количество её продажи входят: деньги есть, и в ABC по ним
    она участвует наравне с остальными.
    """
    files, _, _ = profile_layer
    pair = _row(_base(files), NO_MATRIX)

    assert pair.MatrixDays == 0
    assert pair.Quadrant == NOT_APPLICABLE
    assert pd.isna(pair.Adi) and pd.isna(pair.Cv2) and pd.isna(pair.ZeroShare)
    assert pd.isna(pair.DeficitDayShare)

    assert pair.AbcFreqStore == NOT_APPLICABLE
    assert pair.AbcFreqSubgroup == NOT_APPLICABLE
    assert pair.AbcRevenueStore in {'A', 'B', 'C'}
    assert pair.AbcQtyStore in {'A', 'B', 'C'}


def test_empty_series_out_of_abc(profile_layer):
    """Пустой ряд помечен, истории у него нет, в ABC он не участвует."""
    files, _, _ = profile_layer
    base = _base(files)
    pair = _row(base, EMPTY)

    assert pair.IsEmpty == 1
    assert pd.isna(pair.LifeDays) and pd.isna(pair.Price)
    assert pair.Quadrant == NOT_APPLICABLE
    assert all(pair[name] == NOT_APPLICABLE for name in series_profile.ABC_COLUMNS)

    # в основании ABC пустой ряд не участвует: сумма по магазину S2 — только живые пары
    alive = base[(base.Variant == 'sales') & (base.LocationId == 'S2') & (base.IsEmpty == 0)]
    assert set(alive.AbcRevenueStore) <= {'A', 'B', 'C'}


def test_abc_classes(profile_layer):
    """
    ABC по выручке магазина совпадает с ручным расчётом.

    Магазин S1: I2 — 4200 руб., I1 — 1950, I3 — 520, итого 6670. Порог A — 80%:
    I2 идёт первой с накопленной долей 0, I1 — с 0.63, обе A. Перед I3 накоплено
    0.92 — это уже за границей A, но до 95%, значит B.
    """
    files, _, _ = profile_layer
    base = _base(files)

    assert _row(base, WEIGHT_SAME_L3).AbcRevenueStore == 'A'
    assert _row(base, NORMAL_UNIT).AbcRevenueStore == 'A'
    assert _row(base, DISPUTED).AbcRevenueStore == 'B'

    # внутри своей подгруппы I3 единственная, поэтому она же и A
    assert _row(base, DISPUTED).AbcRevenueSubgroup == 'A'

    # класс одинаков в обеих строках пары: ABC — сегмент ассортимента,
    # а не свойство способа измерить ряд
    assert _row(base, DISPUTED, 'demand').AbcRevenueStore == 'B'


def test_abc_quantity_does_not_mix_branches(profile_layer):
    """
    Количество складывается только внутри ветки: `кг` со `шт` не смешиваются.

    В S1 у `кг` две пары — I2 (21 кг) и I3 (2.6 кг): перед I3 накоплено 0.89,
    это класс B. Если бы ветки сложили, итог стал бы 62.6 и I3 ушла бы в C.
    """
    files, _, _ = profile_layer
    base = _base(files)

    assert _row(base, DISPUTED).AbcQtyStore == 'B'
    assert _row(base, WEIGHT_SAME_L3).AbcQtyStore == 'A'
    assert _row(base, NORMAL_UNIT).AbcQtyStore == 'A'  # в S1 это единственная пара `шт`


def test_abc_thresholds_come_from_profile(layer):
    """Границы классов задаёт профиль параметров, а не код."""
    _, paths, profile = layer

    strict_file = profile.file.with_name('strict_abc.yaml')
    strict_file.write_text(
        profile.file.read_text(encoding='utf-8').replace('[0.80, 0.95]', '[0.50, 0.95]'),
        encoding='utf-8')

    files = series_profile.run(params.load(strict_file), paths=paths)
    base = _base(files)

    # перед I1 накоплено 0.63: при границе A в 80% это был класс A, при 50% — уже B
    assert _row(base, NORMAL_UNIT).AbcRevenueStore == 'B'
    assert _row(base, WEIGHT_SAME_L3).AbcRevenueStore == 'A'


def test_checks_pass_on_fixture(profile_layer):
    """На целой фикстуре проверки проходят, а пустые ряды дают замечание."""
    _, paths, _ = profile_layer
    journal = paths.checks / 'series_profile_checks.parquet'
    assert journal.is_file(), 'журнал проверок не записан'

    rows = pd.read_parquet(journal)
    assert not (rows.Status == 'error').any()
    empty = rows[rows.CheckName == 'profile_empty_pairs'].iloc[0]
    assert empty.Status == 'warning' and empty.Value == 1

    report = paths.checks / 'series_profile_checks.md'
    assert 'profile_empty_pairs' in report.read_text(encoding='utf-8')


def test_checks_stop_on_sum_mismatch(profile_layer):
    """Расхождение сумм профиля и сырья останавливает прогон."""
    _, paths, profile = profile_layer

    # в выгрузке появился ещё один файл, профиль посчитан без него
    extra = dict(fixtures.rows()[0], TransDate='2026-05-12', ItemLocationId=999,
                 SalesQty=7.0, SalesAmount=700.0)
    fixtures.write_parquet([extra], paths.main_data / 'main_data_2026_05_extra.parquet')

    with pytest.raises(ValueError, match='profile_sales_qty_шт|profile_pairs_шт'):
        series_profile.check(profile, paths=paths)


def test_empty_series_has_no_days_out_of_life(profile_layer):
    """
    У пустого ряда нет и дней вне жизни: он помечен и в статистики не входит.

    Формально вне жизни у него все матричные дни, и без обнуления сумма
    «матричных дней вне жизни» по слою удваивалась бы за счёт пар, которых
    в анализе нет (решение 2026-09-20).
    """
    files, _, _ = profile_layer
    base = _base(files)

    assert _row(base, EMPTY).MatrixDaysOutLife == 0        # 14 матричных дней, но ряд пустой
    assert _row(base, DEAD_TAILS).MatrixDaysOutLife == 9   # живая пара считается как прежде

    # сумма по слою — это только живые пары
    sales = base[base.Variant == 'sales']
    assert sales.MatrixDaysOutLife.sum() == sales[sales.IsEmpty == 0].MatrixDaysOutLife.sum()


def test_abc_ties_are_ordered_by_revenue(quadrant_profile):
    """
    При равном основании класс повыше достаётся паре с большей выручкой.

    У Q3 и Q4 частота продаж одинаковая — 7 дней из 14, — но Q4 дороже.
    Порядок по выручке расходится с порядком по `ItemLocationId` (903 < 904),
    поэтому видно, что границу решает выручка, а не номер пары.
    """
    files, _, _ = quadrant_profile
    base = _base(files)

    cheap = _row(base, fixtures.PAIR_IDS[('S9', 'Q3')])
    rich = _row(base, fixtures.PAIR_IDS[('S9', 'Q4')])

    assert cheap.SalesDays == rich.SalesDays == 7
    assert rich.Revenue > cheap.Revenue
    assert rich.AbcFreqStore == 'A' and cheap.AbcFreqStore == 'B'


def test_demand_class_check_catches_unknown_quadrant(profile_layer):
    """
    Проверка класса спроса ловит значение вне перечня, а не только пустую строку.

    `multiIf` всегда возвращает непустой литерал, поэтому проверка на `''`
    не поймала бы ничего и молча говорила бы «ок».
    """
    files, paths, profile = profile_layer

    # подменяем квадрант у одной пары на значение, которого шаг не выдаёт
    broken = paths.analysis / 'broken_base.parquet'
    clickhouse.exec_local(
        f"""select * except Quadrant
                 , if(ItemLocationId = {NORMAL_UNIT}, 'ерунда', Quadrant) as Quadrant
            from file('{files['base']}')""",
        paths.tmp / 'broken_base.sql', broken, 'Parquet')
    broken.replace(files['base'])

    with pytest.raises(ValueError, match='profile_filled_demand_class'):
        series_profile.check(profile, paths=paths)


def test_empty_journal_stops_the_run(workspace):
    """
    Журнал без единой строки — это поломка запроса проверок, а не «всё сошлось».

    Иначе слой объявили бы проверенным, не сверив ни одной величины, — и так
    повёл бы себя каждый следующий шаг: журнал у них общий.
    """
    paths, _ = workspace
    paths.ensure()

    empty_body = """select 'проба' as CheckName, 'error' as Level, toFloat64(0) as Value
                         , toFloat64(0) as Threshold, '' as Details
                    from t_X where 1 = 0"""
    with pytest.raises(ValueError, match='журнал проверок пуст'):
        checks_journal.run('probe', 'Проба', 't_X as (select 1 as x)', empty_body,
                           paths, 'проба не прошла')


def test_demand_class_bounds_are_validated(layer):
    """Границы класса спроса подставляются в SQL, поэтому проверяются до запроса."""
    _, paths, profile = layer

    bad_file = profile.file.with_name('bad_demand_class.yaml')
    bad_file.write_text(
        profile.file.read_text(encoding='utf-8').replace('adi: 1.32', "adi: '1,32'"),
        encoding='utf-8')

    with pytest.raises(ValueError, match='demand_class'):
        series_profile.run(params.load(bad_file), paths=paths)


def test_checks_only_needs_computed_profile(workspace):
    """Проверки по непосчитанному профилю не «проходят», а говорят, чего нет."""
    paths, profile = workspace
    with pytest.raises(FileNotFoundError, match='profile_base.parquet'):
        series_profile.check(profile, paths=paths)
