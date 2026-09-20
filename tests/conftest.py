"""
Общая обвязка тестов: каталог данных теста внутри проекта и свой профиль.

Тест считает в `data/test/<имя теста>/` — там те же слои, что у рабочего прогона
(`main_data`, `prepared`, `checks`, `tmp`). Каталог остаётся после прогона: по нему
смотрят, что именно получилось, и открывают те же parquet и файлы SQL-команд, что
и на полной выгрузке. Перед тестом его содержимое удаляется, чтобы не смотреть
на прошлый прогон.

Корень данных подменяется переменной `DF_DATA_DIR` — тем же механизмом, которым
данные переносят на другой диск или сервер. `config.yaml` берётся рабочий: тест
проверяет ту конфигурацию слоёв, что в проекте, а не её копию.
"""

import pathlib
import shutil
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[1]
for module_dir in ('src/lib', 'src/checks', 'src/preprocessing'):
    sys.path.append(f"{ROOT}/{module_dir}")
sys.path.append(str(pathlib.Path(__file__).resolve().parent))  # fixtures.py

import fixtures  # noqa: E402
import params  # noqa: E402
import series_days  # noqa: E402
import settings  # noqa: E402


TEST_DATA = ROOT / 'data' / 'test'  # корень данных тестов, в git не хранится

# Профиль фикстуры отличается от рабочего только периодом и порогом магазинов
# вне общей массы: 500 руб. за период — между выручкой S2 и S3.
PROFILE = """
period: {{start: {start}, end: {end}}}
cohort: {{date: {cohort}}}
scope:
  disputed_subgroups: [{disputed}]
  outlier_stores: {{revenue_below_mln: 0.0005}}
branches:
  кг: [кг]
  шт: [шт]
returns: zero
stock_threshold:
  шт: 1
  кг: 0.1
life: active_span
"""


@pytest.fixture
def workspace(request, monkeypatch):
    """Каталоги и профиль теста. Сырьё ещё не записано."""
    test_dir = TEST_DATA / request.node.name
    shutil.rmtree(test_dir, ignore_errors=True)
    test_dir.mkdir(parents=True)

    # переменная важнее config.yaml, поэтому она и задаёт корень данных теста:
    # с рабочим значением из .env тест писал бы поверх настоящей выгрузки
    monkeypatch.setenv(settings.DATA_DIR_ENV, str(test_dir))
    paths = settings.load().paths
    assert paths.data == test_dir, f"тест считает не в своём каталоге: {paths.data}"

    profile_file = test_dir / 'series_analysis.yaml'
    profile_file.write_text(PROFILE.format(start=fixtures.PERIOD[0], end=fixtures.PERIOD[1],
                                           cohort=fixtures.COHORT_DATE,
                                           disputed=fixtures.DISPUTED_SUBGROUP),
                            encoding='utf-8')

    paths.main_data.mkdir(parents=True, exist_ok=True)
    return paths, params.load(profile_file)


@pytest.fixture
def layer(workspace):
    """Слой дневных рядов с проверками: так шаг гоняют, когда данные новые."""
    paths, profile = workspace
    fixtures.write_main_data(paths.main_data)
    files = series_days.run(profile, paths=paths, checks=True)
    return files, paths, profile


@pytest.fixture
def layer_no_checks(workspace):
    """Слой без проверок: так шаг гоняют в регулярном расчёте и экспериментах."""
    paths, profile = workspace
    fixtures.write_main_data(paths.main_data)
    files = series_days.run(profile, paths=paths)
    return files, paths, profile
