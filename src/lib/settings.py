"""
Настройки проекта: пути и подключение к ClickHouse.

Единственное место, где читаются `config.yaml` и `.env`. Остальной код берёт
готовые значения:

    from settings import paths, config

    paths.main_data / 'main_data_*.parquet'
    paths.prepared / 'grid.parquet'
    config.clickhouse.host

Правила:

- здесь только каталоги. Имена файлов и маски задаёт шаг, который их читает
  или пишет: источники могут меняться, а каталоги остаются;
- корень проекта определяется от этого файла, а не от текущего каталога:
  скрипт можно запускать откуда угодно, в том числе из Prefect;
- относительный путь в конфиге считается от корня проекта (слои — от корня
  данных), абсолютный берётся как есть;
- переменная окружения важнее `.env`: `load_dotenv` не перезаписывает уже
  заданные переменные, поэтому `DF_DATA_DIR` на сервере перекрывает локальный;
- при загрузке проверяется только состав ключей. Каталоги проверяет и создаёт
  `paths.ensure()` — его вызывают в начале прогона, а не при импорте.
"""

import os
import pathlib
from dataclasses import dataclass, fields

import yaml
from dotenv import load_dotenv


ROOT = pathlib.Path(__file__).resolve().parents[2]  # src/lib/settings.py -> корень проекта
CONFIG_FILE = ROOT / 'config.yaml'
ENV_FILE = ROOT / '.env'

DATA_DIR_ENV = 'DF_DATA_DIR'  # переопределение корня данных


@dataclass(frozen=True)
class Paths:
    root: pathlib.Path       # корень проекта
    data: pathlib.Path       # корень данных
    main_data: pathlib.Path  # сырые факты
    prepared: pathlib.Path   # подготовленный слой
    features: pathlib.Path   # признаки
    forecast: pathlib.Path   # прогнозы и заказ
    checks: pathlib.Path     # результаты проверок качества
    tmp: pathlib.Path        # промежуточные файлы и SQL-команды
    logs: pathlib.Path       # логи

    def ensure(self):
        """
        Подготовить каталоги к прогону.

        Сырые данные должны уже лежать на месте, иначе прогон бессмыслен.
        Каталоги результатов создаются: `INTO OUTFILE` в ClickHouse их не создаёт.
        """
        if not self.main_data.is_dir():
            raise FileNotFoundError(f"нет каталога сырых данных: {self.main_data}")
        for path in (self.prepared, self.features, self.forecast, self.checks, self.tmp, self.logs):
            path.mkdir(parents=True, exist_ok=True)


@dataclass(frozen=True)
class ClickHouseConnection:
    host: str | None
    port: str | None
    user: str | None
    password: str | None


@dataclass(frozen=True)
class Config:
    paths: Paths
    clickhouse: ClickHouseConnection


def _resolve(value, base: pathlib.Path) -> pathlib.Path:
    """Путь из конфига: абсолютный — как есть, относительный — от base."""
    path = pathlib.Path(value)
    return path if path.is_absolute() else base / path


def _check_keys(section: dict, expected: set, where: str):
    """Опечатка в имени ключа должна ронять загрузку, а не давать путь по умолчанию."""
    missing = expected - section.keys()
    unknown = section.keys() - expected
    if missing or unknown:
        raise ValueError(
            f"{where}: нет ключей {sorted(missing) or '—'}, лишние ключи {sorted(unknown) or '—'}"
        )


def _load_paths(cfg: dict, config_file: pathlib.Path) -> Paths:
    section = cfg.get('paths') or {}
    _check_keys(section, {'data', 'layers', 'logs'}, f"{config_file}, раздел paths")

    layers = section.get('layers') or {}
    layer_names = {f.name for f in fields(Paths)} - {'root', 'data', 'logs'}
    _check_keys(layers, layer_names, f"{config_file}, раздел paths.layers")

    data = _resolve(os.getenv(DATA_DIR_ENV) or section['data'], ROOT)
    return Paths(
        root=ROOT,
        data=data,
        logs=_resolve(section['logs'], ROOT),
        **{name: _resolve(value, data) for name, value in layers.items()},
    )


def load(config_file: pathlib.Path = CONFIG_FILE, env_file: pathlib.Path = ENV_FILE) -> Config:
    """
    Прочитать настройки. Без аргументов — рабочие файлы проекта;
    в тестах передаётся свой конфиг с временными каталогами.
    """
    load_dotenv(env_file)  # уже заданные переменные окружения не перезаписываются
    config_file = pathlib.Path(config_file)
    cfg = yaml.safe_load(config_file.read_text(encoding='utf-8')) or {}

    return Config(
        paths=_load_paths(cfg, config_file),
        clickhouse=ClickHouseConnection(
            host=os.getenv('CH_HOST'),
            port=os.getenv('CH_PORT'),
            user=os.getenv('CH_USER'),
            password=os.getenv('CH_PSWD'),
        ),
    )


config = load()
paths = config.paths
