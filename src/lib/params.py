"""
Профиль параметров прогона: период, пороги, границы классов.

Пути и подключение живут в `settings.py`, параметры эксперимента — здесь:

    import params

    profile = params.load(paths.root / 'profiles' / 'series_analysis.yaml')
    period = profile.section('period', {'start', 'end'})
    profile.value('returns')

Правила:

- **параметры не зашиваются в код.** Прогон на другом периоде или с другим порогом
  меняет только YAML;
- **раздел проверяется на состав ключей**: опечатка роняет прогон с перечнем
  недостающих и лишних ключей, а не даёт значение по умолчанию;
- **верхний уровень не проверяется целиком**: следующие этапы анализа дописывают
  в тот же файл свои разделы, и шаг этапа 1 о них знать не обязан;
- модуль называется `params`, а не `profile`: `profile` — модуль стандартной
  библиотеки, и при `sys.path.append` импортировался бы он, а не наш файл.
"""

import pathlib
from dataclasses import dataclass

import yaml


@dataclass(frozen=True)
class Profile:
    """Прочитанный профиль. `file` нужен, чтобы ошибка называла файл с опечаткой."""

    file: pathlib.Path
    data: dict

    def value(self, name: str):
        """
        Значение по имени раздела. Вложенный ключ — через точку
        (`scope.outlier_stores`). Нет ключа — ошибка.
        """
        node = self.data
        passed = []
        for key in name.split('.'):
            where = '.'.join(passed) or 'верхний уровень'
            if not isinstance(node, dict) or key not in node:
                raise ValueError(f"{self.file}: нет ключа '{key}' ({where})")
            node = node[key]
            passed.append(key)
        return node

    def section(self, name: str, expected: set) -> dict:
        """Раздел-словарь с проверкой состава ключей."""
        section = self.value(name)
        if not isinstance(section, dict):
            raise ValueError(f"{self.file}, раздел '{name}': ожидается набор ключей, а не {section!r}")

        missing = expected - section.keys()
        unknown = section.keys() - expected
        if missing or unknown:
            raise ValueError(
                f"{self.file}, раздел '{name}': нет ключей {sorted(missing) or '—'}, "
                f"лишние ключи {sorted(unknown) or '—'}"
            )
        return section


def load(profile_file) -> Profile:
    """Прочитать профиль. Даты YAML отдаёт как `datetime.date`."""
    profile_file = pathlib.Path(profile_file)
    if not profile_file.is_file():
        raise FileNotFoundError(f"нет профиля параметров: {profile_file}")

    data = yaml.safe_load(profile_file.read_text(encoding='utf-8'))
    if not isinstance(data, dict):
        raise ValueError(f"{profile_file}: профиль пуст или это не набор разделов")
    return Profile(file=profile_file, data=data)
