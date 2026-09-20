"""
Порядок запуска шагов: одна точка входа на весь расчёт.

Здесь видно, что за чем считается и на каком профиле параметров. Каждый шаг —
самостоятельный скрипт со своим `run(...)`, поэтому запускается он отдельным
процессом: так порядок читается списком `STEPS`, а не цепочкой импортов, и шаг
можно в любой момент запустить руками той же командой, что стоит в списке.

Запуск:

    uv run python main.py                      # весь расчёт
    uv run python main.py --checks             # с проверками после каждого шага
    uv run python main.py --only series_profile  # пересчитать один шаг

Шаги останавливают прогон сами: проверки уровня error бросают исключение,
процесс завершается ненулевым кодом, и следующий шаг не начинается.

Позже этот же список станет Prefect flow: задачи будут звать те же `run(...)`,
а проверки пойдут отдельными задачами ([docs/architecture.md](docs/architecture.md)).

## Вывод и логи

Шаги пишут в общий `logs/ГГГГ_ММ_ДД.log` и в консоль от WARNING — в stdout.
Поэтому stdout не перехватывается: пока шаг считает, его сообщения видно живьём,
а перехват сложил бы те же строки в тот же файл второй раз. Перехватывается
только stderr: там traceback упавшего шага, и в лог он сам не попадает.
"""

import argparse
import pathlib
import subprocess
import sys
import time

_SRC = pathlib.Path(__file__).resolve().parent / 'src'
sys.path.append(f"{_SRC}/lib")  # модули src/lib
import settings
from logger import logger


# Порядок расчёта. Шаг добавляется сюда, когда у него появляется `run(...)`;
# номер этапа — из prd/plan-store-item-time-series-analysis.md.
STEPS = (
    {'name': 'series_days',
     'script': 'src/preprocessing/series_days.py',
     'about': 'этап 1: дневные ряды «магазин × товар» с разметкой'},
    {'name': 'series_profile',
     'script': 'src/analysis/series_profile.py',
     'about': 'этап 2: профиль рядов — история, класс спроса, дефицит, ABC'},
)

PROFILE_FILE = settings.paths.root / 'profiles' / 'series_analysis.yaml'


def run_step(step: dict, profile_file, checks: bool, paths) -> float:
    """
    Запустить шаг отдельным процессом. Ненулевой код возврата останавливает расчёт.

    Интерпретатор берётся `sys.executable`, а не строкой `python3`: шагам нужно
    окружение проекта, и системный питон упал бы на первом же импорте. Значит,
    и сам `main.py` запускают через `uv run`.
    """
    command = [sys.executable, str(paths.root / step['script']),
               '--profile', str(profile_file)]
    if checks:
        command.append('--checks')

    logger.info(f"шаг {step['name']} — {step['about']}")
    logger.info(f"выполняется команда: {' '.join(command)}")
    started = time.time()

    try:
        # stdout не перехватывается: сообщения шага видно по ходу дела (см. шапку)
        subprocess.run(command, stderr=subprocess.PIPE, text=True, check=True)
    except subprocess.CalledProcessError as err:
        logger.error(f"шаг {step['name']} упал, код возврата {err.returncode}")
        if err.stderr:
            logger.error(f"STDERR шага {step['name']}:\n{err.stderr.rstrip()}")
        raise

    elapsed = time.time() - started
    logger.info(f"шаг {step['name']} — готово за {elapsed:.1f} c")
    return elapsed


def run(profile_file=PROFILE_FILE, checks: bool = False, only: str | None = None,
        paths=None) -> dict:
    """
    Посчитать расчёт по порядку из `STEPS`.

    `only` — имя одного шага: пересчитать его, не трогая остальные. Возвращает
    время шагов, чтобы в конце было видно, где расчёт стоит дольше всего.
    """
    paths = paths or settings.paths
    paths.ensure()  # нет сырья — говорим об этом до запуска первого процесса

    profile_file = pathlib.Path(profile_file)
    if not profile_file.is_file():
        raise FileNotFoundError(f"нет профиля параметров: {profile_file}")

    steps = STEPS
    if only:
        steps = tuple(step for step in STEPS if step['name'] == only)
        if not steps:
            raise ValueError(f"нет шага '{only}'. Есть: "
                             f"{', '.join(step['name'] for step in STEPS)}")

    logger.info(f"расчёт: {len(steps)} шаг(ов), профиль {profile_file}, "
                f"проверки {'включены' if checks else 'выключены'}")
    started = time.time()

    timing = {step['name']: run_step(step, profile_file, checks, paths) for step in steps}

    summary = [f"расчёт закончен за {time.time() - started:.1f} c"]
    summary += [f"  {name}: {elapsed:.1f} c" for name, elapsed in timing.items()]
    logger.warning('\n'.join(summary))  # WARNING, чтобы итог был виден в консоли
    return timing


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Порядок запуска шагов расчёта',
        epilog='Порядок: ' + ' → '.join(step['name'] for step in STEPS))
    parser.add_argument('--profile', default=PROFILE_FILE, help='профиль параметров')
    parser.add_argument('--checks', action='store_true',
                        help='прогнать проверки после каждого шага')
    parser.add_argument('--only', help='пересчитать один шаг по имени')
    args = parser.parse_args()

    try:
        run(args.profile, checks=args.checks, only=args.only)
    except subprocess.CalledProcessError as err:
        # почему шаг упал, уже сказано выше — и строкой лога, и его stderr.
        # Свой traceback здесь добавил бы только «процесс вернул 1»
        sys.exit(err.returncode)
