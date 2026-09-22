"""
Журнал проверок: общая часть для всех слоёв.

Каждая проверка формулируется одинаково — «значение не больше порога», поэтому
статус считается общим правилом, а строки складываются в один журнал:

| колонка | что |
|---|---|
| `CheckName` | имя проверки |
| `Level` | `error` — останавливает прогон, `warning` — только запись в отчёт и лог |
| `Value`, `Threshold` | измеренное значение и порог |
| `Status` | `ok`, `error` или `warning` |
| `Details` | числа, по которым видно, что именно разошлось |
| `RunAt` | время прогона, местное, строкой (parquet хранит `DateTime` в UTC) |

Шаг приносит сюда две части своего SQL: `preamble` — определения CTE
(посчитанные суммы и счётчики), `body` — `union all` из строк проверок.
Собирает запрос, пишет журнал и отчёт, а на `error` бросает исключение
этот модуль: одинаково для всех слоёв.

Журнал — один файл на слой в `data/checks/` (`<имя>_checks.parquet`, рядом отчёт
`.md`), и каждый прогон его перезаписывает. Интересен последний результат: слой
либо прошёл проверки сейчас, либо нет. Метки времени в имени файла нет намеренно
(решение 2026-09-20) — иначе при двух-трёх прогонах в день каталог за год
зарастал бы тысячами файлов, среди которых не найти текущий. Время прогона
никуда не делось: оно в колонке `RunAt` и в заголовке отчёта.
"""

import datetime
import json
import pathlib
import sys

sys.path.append(f"{pathlib.Path(__file__).resolve().parents[1]}/lib")  # модули src/lib
import clickhouse
import sql
from logger import logger


def _journal_sql(preamble: str, body: str, run_at: str) -> str:
    """Собрать запрос журнала: статус и время прогона — общим правилом."""
    return f"""with {preamble}
        select
            CheckName
            , Level
            , Value
            , Threshold
            , if(Value <= Threshold, 'ok', Level) as Status
            , Details
            -- время прогона строкой: parquet хранит DateTime в UTC, и при чтении
            -- журнала время разъезжалось бы с логом. В имени файла времени нет,
            -- поэтому здесь единственное место, где видно, когда слой проверяли
            , {run_at} as RunAt
        from (
            {body}
        )
        -- сначала то, из-за чего прогон встал, потом предупреждения, потом ok
        order by multiIf(Status = 'error', 0, Status = 'warning', 1, 2), CheckName
        {sql.PARQUET_SETTINGS}
    """


def run(name: str, title: str, preamble: str, body: str, paths, error_text: str) -> list:
    """
    Посчитать проверки, записать журнал и отчёт, остановить прогон на `error`.

    `name` — имя файлов журнала (`<name>_checks.parquet` и `.md` рядом),
    `title` — заголовок отчёта, `error_text` — начало сообщения исключения:
    по нему в логе видно, какой слой не прошёл проверки.

    Возвращает строки журнала.
    """
    # имя без метки времени: прогон перезаписывает журнал прошлого. Нужен
    # последний результат, а не история (см. шапку модуля). Время прогона
    # остаётся в колонке RunAt и в заголовке отчёта
    started = datetime.datetime.now()
    journal_file = paths.checks / f'{name}_checks.parquet'

    # отчёт прошлого прогона убирается до запуска, как `exec_local` убирает журнал.
    # Иначе упавший запрос проверок оставил бы рядом старый `.md` со словами
    # «Замечаний нет» — и он читался бы как результат этого прогона. Упавший
    # прогон должен оставлять отсутствие файлов, а не чужие файлы
    journal_file.with_suffix('.md').unlink(missing_ok=True)

    clickhouse.exec_local(
        _journal_sql(preamble, body, sql.literal(started.strftime('%Y-%m-%d %H:%M:%S'))),
        paths.tmp / f'{name}_checks.sql', journal_file, 'Parquet')

    rows = _read(journal_file, paths, name)
    # пустой журнал — это не «всё сошлось», а поломка самого запроса проверок:
    # ни одна величина не сверена. Молчать об этом нельзя, иначе слой объявят
    # проверенным, не проверив
    if not rows:
        raise ValueError(f"журнал проверок пуст: {journal_file}. Ни одна проверка не посчитана — "
                         f"смотрите запрос {paths.tmp / f'{name}_checks.sql'}")

    report_file = _write_report(rows, journal_file, title, paths, name)
    _report(rows, report_file, error_text)
    return rows


def _read(journal_file, paths, name: str) -> list:
    """Прочитать журнал обратно: строк десяток, файл ради этого не нужен."""
    # порядок файла сохраняется: в нём сначала то, из-за чего прогон встал
    text = clickhouse.exec_local(
        f"select * from file('{journal_file}') format JSONEachRow",
        paths.tmp / f'{name}_checks_read.sql', return_result=True)
    return [json.loads(line) for line in (text or '').splitlines() if line.strip()]


def _write_report(rows: list, journal_file, title: str, paths, name: str) -> pathlib.Path:
    """
    Отчёт прогона рядом с журналом: замечания вверху, полная таблица ниже.

    Отдельный файл нужен потому, что в логе замечание про одну позицию теряется
    среди сообщений шага, а разбирать его будет человек — возможно, назавтра.
    Таблицу готовит сам ClickHouse (`FORMAT Markdown`) из журнала прогона.
    """
    report_file = journal_file.with_suffix('.md')
    table = clickhouse.exec_local(
        f"""select CheckName as `Проверка`, Status as `Статус`, Value as `Значение`
                 , Threshold as `Порог`, Details as `Подробности`
            from file('{journal_file}') format Markdown""",
        paths.tmp / f'{name}_checks_report.sql', return_result=True)

    issues = [row for row in rows if row['Status'] != 'ok']
    lines = [f"# {title} — {rows[0]['RunAt']}", '',
             f"Проверок {len(rows)}: ошибок {sum(r['Status'] == 'error' for r in rows)}, "
             f"замечаний {sum(r['Status'] == 'warning' for r in rows)}.", '']

    if issues:
        lines.append('## Замечания и ошибки')
        lines.append('')
        lines += [f"- **{row['CheckName']}** ({row['Status']}): {row['Details']}" for row in issues]
        lines.append('')
    else:
        lines += ['Замечаний нет.', '']

    lines += ['## Все проверки', '', table or '', '',
              f"Журнал прогона: `{journal_file}`", '']
    report_file.write_text('\n'.join(lines), encoding='utf-8')
    return report_file


def _report(rows: list, report_file, error_text: str):
    """
    Подвести итог прогона и остановить его, если есть error.

    Замечания собираются в один блок в конце, а не только идут построчно по ходу
    дела: отдельная позиция с незнакомой единицей расчёт не останавливает — иначе
    одна SKU оставила бы без прогноза всю сеть, — но и потеряться среди сообщений
    шага она не должна.
    """
    errors = [r for r in rows if r['Status'] == 'error']
    warnings = [r for r in rows if r['Status'] == 'warning']

    for row in rows:
        message = f"проверка {row['CheckName']}: {row['Status']} — {row['Details']}"
        (logger.error if row['Status'] == 'error' else
         logger.warning if row['Status'] == 'warning' else logger.info)(message)

    summary = [f"итог проверок: всего {len(rows)}, ошибок {len(errors)}, замечаний {len(warnings)}"]
    summary += [f"  [{row['Status']}] {row['CheckName']}: {row['Details']}"
                for row in errors + warnings]
    summary.append(f"  отчёт: {report_file}")
    # уровень warning, чтобы итог был виден в консоли, а не только в файле лога
    (logger.warning if errors or warnings else logger.info)('\n'.join(summary))

    if errors:
        details = '; '.join(f"{r['CheckName']}: {r['Details']}" for r in errors)
        raise ValueError(f"{error_text} ({len(errors)}): {details}. Отчёт: {report_file}")
