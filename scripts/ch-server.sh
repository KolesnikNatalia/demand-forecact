#!/usr/bin/env bash
#--------------------------------------------------------------------------------------
# Управление локальным сервером ClickHouse.
#
# Установка single-binary: один бинарь /clickhouse/clickhouse, который выбирает режим
# по подкоманде (server / client / local) или по имени, под которым его вызвали
# (симлинки clickhouse-client, clickhouse-local в ~/.local/bin).
#
# Пути в /clickhouse/config.xml сделаны абсолютными (<path> и access), поэтому
# каталог данных не зависит от того, откуда запущен сервер. cd в $CH_HOME ниже
# оставлен подстраховкой на случай, если конфиг снова станет относительным.
#
# Команды: start | stop | restart | status | log
#--------------------------------------------------------------------------------------
set -euo pipefail

CH_HOME="${CH_HOME:-/clickhouse}"
CH_BIN="$CH_HOME/clickhouse"
CH_CONFIG="$CH_HOME/config.xml"
CH_LOG="${CH_LOG:-$CH_HOME/server.log}"
CH_TCP_PORT="${CH_TCP_PORT:-9000}"
WAIT_SEC="${WAIT_SEC:-60}"


# Возвращает pid живого сервера или пустую строку.
# Проверяем не только «pid жив», но и что это действительно наш бинарь:
# после перезагрузки pid из status-файла может достаться чужому процессу.
server_pid() {
    local pid
    [[ -r "$CH_HOME/status" ]] || return 0
    pid="$(awk '/^PID:/ {print $2; exit}' "$CH_HOME/status" 2>/dev/null || true)"
    [[ -n "$pid" ]] || return 0
    kill -0 "$pid" 2>/dev/null || return 0
    [[ "$(readlink -f "/proc/$pid/exe" 2>/dev/null || true)" == "$(readlink -f "$CH_BIN")" ]] || return 0
    echo "$pid"
}

port_open() {
    (exec 3<>"/dev/tcp/127.0.0.1/$CH_TCP_PORT") 2>/dev/null
}

require_bin() {
    [[ -x "$CH_BIN" ]] || { echo "Нет бинарника $CH_BIN" >&2; exit 1; }
    [[ -r "$CH_CONFIG" ]] || { echo "Нет конфига $CH_CONFIG" >&2; exit 1; }
}


cmd_start() {
    require_bin

    if port_open; then
        echo "Сервер уже слушает 127.0.0.1:$CH_TCP_PORT"
        return 0
    fi

    # Каталог данных занят живым процессом, но порт закрыт — это не сервер,
    # а интерактивный clickhouse/clickhouse-local, который держит лок на
    # $CH_HOME/status. Пока он жив, сервер не стартует.
    local busy
    busy="$(server_pid)"
    if [[ -n "$busy" ]]; then
        echo "Каталог $CH_HOME занят процессом $busy, но порт $CH_TCP_PORT закрыт." >&2
        echo "Похоже, это интерактивный clickhouse, а не сервер. Завершите его и повторите:" >&2
        ps -o pid,tty,stat,etime,cmd -p "$busy" >&2 || true
        return 1
    fi

    echo "Запускаю сервер (лог: $CH_LOG)"
    # при абсолютных путях в конфиге cd не обязателен, но лишним не будет
    ( cd "$CH_HOME" && nohup "$CH_BIN" server --config-file="$CH_CONFIG" >>"$CH_LOG" 2>&1 & )

    local i
    for ((i = 1; i <= WAIT_SEC; i++)); do
        if port_open; then
            echo "Готов за ${i}с: $(client_query 'select version()') на 127.0.0.1:$CH_TCP_PORT"
            return 0
        fi
        # сервер мог упасть на старте — не ждём весь таймаут молча
        if ! server_pid >/dev/null && ((i > 3)) && [[ -s "$CH_LOG" ]] \
           && tail -50 "$CH_LOG" | grep -qE '<Error>|Address already in use|Cannot lock'; then
            echo "Сервер не поднялся, ошибки из лога:" >&2
            tail -50 "$CH_LOG" | grep -E '<Error>|Address already in use|Cannot lock' | tail -5 >&2
            return 1
        fi
        sleep 1
    done

    echo "Порт $CH_TCP_PORT не открылся за ${WAIT_SEC}с. Хвост лога:" >&2
    tail -20 "$CH_LOG" >&2
    return 1
}

cmd_stop() {
    local pid
    pid="$(server_pid)"
    if [[ -z "$pid" ]]; then
        echo "Сервер не запущен"
        return 0
    fi

    echo "Останавливаю сервер (pid $pid)"
    kill -TERM "$pid"

    local i
    for ((i = 1; i <= WAIT_SEC; i++)); do
        if ! kill -0 "$pid" 2>/dev/null; then
            echo "Остановлен за ${i}с"
            return 0
        fi
        sleep 1
    done

    # Намеренно не добиваем -9: у СУБД это чревато незакрытыми частями данных.
    echo "Процесс $pid не завершился за ${WAIT_SEC}с. Проверьте лог; если нужно — kill -9 $pid" >&2
    return 1
}

cmd_status() {
    local pid
    pid="$(server_pid)"
    if [[ -n "$pid" ]] && port_open; then
        echo "Запущен: pid $pid, версия $(client_query 'select version()'), порт $CH_TCP_PORT"
        echo "Аптайм:  $(client_query "select formatReadableTimeDelta(uptime())")"
    elif [[ -n "$pid" ]]; then
        echo "Процесс $pid жив, но порт $CH_TCP_PORT закрыт (стартует или это интерактивный clickhouse)"
        return 1
    else
        echo "Не запущен"
        return 1
    fi
}

client_query() {
    "$CH_HOME/clickhouse" client --host 127.0.0.1 --port "$CH_TCP_PORT" --query "$1" 2>/dev/null || echo '?'
}

cmd_log() {
    tail -f "$CH_LOG"
}


case "${1:-status}" in
    start)   cmd_start ;;
    stop)    cmd_stop ;;
    restart) cmd_stop; cmd_start ;;
    status)  cmd_status ;;
    log)     cmd_log ;;
    *)
        echo "Использование: ${0##*/} {start|stop|restart|status|log}" >&2
        exit 2
        ;;
esac
