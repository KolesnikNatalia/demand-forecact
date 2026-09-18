import os, sys
import time
import datetime
import subprocess
import enum
import pathlib
from abc import ABC, abstractmethod;
from logger import logger
from dotenv import load_dotenv, find_dotenv


sys.path.append(os.fspath(pathlib.Path(__file__).resolve().parents[1])) # через относительный путь подключаем settings
load_dotenv(find_dotenv()) # Чтение из .env


# CONSTANT
# ---------------------------------------
CLICKHOUSE_SERVICE = 'clickhouse-server'
CLICKHOUSE_DATE_NULL = datetime.date(1970,1,1)



# Connection to ClickHouse client settings
# ----------------------------------------
connection = {
    'host' : os.getenv('CH_HOST')
    , 'port' : os.getenv('CH_PORT')
    , 'user' : os.getenv('CH_USER')
    , 'password' : os.getenv('CH_PSWD')
    , 'database' : None
    , 'multiline' : None
    , 'multiquery' : None
    , 'format' : None
}

# ClickHouse select query & other settings
# ----------------------------------------
settings = {
    'input_format_with_names_use_header' : 0
    , 'input_format_null_as_default' : 0
    , 'mutations_sync' : 1
    , 'format_csv_delimiter': '";"'

    , 'join_default_strictness': 'ALL'
    , 'partial_merge_join_optimizations': 0
    , 'join_use_nulls': 0
    , 'allow_experimental_analyzer': 1
    , 'max_expanded_ast_elements': 5000000
    , 'max_ast_elements': 1000000
    }


# ClickHouse select query & other settings
# ----------------------------------------
default_file_format = 'CSVWithNames' # CSVWithNames # Native
# logger = None


#
# ----------------------------------------

def dictionary_to_list(dictionary, format_string:str = "{0} {1}") -> list:
    if dictionary: return [format_string.format(key, value) for key, value in dictionary.items() if value is not None and value != '']

def connection_params(dictionary:dict = None) -> str:
    if dictionary: return ' '.join(dictionary_to_list(dictionary, "--{0} {1}"))
    return ''

def settings_params(dictionary:dict = None) -> str:
    if dictionary: return ' '.join(dictionary_to_list(dictionary, "--{0}={1}"))
    return ''

def log(message:str):
    logger.error(message) if logger else print(message)


# ClickHouse calls
# ----------------------------------------

def flush_service(service_name:str = CLICKHOUSE_SERVICE, wait_sec:int = 5):
    # ВНИМАНИЕ: требует пакетной установки ClickHouse (есть служба systemd/service).
    # В локальной среде сервер запущен из single-binary (/clickhouse/clickhouse,
    # симлинки в ~/.local/bin): службы clickhouse-server нет, и вызов молча ничего
    # не сделает. Здесь сервер перезапускается вручную.
    os.system(f"service {service_name} restart") # перегружаем службу
    time.sleep(wait_sec) # отправляем в сон

def exec_local(command:str, command_file:str, save_file:str = '', save_format:str = default_file_format):
    # Если задан запрос в виде текста command и указан файл save_file
    # для сохранения результата, то добавляем команду выгрузки к запросу
    if save_file:
        if os.path.exists(save_file):
            os.remove(save_file) # удаляем выходной файл для перезаписи
            time.sleep(0.01)

    if command:
        if save_file and "INTO OUTFILE" not in command:
            command += f"\nINTO OUTFILE '{save_file}' FORMAT {save_format};"
        # if len(command_file)==0:
        #     command_file = os.path.dirname(save_file)+'/temp_clhouse/'+ os.path.basename(save_file).split('.')[0]+'.tmp'
        with open(command_file, 'w', encoding='utf-8') as file:
            file.write(command) # сохраняем запрос в временный файл

    start_command_local = "clickhouse-local {0} {1} --queries-file {2}".format( '', settings_params(settings), command_file)
    # print(start_command_local)
    result = subprocess.getstatusoutput(start_command_local)
    if( result[0] != 0):
        log("### Exception: {0} {1}".format(result[1], open(command_file, 'r').read()))
        raise Exception("### Exception: {0} {1}".format(result[1], open(command_file, 'r').read()))


def exec_client(command:str, command_file:str, save_file:str = '', save_format:str = default_file_format, try_max:int = 0, wait_sec:int = 5, return_result=False):
    # Если задан запрос в виде текста command и указан файл save_file
    # для сохранения результата, то добавляем команду выгрузки к запросу
    if save_file:
        if os.path.exists(save_file): os.remove(save_file) # удаляем выходной файл для перезаписи
        time.sleep(0.01)

    if command:
        if save_file and "INTO OUTFILE" not in command:
            command += f"\nINTO OUTFILE '{save_file}' FORMAT {save_format};"
        # if len(command_file)==0:
        #     command_file = os.path.dirname(save_file)+'/temp_clhouse'+ os.path.basename(save_file).split('.')[0]+'.tmp'
        with open(command_file, 'w', encoding='utf-8') as file:
            file.write(command) # сохраняем запрос в временный файл
    start_command_client = "clickhouse-client {0} {1} --queries-file {2}".format(connection_params(connection), settings_params(settings), command_file)
    try_count = 0
    result = [1, '']
    while (result[0] != 0 and try_count <= try_max):
        try_count += 1
        result = subprocess.getstatusoutput(start_command_client)
        if( result[0] != 0 ):
            log("### Exception: TRY[{2}]  {0} {1}".format(result[1], open(command_file, 'r').read(), try_count))
            # flush_service(CLICKHOUSE_SERVICE, wait_sec)
    if( result[0] != 0):
        log("### Exception: {0} {1}".format(result[1], open(command_file, 'r').read()))
        raise Exception("### Exception: {0} {1}".format(result[1], open(command_file, 'r').read()))
    if return_result:
        return result[1]

# ClickHouse file formats
# ----------------------------------------

class FileFormat(object):
    def __init__(self, format_name:str, settings:dict ={}):
        self._format_name = format_name
        self._settings = settings.copy()

    def __str__(self):
        return self._format_name

    def __len__(self):
        return len(self._settings)

    def __getitem__(self, item):
        return self._settings.get(item)

    def __setitem__(self, key, value):
        self._settings[key] = value

    def __delitem__(self, key):
        if key in self._settings:
            del self._settings[key]

    def __contains__(self, key):
        return (True if key in self._settings else False)

    def settings(self) ->str:
        if self._settings: return 'SETTINGS {0}'.format(', '.join(dictionary_to_list(self._settings, "{0}={1}")))
        return ''


class FileFormatNull(FileFormat):
    def __init__(self):
        super().__init__('Null')


class FileFormatNative(FileFormat):
    def __init__(self):
        super().__init__('Native')


class FileFormatCsv(FileFormat):
    def __init__(self, format_csv_delimiter:str = ','):
        def _upper_n(text:str, len:int =0) ->str:
            if len == 0: return text.upper()
            return text[:len].upper() + text[len:]

        super().__init__(_upper_n(self.__class__.__name__[len('FileFormat'):], 3))
        self['format_csv_delimiter'] = f"'{format_csv_delimiter}'"
        self['input_format_with_names_use_header'] = 1

    @property
    def csv_delimiter(self) ->str:
        return self['format_csv_delimiter']

    @csv_delimiter.setter
    def csv_delimiter(self, value:str):
        self['format_csv_delimiter'] = value

    @property
    def names_use_header(self) ->int:
        return self['input_format_with_names_use_header']

    @names_use_header.setter
    def names_use_header(self, value:int):
        self['input_format_with_names_use_header'] = value


class FileFormatCsvWithNames(FileFormatCsv):
    def __init__(self, format_csv_delimiter:str = ','):
        super().__init__(format_csv_delimiter)
        self['input_format_skip_unknown_fields'] = 1

    @property
    def skip_unknown_fields(self) ->int:
        return self['input_format_skip_unknown_fields']

    @skip_unknown_fields.setter
    def skip_unknown_fields(self, value:int):
        self['input_format_skip_unknown_fields'] = value


class FileFormatCsvWithNamesAndTypes(FileFormatCsvWithNames):
    def __init__(self, format_csv_delimiter:str = ','):
        super().__init__(format_csv_delimiter)


class FileFormatParquet(FileFormat):
    def __init__(self):
        super().__init__('Parquet')
        self.extension = '.parquet'


class FileFormatType(enum.Enum):
    """
    File formats
    """
    Null = 0
    Native = 1
    Csv = 2
    CsvWithNames = 3
    CsvWithNamesAndTypes = 4
    Parquet = 5


def file_format(file_format_type:FileFormatType) ->FileFormat:
    """
    Factory Method
    """
    file_format_dictionary = {
        FileFormatType.Null : FileFormatNull,
        FileFormatType.Native : FileFormatNative,
        FileFormatType.Csv : FileFormatCsv,
        FileFormatType.CsvWithNames : FileFormatCsvWithNames,
        FileFormatType.CsvWithNamesAndTypes : FileFormatCsvWithNamesAndTypes,
        FileFormatType.Parquet : FileFormatParquet,
    }
    return file_format_dictionary[file_format_type]()




file_format_default = FileFormatParquet()


# Шаблон запроса для формирования массива узлов иерархии из дерева
class HierarchyTemplateAbc(ABC):
    def __init__(self, source_name:str, fields:dict ={}):
        self._source_name = source_name
        self._fields = fields.copy()

    def __str__(self):
        return self.query()

    def template(self, field_name:str) ->str:
        """
        Если передан список полей, то вернуть название поля по идентификатору
        иначе вернуть идентификатор как маску шаблона ('{id}', '{name}', ...)
        """
        return self._fields[field_name] if field_name in self._fields else '{{{field_name}}}'

    @abstractmethod
    def query_buffer(self) ->str:
        """
        Запрос к источнику данных, возвращающий список полей:
            LevelId - узел дерева
            ParentLevelId - ссылка на родительский узел дерева
            LevelName - наименование узла дерева
        """
        pass

    def query(self, *, table_name_out:str ='tHierarchyTable', hierarchy_level_max:int =5, text_delimiter:str ='/') ->str:
        """
        Формирует шаблон запроса к clickhouse по заранее подготовленному источнику с полями:
            LevelId - узел дерева
            ParentLevelId - ссылка на родительский узел дерева
            LevelName - наименование узла дерева
        """
        level_id_list = [f"tHierarchyObj{hierarchy_level:02}.LevelId" for hierarchy_level in range(1, hierarchy_level_max + 1)]
        level_name_list = [f"tHierarchyObj{hierarchy_level:02}.LevelName" for hierarchy_level in range(1, hierarchy_level_max + 1)]

        level_join_list = []
        for hierarchy_level in range(2, hierarchy_level_max + 1):
            level_join_list.append( f"LEFT JOIN {table_name_out}Tmp AS tHierarchyObj{hierarchy_level:02} ON (tHierarchyObj{hierarchy_level:02}.LevelId = tHierarchyObj{hierarchy_level-1:02}.ParentLevelId)")

        # сортируем списки полей в обратном порядке, т.к. иерархия будет строиться от 'потомка' к 'родителю'
        # и ее нужно развернуть в обратную сторону (можно это также сделать через arrayReverse() в ClickHouse )
        level_id_list.sort(reverse=True)
        level_name_list.sort(reverse=True)

        return """
            {table_name}Tmp AS (
                {query_buffer}
            )
            , {table_name} AS (
                SELECT
                    tHierarchyObj01.LevelId AS LevelId
                    , tHierarchyObj01.ParentLevelId AS ParentLevelId
                    , tHierarchyObj01.LevelName AS LevelName
                    , arrayFilter(x -> length(toString(x)) > 0, [{fields_id_list}]) AS LevelIdList
                    , arrayFilter(x -> length(toString(x)) > 0, [{fields_name_list}]) AS LevelNameList
                    , arrayStringConcat(LevelNameList, '{fields_delimiter}') AS LevelNameTree
                FROM {table_name}Tmp AS tHierarchyObj01
                {join_clause}
            )
        """.format( query_buffer = self.query_buffer()
                ,   table_name = table_name_out
                ,   fields_id_list =', '.join(level_id_list)
                ,   fields_name_list =', '.join(level_name_list)
                ,   fields_delimiter = text_delimiter
                ,   join_clause='\n'.join(level_join_list) )


class HierarchyTemplate(HierarchyTemplateAbc):
    def __init__(self, table_name:str, fields:dict ={}):
        super().__init__(table_name, fields)

    @property
    def table_name(self) ->str:
        return self._source_name

    @table_name.setter
    def table_name(self, value:str):
        self._source_name = value

    def query_buffer(self) ->str:
        return f"""SELECT
                `{self.template('id')}` AS LevelId
                , `{self.template('parent_id')}` AS ParentLevelId
                , `{self.template('name')}` AS LevelName
            FROM {self.table_name}"""
