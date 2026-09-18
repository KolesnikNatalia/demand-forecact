#--------------------------------------------------------------------------------------
# description
#
# name: "InfoVizion" (ИнфоВижен) функциональный блок "Коммерческая аналитика"
#
# author: ООО "Инклик"
#
#--------------------------------------------------------------------------------------
__version__ = "$Revision: 0.1 $"
# $Source$

import sys, os
import logging
import pathlib
import datetime
sys.path.append(os.fspath(pathlib.Path(__file__).resolve().parent)) # через относительный путь подключаем settings
from settings import paths


# Logger
# ---------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# print(param.log_file)

paths.logs.mkdir(parents=True, exist_ok=True) # FileHandler каталог не создаёт

handler_file = logging.FileHandler(
    filename=paths.logs / f'{datetime.date.today():%Y_%m_%d}.log',
    mode='a+',
    encoding='utf-8')
handler_file.setLevel(logging.DEBUG)
handler_file.setFormatter(logging.Formatter(fmt='%(asctime)s [%(filename)s %(lineno)4d] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(handler_file)

handler_stdout = logging.StreamHandler(sys.stdout)
handler_stdout.setLevel(logging.WARNING)
handler_stdout.setFormatter(logging.Formatter(fmt='%(asctime)s %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(handler_stdout)
