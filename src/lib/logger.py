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
sys.path.append(os.fspath(pathlib.Path(__file__).resolve().parents[1])) # через относительный путь подключаем settings


# Logger
# ---------------------------------------
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# print(param.log_file)

log_dir = pathlib.Path(__file__).parents[2] / 'logs'
log_dir.mkdir(parents=True, exist_ok=True)

handler_file = logging.FileHandler(
    filename=log_dir / f'{datetime.date.today():%Y_%m_%d}.log',
    mode='a+',
    encoding='utf-8')
handler_file.setLevel(logging.DEBUG)
handler_file.setFormatter(logging.Formatter(fmt='%(asctime)s [%(filename)s %(lineno)4d] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(handler_file)

handler_stdout = logging.StreamHandler(sys.stdout)
handler_stdout.setLevel(logging.WARNING)
handler_stdout.setFormatter(logging.Formatter(fmt='%(asctime)s %(message)s', datefmt='%H:%M:%S'))
logger.addHandler(handler_stdout)
