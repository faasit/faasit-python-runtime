import logging
import os

log_level = int(os.getenv('FAASIT_LOG', "2"))*10
log = logging.getLogger('faasit')
log.setLevel(log_level)

handler = logging.StreamHandler()

formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s")

handler.setFormatter(formatter)

log.addHandler(handler)