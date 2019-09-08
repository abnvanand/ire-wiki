import logging as log
import sys
import time
from os import mkdir

from src import constants
from src.helpers import Helpers
from src.parser import XMLParser

log.basicConfig(format='%(levelname)s: %(filename)s-%(funcName)s()-%(message)s',
                level=log.DEBUG)  # STOPSHIP

log.debug("sys.argv %s", sys.argv)

DUMP_PATH = sys.argv[1] if len(sys.argv) > 1 else constants.DEFAULT_DUMP_PATH
INDEX_DIR = sys.argv[2] if len(sys.argv) > 2 else constants.DEFAULT_INDEX_DIR

try:
    mkdir(INDEX_DIR)
except FileExistsError:
    pass

start_time = time.process_time()

Helpers.load_stopwords(constants.STOPWORDS_FILE_PATH)
log.debug("AppGlobals.stopwords type: %s size: %s", type(Helpers.stopwords), len(Helpers.stopwords))

# Parse xml dump
xmlparser = XMLParser()
xmlparser.parse(DUMP_PATH, INDEX_DIR)
# Parser internally calls indexer for each page of the document


log.info("Indexed in %s secs.", time.process_time() - start_time)
