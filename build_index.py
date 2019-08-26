import logging
import time
from collections import defaultdict

from src import constants
from src.helpers import Helpers
from src.indexer import Indexer
from src.parser import XMLParser

logging.basicConfig(format='%(levelname)s: %(filename)s-%(funcName)s()-%(message)s',
                    level=logging.INFO)  # STOPSHIP

# TODO: Get as commandline args
DUMP_PATH = input(f"Enter dump path. (default: {constants.DEFAULT_DUMP_PATH})")
if not DUMP_PATH:
    DUMP_PATH = constants.DEFAULT_DUMP_PATH
INDEX_DIR = input(f"Enter index dir. (default: {constants.DEFAULT_INDEX_DIR})")
if not INDEX_DIR:
    INDEX_DIR = constants.DEFAULT_INDEX_DIR

# TODO: Steps[Build][Integrate]
# 1. Parsing[][]
# 2. Tokenizing[][]
# 3. Case Folding[][]
# 4. Stop Words Removal[][]
# 5. Stemming[][]
# 6. Inverted Index Creation[][]
# 7. Field queries
x_start = time.clock()

Helpers.load_stopwords(constants.STOPWORDS_FILE_PATH)
logging.debug("AppGlobals.stopwords", Helpers.stopwords)

xmlparser = XMLParser()
xmlparser.parse(DUMP_PATH)

# TODO: verify whether python GC bug still exists: https://stackoverflow.com/a/2480015/5463404
# print("termid_docid_list", Indexer.termid_docid_list)
Indexer.termid_docid_list.sort()
# print("termid_docid_list sorted", Indexer.termid_docid_list)
postings_list = defaultdict(list)
for termid, docid in Indexer.termid_docid_list:
    postings_list[termid].append(docid)

with open(f"{INDEX_DIR}/{constants.POSTINGS_FILE_NAME}", "w") as fp:  # format=> termid:docid1,docid2,docid3....\n
    for termid in postings_list:
        print(f"{termid}{constants.TERM_POSTINGS_SEP}{constants.DOCIDS_SEP.join(postings_list[termid])}", file=fp)

Helpers.compress(str(Helpers.term_termid_map), f"{constants.TERM_ID_MAPPING_FILE_NAME}.bz2")
# with open(f"{INDEX_DIR}/{constants.TERM_ID_MAPPING_FILE_NAME}", "w") as fp:
#   for term in Helpers.term_termid_map:
#       print(f"{term}:{Helpers.term_termid_map[term]}", file=fp)

with open(f"{INDEX_DIR}/{constants.DOC_ID_TITLE_MAPPING_FILE_NAME}", "w") as fp:
    fp.write(str(Helpers.docid_docname_map))

x_end = time.clock()
print("Indexed in ", x_end - x_start)
