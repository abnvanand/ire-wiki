DEFAULT_DUMP_PATH = "dumps/sample26.xml"
DEFAULT_INDEX_DIR = "indexes"

PRIMARY_IDX_FILE = "primary.txt"
SECONDARY_IDX_FILE = "secondary.txt"            # Used with BLOCK based index
SECONDARY_IDX_FILE_OFFSETVERSION = "secondary-offset.txt"  # Used with OFFSET based index
TERIARY_INDEX_FILE = "tertiary.txt"

DOC_TITLES_FILE = "docid-title-map.txt"
DOC_TITLEOFFSET_FILE = "docid-title-offset.txt"
DOC_NTERMS_FILE = "docid-termcount-map.txt"

STOPWORDS_FILE_PATH = "stopwords.txt"

TERM_POSTINGLIST_SEP = ":"
DOCIDS_SEP = ";"
DOCID_TF_ZONES_SEP = "|"
ZONES_SEP = ","
ZONE_ZFREQ_SEP = "."
TERM_OFFSET_SEP = "="
DOCID_OFFSET_SEP = "="
DOCID_NTERMS_SEP = "="
DOCID_TITLE_SEP = "="

QUERY_FILE = "dumps/queryfile"
OUTPUT_FILE = "dumps/output.txt"

FIELD_QUERY_OPERATOR = "OR"

FREQUENCY = "freq"
ZONES = "zones"

TMP_BLK_PREFIX = "temp"
PRIMARY_BLK_PREFIX = "primary"

INDEX_TYPE_BLOCK = "BLOCK"
INDEX_TYPE_OFFSET = "OFFSET"
INDEX_TO_USE = "OFFSET"  # BLOCK vs OFFSET
