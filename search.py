import logging as log
import re

from src import constants
from src.constants import STOPWORDS_FILE_PATH
from src.helpers import Helpers
from src.stemmer import PorterStemmer

log.basicConfig(format='%(levelname)s: %(filename)s-%(funcName)s()-%(message)s',
                level=log.INFO)  # STOPSHIP

ONE_WORD_QUERY = "ONE_WORD_QUERY"
FREE_TEXT_QUERY = "FREE_TEXT_QUERY"
FIELD_QUERY = "FIELD_QUERY"

field_type_map = {
    "title": "T",
    "body": "B",
    "infobox": "I",
    "category": "C",
    "ref": "R",
    "link": "L",
}


class Search:
    def __init__(self):
        self.index = {}
        self.term_termid_map = {}
        self.docid_title_map = {}

    def load_docid_title(self, path):
        with open(f"{path}/{constants.DOC_ID_TITLE_MAPPING_FILE_NAME}", 'r') as fp:
            self.docid_title_map = eval(fp.read())

    def load_term_termid(self, path):
        self.term_termid_map = eval(Helpers.uncompress(f"{path}/{constants.TERM_ID_MAPPING_FILE_NAME}.bz2"))
        # with open(f"{path}/{constants.TERM_ID_MAPPING_FILE_NAME}", 'r') as fp:
        #     self.term_termid_map = eval(fp.read())

        # with open(f"{path}/{constants.TERM_ID_MAPPING_FILE_NAME}", 'r') as fp:
        #     for line in fp:
        #         term, termid = line.split(":")
        #         self.term_termid_map[term] = int(termid)

    def load_index(self, path):
        with open(f"{path}/{constants.POSTINGS_FILE_NAME}") as fp:
            for line in fp:
                line = line.strip()
                termid, postings = line.split(constants.TERM_POSTINGS_SEP)
                postings = [int(x) for x in postings.split(constants.DOCIDS_SEP)]
                self.index[int(termid)] = postings

    def get_terms(self, line):
        line = line.lower()
        line = re.sub(r'[^a-z0-9 ]', ' ', line)  # put spaces instead of non-alphanumeric characters
        line = line.split()
        line = [x for x in line if x not in Helpers.stopwords]
        stemmer = PorterStemmer()
        line = [stemmer.stem(word, 0, len(word) - 1) for word in line]
        return line

    def search_index(self, path, queryfile, outputfile):
        Helpers.load_stopwords(STOPWORDS_FILE_PATH)
        self.load_index(path)
        self.load_term_termid(path)
        self.load_docid_title(path)

        log.debug("Index", self.index)
        queryfp = open(queryfile, "r")
        outputfp = open(outputfile, "w")

        # Loop over each query
        for query in queryfp:
            results = []
            query_type = self.get_query_type(query)
            if query_type == ONE_WORD_QUERY:
                results = self.one_word_query(query)
            elif query_type == FREE_TEXT_QUERY:
                results = self.free_text_query(query)
            elif query_type == FIELD_QUERY:
                results = self.field_query(query)

            results = list(results)
            log.info("Results for query: %s", query.rstrip())
            for result in results[:10]:  # print only 10 results
                log.info(result)
                print(result, file=outputfp)
            print(file=outputfp)
            log.info("")

        queryfp.close()
        outputfp.close()

    def one_word_query(self, query):
        terms = self.get_terms(query)
        if len(terms) == 0:
            return
        elif len(terms) > 1:
            return self.free_text_query(query)

        # else terms contains 1 term
        term = terms[0]
        docids = set()
        # import ipdb
        # ipdb.set_trace()

        docids |= set(self.get_postings(f"{term}+T") or [])  # Search in title text
        docids |= set(self.get_postings(f"{term}+B") or [])  # Search in title text
        docids |= set(self.get_postings(f"{term}+I") or [])  # Search in title text
        # TODO: add search in other fields

        return self.get_doc_names_from_ids(docids)

    def free_text_query(self, query):
        terms = self.get_terms(query)
        docids = set()
        for term in terms:
            if not term.isspace():
                docids |= set(self.get_postings(f"{term}+T") or [])  # Search in title text
                docids |= set(self.get_postings(f"{term}+B") or [])  # Search in title text
                docids |= set(self.get_postings(f"{term}+I") or [])  # Search in title text
                docids |= set(self.get_postings(f"{term}+C") or [])  # Search in title text
                docids |= set(self.get_postings(f"{term}+R") or [])  # Search in title text
                docids |= set(self.get_postings(f"{term}+L") or [])  # Search in title text
        return self.get_doc_names_from_ids(docids)

    def field_query(self, field_query):
        FIELD_QUERY_OPERATOR = "OR"  # TODO: decide OR vs AND

        # title:gandhi body:arjun infobox:gandhi category:gandhi ref:gandhi
        docids = set()
        field_terms = field_query.split()  # will now contain ['t:Sachin', 'b:Tendulkar', ...]

        if FIELD_QUERY_OPERATOR == "OR":
            for extended_term in field_terms:
                ft, query = extended_term.split(":")
                terms = self.get_terms(query)
                for term in terms:
                    if not term.isspace():
                        docids |= set(
                            self.get_postings(f"{term}+{field_type_map[ft].upper()}") or [])

        else:  # use AND instead of OR
            # Logic: fill docids of first field type,
            # then perform intersection with subsequent field types
            ft, query = field_terms[0].split(":")
            terms = self.get_terms(query)
            for term in terms:
                if not term.isspace():
                    # Perform OR
                    docids |= set(
                        self.get_postings(f"{term}+{field_type_map[ft].upper()}") or [])

            for extended_term in field_terms[1:]:
                ft, query = extended_term.split(":")
                terms = self.get_terms(query)
                for term in terms:
                    if not term.isspace():
                        # Perform AND (intersection)
                        docids.intersection_update(set(
                            self.get_postings(f"{term}+{field_type_map[ft].upper()}") or []))

        return self.get_doc_names_from_ids(docids)

    @staticmethod
    def get_query_type(query):
        if ":" in query:
            return FIELD_QUERY
        elif len(query.split()) > 1:
            return FREE_TEXT_QUERY
        else:
            return ONE_WORD_QUERY

    def get_doc_names_from_ids(self, docs):
        docnames = set()
        for docid in docs:
            docnames.add(self.docid_title_map.get(str(docid)))
        return docnames

    def get_postings(self, extended_term):
        return self.index.get(self.term_termid_map.get(extended_term))


if __name__ == "__main__":
    srchobj = Search()
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else constants.DEFAULT_INDEX_DIR
    queryfile = sys.argv[2] if len(sys.argv) > 2 else constants.QUERY_FILE
    outputfile = sys.argv[3] if len(sys.argv) > 3 else constants.OUTPUT_FILE

    srchobj.search_index(path, queryfile, outputfile)
