import re

from src import constants
from src.constants import STOPWORDS_FILE_PATH
from src.helpers import Helpers
from src.stemmer import PorterStemmer

import logging as log

ONE_WORD_QUERY = "ONE_WORD_QUERY"
FREE_TEXT_QUERY = "FREE_TEXT_QUERY"
FIELD_QUERY = "FIELD_QUERY"


class Search:
    def __init__(self):
        self.index = {}
        self.term_termid_map = {}
        self.docid_title_map = {}

    def load_docid_title(self, path):
        with open(f"{path}/{constants.DOC_ID_TITLE_MAPPING_FILE_NAME}", 'r') as inf:
            self.docid_title_map = eval(inf.read())

    def load_term_termid(self, path):
        with open(f"{path}/{constants.TERM_ID_MAPPING_FILE_NAME}", 'r') as inf:
            self.term_termid_map = eval(inf.read())

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

    def search_index(self, path):
        Helpers.load_stopwords(STOPWORDS_FILE_PATH)
        self.load_index(path)
        self.load_term_termid(path)
        self.load_docid_title(path)

        log.debug("Index", self.index)

        while True:
            query = input("Enter search term: ")
            if not query:
                break

            query_type = self.get_query_type(query)
            if query_type == ONE_WORD_QUERY:
                print(self.one_word_query(query))
            elif query_type == FREE_TEXT_QUERY:
                print(self.free_text_query(query))
            elif query_type == FIELD_QUERY:
                print(self.field_query(query))

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
        return self.get_doc_names_from_ids(docids)

    def field_query(self, field_query):
        # t:Sachin b:Tendulkar c:Sports
        docids = set()

        field_terms = field_query.split()  # will now contain ['t:Sachin', 'b:Tendulkar', ...]

        for extended_term in field_terms:
            field_type, query = extended_term.split(":")  # t:Sachin tendular will be split to t and Sachin tendulkar
            terms = self.get_terms(query)
            for term in terms:
                if not term.isspace():
                    docids |= set(self.get_postings(f"{term}+{field_type.upper()}") or [])  # Search in title text

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


def test():
    field_query = "t:mahatma b:gandhi"


if __name__ == "__main__":
    srchobj = Search()
    path = input("Enter index folder path: ")
    if not path:
        path = constants.DEFAULT_INDEX_DIR

    srchobj.search_index(path)
