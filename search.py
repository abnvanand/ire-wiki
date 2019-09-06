import logging as log
import os
import re
from bisect import bisect
from collections import defaultdict

from src.constants import *
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
    def __init__(self, indexdir, query_file, output_file):
        self.index_dir = indexdir
        self.QUERY_FILE = query_file
        self.OUTPUT_FILE = output_file

        self.docid_title_map = {}
        self.secondary_index = []

        self.index = defaultdict(list)

        self.n_docs = 0
        self.nameofBlockCurrentlyInMemory = ""

    def load_docid_title(self):
        # {docid: (doctitle, n_terms)}
        with open(os.path.join(self.index_dir, DOC_ID_TITLE_MAPPING_FILE_NAME), 'r') as fp:
            self.docid_title_map = eval(fp.read())
        self.n_docs = len(self.docid_title_map)

    def load_secondary_index(self):
        with open(os.path.join(self.index_dir, SECONDARY_INDEX_FILE), 'r') as fp:
            self.secondary_index = eval(fp.read())

    def load_primary_block(self, block_to_load):
        # TODO: Do not load if already loaded
        if self.nameofBlockCurrentlyInMemory == block_to_load:
            log.debug("Block %s already in memory", block_to_load)
            return

        log.debug("Block %s not in memory. Loading!!!", block_to_load)
        self.index.clear()
        with open(os.path.join(self.index_dir, block_to_load), 'r') as fp:
            for line in fp:
                line = line.strip()
                term, postings = line.split(TERM_POSTINGS_SEP)
                for unit in postings.split(DOCIDS_SEP):
                    docid, freq, zones_tf_pairs = unit.split(DOCID_TF_ZONES_SEP)
                    zones = {}
                    for zone_tf in zones_tf_pairs.split(ZONES_SEP):
                        zone, ztf = zone_tf.split(ZONE_FREQ_SEP)
                        zones[zone] = ztf
                    self.index[term].append((docid, freq, zones))
            self.nameofBlockCurrentlyInMemory = block_to_load

    def get_index(self, term):
        primary_blk_suffix = bisect(self.secondary_index, (term, "z"))  # FIXME: added "z" to match higher than primaryX
        self.load_primary_block(f"{PRIMARY_BLK_PREFIX}{primary_blk_suffix}")
        return self.index[term]

    def get_terms(self, line):
        line = line.lower()
        line = re.sub(r'[^a-z0-9 ]', ' ', line)  # put spaces instead of non-alphanumeric characters
        line = line.split()
        line = [x for x in line if x not in Helpers.stopwords]
        stemmer = PorterStemmer()
        line = [stemmer.stem(word, 0, len(word) - 1) for word in line]
        return line

    def search_index(self):
        Helpers.load_stopwords(STOPWORDS_FILE_PATH)
        self.load_secondary_index()
        self.load_docid_title()

        log.debug("Secondary index", self.secondary_index)
        queryfp = open(self.QUERY_FILE, "r")
        outputfp = open(self.OUTPUT_FILE, "w")

        # Loop over each query
        for query in queryfp:
            docids = []
            query_type = self.get_query_type(query)
            if query_type == ONE_WORD_QUERY:
                docids = self.one_word_query(query)
            elif query_type == FREE_TEXT_QUERY:
                docids = self.free_text_query(query)
            elif query_type == FIELD_QUERY:
                docids = self.field_query(query)

            docids = list(docids)
            # TODO: rank results

            log.info("Results for query: %s", query.rstrip())
            for result in self.get_doc_names_from_ids(docids):  # print only 10 results
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

        # else terms contains only 1 term
        term = terms[0]

        results = []

        for docid, tf, zones in self.get_index(term):
            n_terms = self.docid_title_map[docid][1]
            tf_idf_score = float(tf) * self.get_idf(term)
            results.append((tf_idf_score / n_terms, docid))

        docids = [docid for tfidf, docid in sorted(results, reverse=True)[:10]]
        return docids

    def free_text_query(self, query):
        terms = self.get_terms(query)
        results = set()
        for term in terms:
            if not term.isspace():
                for docid, tf, zones in self.get_index(term):
                    n_terms = self.docid_title_map[docid][1]
                    tf_idf_score = float(tf) * self.get_idf(term)
                    score = tf_idf_score * self.get_idf(term)  # tfidf_t_d x w_t_q
                    results.add((score / n_terms, docid))

        results = sorted(results, reverse=True)
        docids = [docid for tfidf, docid in results[:10]]

        # TODO: rank results using weighted zones not just tfidf scores
        return docids

    def field_query(self, field_query):
        # TODO: decide OR vs AND
        # title:gandhi body:arjun infobox:gandhi category:gandhi ref:gandhi
        docids = set()
        field_terms = field_query.split()  # will now contain ['t:Sachin', 'b:Tendulkar', ...]

        for extended_term in field_terms:
            ft, query = extended_term.split(":")  # ft=t, query=Sachin
            terms = self.get_terms(query)
            for term in terms:
                if term and not term.isspace():
                    for docid, tf, zones in self.get_index(term):
                        if field_type_map[ft] in zones:
                            docids.add(docid)

        # TODO: rank results using cosine similarity and weighted zones
        return list(docids)[:10]

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

    def get_idf(self, term):
        N = self.n_docs
        Nt = len(self.get_index(term))
        from math import log10
        return log10(N / Nt)


if __name__ == "__main__":
    import sys

    index_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INDEX_DIR
    queryfile = sys.argv[2] if len(sys.argv) > 2 else QUERY_FILE
    outputfile = sys.argv[3] if len(sys.argv) > 3 else OUTPUT_FILE

    srchobj = Search(index_dir, queryfile, outputfile)
    srchobj.search_index()
    log.info(srchobj.n_docs)
