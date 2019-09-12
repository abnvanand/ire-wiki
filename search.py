import logging as log
import os
import re
import resource
import sys
import time
from bisect import bisect, bisect_left
from collections import defaultdict
from math import log10
from operator import itemgetter
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

zone_weights = {
    "T": 500 / 1022,
    "B": 500 / 1022,
    "I": 10 / 1022,
    "C": 10 / 1022,
    "R": 1 / 1022,
    "L": 1 / 1022
}


class Search:
    def __init__(self, indexdir, query_file, output_file):
        self.index_dir = indexdir
        self.QUERY_FILE = query_file
        self.OUTPUT_FILE = output_file

        self.docid_info_dict = {}
        self.doctitles_fp = None

        self.prim_idx_fp = None
        self.secondary_fp = None
        self.tertiary_offset_list = []

        self.N_DOCS = 0  # TOTAL Number of docs in corpus

    def load_tertiary_map(self):
        """Loads entire tertiary index into memory"""
        log.debug("Loading tertiary index terms from %s in memory",
                  TERIARY_INDEX_FILE)

        starttime = time.process_time()
        tertiary_offset_list = []

        with open(os.path.join(self.index_dir, TERIARY_INDEX_FILE), 'r') as fp:
            for line in fp:
                line = line.strip()
                term, offset = line.split(TERM_OFFSET_SEP)
                tertiary_offset_list.append((term, offset))
                # self.term_offset_dict[term] = offset
        self.tertiary_offset_list = tuple(tertiary_offset_list)
        tertiary_offset_list.clear()
        del tertiary_offset_list
        log.debug("Tertiary index loaded in %s sec in memory", time.process_time() - starttime)

    def load_docinfo(self):
        """Loads title offsets and n_terms in memory"""
        log.debug("Loading doc nterms from %s and title offsets from %s",
                  DOC_NTERMS_FILE, DOC_TITLEOFFSET_FILE)

        with open(os.path.join(self.index_dir, DOC_NTERMS_FILE)) as nterms_fp, \
                open(os.path.join(self.index_dir, DOC_TITLEOFFSET_FILE)) as offset_fp:
            for line in nterms_fp:
                docid, n_terms = line.strip().split(DOCID_NTERMS_SEP)
                docid, offset = offset_fp.readline().strip().split(DOCID_OFFSET_SEP)
                self.docid_info_dict[docid] = (int(n_terms), offset)

            # Set total number of docs in corpus
            self.N_DOCS = len(self.docid_info_dict)

        # Open a pointer to actual titles file (DONOT load the entire file)
        self.doctitles_fp = open(os.path.join(self.index_dir, DOC_TITLES_FILE), 'r')
        log.debug("Opened a pointer to doc titles file")

    def mmap_primary_index(self):
        log.debug("Memory mapping primary index from %s", PRIMARY_IDX_FILE)

        # Make sure we have enough swap space available to mmap
        # else we will get OSError: [Errno 12] Cannot allocate memory
        # swap space must be more than the index file available
        # else fallback to file.open()
        # self.prim_idx_fp = mmap(fp.fileno(), 0)
        self.prim_idx_fp = open(os.path.join(self.index_dir, PRIMARY_IDX_FILE), 'r+b')
        log.debug("Pointer to primary index file opened. Non-mmapped version")

    def mmap_secondary_index(self):
        log.debug("Memory mapping secondary index from %s", SECONDARY_IDX_FILE_OFFSETVERSION)

        self.secondary_fp = open(os.path.join(self.index_dir, SECONDARY_IDX_FILE_OFFSETVERSION), 'r')
        log.debug("Pointer to secondary index file opened. Non-mmapped version")

    def get_postinglist(self, term, queryzone=None):
        """Returns first FEW terms from term's postings list"""
        log.debug("Fetching postings for term: %s", term)
        start_time = time.process_time()

        offset = self.get_term_offset(term)
        if offset == -1:  # No such term in index
            return []

        self.prim_idx_fp.seek(offset)  # seek to postings location

        res = []

        line = self.prim_idx_fp.readline()
        log.debug("Fetched postings in: %s", time.process_time() - start_time)

        build_start = time.process_time()

        line = line.decode("utf-8")
        line = line.rstrip()
        term, postings = line.split(TERM_POSTINGLIST_SEP)

        for unit in postings.split(DOCIDS_SEP):
            docid, freq, zones_tf_pairs = unit.split(DOCID_TF_ZONES_SEP)
            zones = {}
            for zone_tf_pair in zones_tf_pairs.split(ZONES_SEP):
                zone, ztf = zone_tf_pair.split(ZONE_ZFREQ_SEP)
                zones[zone] = int(ztf)

            if queryzone and queryzone not in zones:
                # if query asks for a specific zone
                # make sure to only include postings which have that zone
                continue

            zone_score = 0
            for z in zones:
                zone_score += int(zones[z]) * zone_weights[z.upper()]

            # rank the postings by zone based scoring
            # so that we only return FEW good postings
            # res.append((docid, freq, zones))
            res.append((zone_score, docid, freq, zones))

        res = [(docid, freq, zones) for zonescore, docid, freq, zones in sorted(res, reverse=True)[:15000]]
        # res.reverse()
        log.debug("Built postings in: %s", time.process_time() - build_start)

        # limit to first FEW entries of postings list.
        return res[:40000]

    def get_term_offset(self, term):
        log.debug("Getting offset for %s", term)

        # eg ['dog', 'feelanc', 'ganga']; term="feel"
        # so idx returned will be 1
        # but we need to search postings starting from term dog
        # bcoz feel lies before feelanc
        idx = bisect_left(self.tertiary_offset_list, (term, 0))
        idx = max(idx - 1, 0)
        secondary_offset = self.tertiary_offset_list[idx][1]
        # secondary_offset
        self.secondary_fp.seek(int(secondary_offset))
        for _ in range(TERTIARY_GAP + 2):
            line = self.secondary_fp.readline()
            secondary_term, primary_offset = line.strip().split(TERM_OFFSET_SEP)
            if secondary_term == term:
                return int(primary_offset)

        return -1

    def get_terms(self, line):
        line = line.lower().strip()
        line = re.sub(r'[^a-z0-9 ]', ' ', line)  # put spaces instead of non-alphanumeric characters
        line = line.split()
        line = [x for x in line if x not in Helpers.stopwords]
        stemmer = PorterStemmer()
        line = [stemmer.stem(word, 0, len(word) - 1) for word in line]
        return line

    def search_index(self):
        begin = time.process_time()
        Helpers.load_stopwords(STOPWORDS_FILE_PATH)
        log.debug("Stopwords loaded in: %s sec", time.process_time() - begin)

        start_time = time.process_time()
        self.load_docinfo()
        log.debug("Doc info loaded in: %s sec", time.process_time() - start_time)

        start_time = time.process_time()
        self.load_tertiary_map()
        self.mmap_secondary_index()
        self.mmap_primary_index()

        log.info("Loading indexes completed in %s seconds", time.process_time() - begin)

        # queryfp = open(self.QUERY_FILE, "r")
        # outputfp = open(self.OUTPUT_FILE, "w")

        # start_time = time.process_time()

        # Loop over each query
        # for query in queryfp:
        while True:
            query = input("Enter query: ")
            docids = []

            query_type = self.get_query_type(query)

            start_time = time.process_time()

            if query_type == ONE_WORD_QUERY:
                docids = self.one_word_query(query)
            elif query_type == FREE_TEXT_QUERY:
                docids = self.free_text_query(query)
            elif query_type == FIELD_QUERY:
                docids = self.field_query(query)

            log.info("Results for query: %s", query.rstrip())
            doctitles = self.get_doctitles(docids)
            for doctitle in doctitles:  # print only 10 results
                log.info(doctitle)
                # print(doctitle, file=outputfp)
            # print(file=outputfp)
            log.info("")
            log.info("Fetched in: %s sec", time.process_time() - start_time)

        # log.info("Fetched in: %s sec", time.process_time() - start_time)

        # queryfp.close()
        # outputfp.close()

        self.prim_idx_fp.close()
        self.secondary_fp.close()

    def one_word_query(self, query):
        terms = self.get_terms(query)
        if len(terms) == 0:
            return
        elif len(terms) > 1:
            return self.free_text_query(query)

        # else terms contains only 1 term
        term = terms[0]

        results = []
        postingslist = self.get_postinglist(term)
        if len(postingslist) == 0:
            return results

        for docid, tf, zones in postingslist:
            n_terms = self.get_n_terms(docid)
            tf = int(tf)
            tf_idf_score = tf * self.get_idf(len(postingslist))
            results.append((tf_idf_score / n_terms, docid))

        docids = [docid for tfidf, docid in sorted(results, reverse=True)[:10]]
        return docids

    def free_text_query(self, query):
        terms = self.get_terms(query)
        docs_score = defaultdict(lambda: [0 for _ in terms])
        for i, term in enumerate(terms):
            postingslist = self.get_postinglist(term)
            for docid, tf, zones in postingslist:
                tf = int(tf)  # length normalize the document vector
                tf = log10(1 + tf) / log10(self.get_n_terms(docid))
                df = len(postingslist)

                # Wtd = log(1+tf) * log(N/df)
                wtd = tf * log10(self.N_DOCS / df)
                # for now treat each query term equally important
                wtq = log10(self.N_DOCS / df) / len(terms)
                # Dot product of doc's vector's component and query vector's component
                score = wtd * wtq  # tfidf_t_d x w_t_q
                docs_score[docid][i] = score

        for docid in docs_score:
            docs_score[docid] = sum(docs_score[docid])

        docs_score = sorted(docs_score.items(), key=itemgetter(1), reverse=True)
        # docs_score = sorted(docs_score, reverse=True)  # x[0] equals the score
        docids = [docid for docid, score in docs_score[:10]]

        return docids

    def field_query(self, field_query):
        # title:mahatma gandhi body:arjun infobox:gandhi category:gandhi ref:gandhi
        field_terms = field_query.split()
        # field_terms now contains ['t:mahatma','gandhi' 'b:arjun',
        #                           'i:gandhi', 'c:gandhi', 'r:gandhi']
        docs_score = defaultdict(lambda: [0 for _ in field_terms])

        cur_field = ""
        for i, extended_term in enumerate(field_terms):
            splits = extended_term.split(":")  # ft=t, query=Sachin
            if len(splits) > 1:
                # then ft contains field type
                cur_field = splits[0]
                query = splits[1]
            else:
                # ft contains the `term`  and query is empty
                query = splits[0]

            term = self.get_terms(query)
            if term:
                term = term[0]

            # get posting for term
            postingslist = self.get_postinglist(term, field_type_map[cur_field])

            for docid, tf, zones in postingslist:
                if field_type_map[cur_field] not in zones:
                    continue

                # else this doc has this term in the required zone
                tf = log10(1 + int(tf)) / log10(self.get_n_terms(docid))
                df = len(postingslist)
                # Wtd = log(1+tf) * log(N/df)
                wtd = tf * log10(self.N_DOCS / df)
                wtq = 1 / len(field_terms)  # for now treat each query term equally important TODO: do weighted
                # Dot product of doc's vector's component and query vector's component
                score = wtd * wtq  # tfidf_t_d x w_t_q
                docs_score[docid][i] = score

        for docid in docs_score:
            docs_score[docid] = sum(docs_score[docid])
        docs_score = sorted(docs_score.items(), key=itemgetter(1), reverse=True)
        docids = [docid for docid, score in docs_score[:10]]

        return docids

    @staticmethod
    def get_query_type(query):
        if ":" in query:
            return FIELD_QUERY
        elif len(query.split()) > 1:
            return FREE_TEXT_QUERY
        else:
            return ONE_WORD_QUERY

    def get_idf(self, Nt):
        return log10(self.N_DOCS / Nt)

    def get_doctitles(self, docids):
        return [self.get_doctitle(docid) for docid in docids]

    def get_doctitle(self, docid):
        # First get the offset
        offset = self.docid_info_dict[docid][1]
        # Then seek to offset position in file
        self.doctitles_fp.seek(int(offset))
        line = self.doctitles_fp.readline()
        _, title = line.rstrip().split("=")
        return title

    def get_n_terms(self, docid):
        return self.docid_info_dict[docid][0]


def memory_limit():
    soft, hard = resource.getrlimit(resource.RLIMIT_AS)
    available_memory_kB = get_memory()
    resource.setrlimit(resource.RLIMIT_AS, (available_memory_kB * 1024 / 1.1, hard))
    log.debug("Memory limited to: %s kB", available_memory_kB / 1.1)
    return available_memory_kB * 1024 / 1.1


def get_memory():
    """Returns available memory in kBs"""
    with open('/proc/meminfo', 'r') as mem:
        free_memory = 0
        for i in mem:
            sline = i.split()
            # if str(sline[0]) in ('MemFree:', 'Buffers:', 'Cached:'):
            #     free_memory += int(sline[1])
            if str(sline[0]) == 'MemAvailable:':  # Better estimate
                free_memory = int(sline[1])
                break
    log.debug("MemAvailable: %s kB", free_memory)
    return free_memory


def main():
    index_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INDEX_DIR
    queryfile = sys.argv[2] if len(sys.argv) > 2 else QUERY_FILE
    outputfile = sys.argv[3] if len(sys.argv) > 3 else OUTPUT_FILE

    start_time = time.process_time()

    srchobj = Search(index_dir, queryfile, outputfile)
    srchobj.search_index()

    log.info(srchobj.N_DOCS)
    log.info("Total time taken by script: %s seconds", time.process_time() - start_time)


if __name__ == "__main__":
    # print("Running in memory limit: ", memory_limit())  # Limits maximum memory usage
    try:
        main()
    except MemoryError:
        sys.stderr.write('MAXIMUM MEMORY EXCEEDED')
        sys.exit(-1)
