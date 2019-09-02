import heapq
import logging as log
import sys
import time
from collections import defaultdict, OrderedDict

from src.constants import TERM_POSTINGS_SEP, POSTINGS_FILE_NAME, DOCIDS_SEP, DEFAULT_INDEX_DIR, DOCID_TF_SEP

# INDEX_BLOCK_MAX_SIZE = 10 ** 9  # 10^9 => 1000 x 10^6 Bytes = 1000MB = 1GB
INDEX_BLOCK_MAX_SIZE = 10 ** 7  # 10^7 => 10 x 10^6 Bytes = 10MB


class Indexer:
    # termid_docid_list = []
    # postings_list = defaultdict(list)

    @staticmethod
    def bsbi():
        pass

    @staticmethod
    def spimi():
        pass

    # @staticmethod
    # def basic_index():
    #     # TODO: verify whether python GC bug still exists: https://stackoverflow.com/a/2480015/5463404
    #     log.debug("termid_docid_list length: %s", len(Indexer.termid_docid_list))
    #     Indexer.termid_docid_list.sort()
    #     Indexer.postings_list.clear()
    #     for termid, docid in Indexer.termid_docid_list:
    #         Indexer.postings_list[termid].append(docid)

    @staticmethod
    def write_index_to_disk(INDEX_DIR):
        # Write index file
        with open(f"{INDEX_DIR}/{POSTINGS_FILE_NAME}",
                  "w") as fp:  # format=> termid:docid1,docid2,docid3....\n
            for termid in Indexer.postings_list:
                print(
                    f"{termid}{TERM_POSTINGS_SEP}{DOCIDS_SEP.join(Indexer.postings_list[termid])}",
                    file=fp)


class SPIMI:
    n_blocks = 0
    max_block_size = INDEX_BLOCK_MAX_SIZE
    block = defaultdict(list)
    INDEX_DIR = DEFAULT_INDEX_DIR  # FIXME

    def __init__(self, block_size_limit=None):
        pass

    @staticmethod
    def spimi_invert(tokenstream=None, is_last_block=False):
        # fill the block
        for term, term_freq, docid in tokenstream:
            SPIMI.block[term].append(f"{docid}{DOCID_TF_SEP}{term_freq}")

        if sys.getsizeof(SPIMI.block) > SPIMI.max_block_size \
                or is_last_block:
            log.debug("sys.getsizeof(SPIMI.dictionary): %s", sys.getsizeof(SPIMI.block))
            sorted_block = SPIMI.sort_terms(SPIMI.block)
            SPIMI.write_block_to_disk(sorted_block)
            SPIMI.block.clear()  # reset the block

    @staticmethod
    def sort_terms(dictionary):
        start_time = time.process_time()
        log.debug("sorting dictionary")
        # return sorted(dictionary.items())    to get a sorted list of (key, value) pairs

        sorted_dict = OrderedDict()

        for term in sorted(dictionary):  # calling sorted on a dict returns list of keys in sorted order
            # TODO: calculate and add term frequency as well for use in TFIDF search result
            sorted_dict[term] = dictionary[term]

        log.debug("Block sorted in %s seconds", time.process_time() - start_time)
        return sorted_dict

    @staticmethod
    def write_block_to_disk(sorted_block):
        start_time = time.process_time()

        SPIMI.n_blocks += 1

        # TODO: use os.pathjoin

        if type(sorted_block) == list:
            # is sorted_block is a list of terms
            log.debug("Write sorted_block list")

            with open(f"{SPIMI.INDEX_DIR}/block{SPIMI.n_blocks}", "w") as block_fp:
                for term, doclist in sorted_block:
                    block_fp.write(f"{term}{TERM_POSTINGS_SEP}{DOCIDS_SEP.join(doclist)}\n")

        else:  # it's a dictionary
            log.debug("Write sorted_block dict")
            with open(f"{SPIMI.INDEX_DIR}/block{SPIMI.n_blocks}", "w") as block_fp:
                for term in sorted_block:
                    doclist = sorted_block[term]
                    block_fp.write(f"{term}{TERM_POSTINGS_SEP}{DOCIDS_SEP.join(doclist)}\n")

        log.debug("Block written to disk in %s seconds", time.process_time() - start_time)

    @staticmethod
    def merge_blocks():
        """Merge blocks in a single index file"""
        log.debug("Merging %s blocks", SPIMI.n_blocks)

        # buffers that will contain first `few` records of each block file
        READ_BUFFER_SIZE = 10000
        WRITE_BUFFER_SIZE = 100000

        # writer
        write_fp = open(f"{SPIMI.INDEX_DIR}/{POSTINGS_FILE_NAME}", "w")

        # readers
        block_fps = [open(f"{SPIMI.INDEX_DIR}/block{i}", "r") for i in range(1, SPIMI.n_blocks + 1)]

        # remaining_lines is a count of read but not processed(not popped from heap) lines from each block
        remaining_lines = [0 for _ in range(SPIMI.n_blocks)]
        minheap = []

        # initialize the minheap with first `few` lines of the block files
        for block_idx, block_fp in enumerate(block_fps):
            log.debug("Reading into memory %s", block_fp.name)
            # FIXME: next() may raise StopIterationException if READ_BUF_SIZE exceeds available number of lines
            for _ in range(READ_BUFFER_SIZE):
                line = next(block_fp)
                minheap.append((line.rstrip("\n"), block_idx))
                remaining_lines[block_idx] += 1

        heapq.heapify(minheap)  # Convert list to minheap
        log.debug("minheap currently after init length = %s", len(minheap))

        write_buffer = OrderedDict()

        while minheap:  # while heap is not empty
            line, block_idx = heapq.heappop(minheap)

            # log.debug("minheap currently after pop: %s", minheap)
            term, docids = line.split(":", 1)
            docids = docids.split(",")
            write_buffer[term] = write_buffer.get(term, []) + docids

            if len(write_buffer) >= WRITE_BUFFER_SIZE:
                for term in write_buffer:
                    # Flush to index file
                    write_fp.write(
                        f"{term}{TERM_POSTINGS_SEP}{DOCIDS_SEP.join(write_buffer[term])}\n")
                write_buffer.clear()

            remaining_lines[block_idx] -= 1
            if remaining_lines[block_idx] == 0:
                # load more lines from that block
                log.debug("Loading more lines from %s", block_fps[block_idx].name)
                log.debug("remain_lines %s", remaining_lines)
                try:  # make raise StopIteration
                    for _ in range(READ_BUFFER_SIZE):
                        newElem = next(block_fps[block_idx])
                        # log.debug("newElem: %s", newElem)
                        heapq.heappush(minheap, (newElem.rstrip("\n"), block_idx))
                        remaining_lines[block_idx] += 1
                        # log.debug("minheap currently after push: %s", minheap)
                except StopIteration:
                    log.warning("Block file %s exhausted", block_fps[block_idx].name)

        # Minheap empty but write buffer may have some items
        if write_buffer:
            for term in write_buffer:
                # Flush to index file
                write_fp.write(
                    f"{term}{TERM_POSTINGS_SEP}{DOCIDS_SEP.join(write_buffer[term])}\n")

        # Close files
        write_fp.close()
        for block_fp in block_fps:
            block_fp.close()
