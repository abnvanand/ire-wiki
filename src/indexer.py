import heapq
import logging as log
import os
import sys
import time
from collections import defaultdict, OrderedDict

from src.constants import TERM_POSTINGS_SEP, DOCIDS_SEP, DEFAULT_INDEX_DIR, DOCID_TF_ZONES_SEP, \
    ZONES, FREQUENCY, TMP_BLK_PREFIX, PRIMARY_BLK_PREFIX, SECONDARY_INDEX_FILE

INDEX_BLOCK_MAX_SIZE = (10 ** 7)  # 10**7 Bytes = 10MB  # TODO: adjust


class SPIMI:
    n_temp_blocks = 0
    max_block_size = INDEX_BLOCK_MAX_SIZE
    block = defaultdict(list)
    INDEX_DIR = DEFAULT_INDEX_DIR  # FIXME
    n_primary_blocks = 0

    def __init__(self, block_size_limit=None):
        pass

    @staticmethod
    def spimi_invert(tokenstream=None, n_terms=1, docid=0, is_last_block=False):
        # fill the block
        for term in tokenstream:  # tokenstream is a dict with unique terms
            # Structure of block=> {term1: ["docid1|45|BIT", "docid2|31|ITB"], term2:[....]}
            SPIMI.block[term].append(
                f"{docid}{DOCID_TF_ZONES_SEP}{tokenstream[term][FREQUENCY]}{DOCID_TF_ZONES_SEP}{''.join(tokenstream[term][ZONES])}")

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
            sorted_dict[term] = dictionary[term]

        log.debug("Block sorted in %s seconds", time.process_time() - start_time)
        return sorted_dict

    @staticmethod
    def write_block_to_disk(sorted_block):
        start_time = time.process_time()

        SPIMI.n_temp_blocks += 1

        if type(sorted_block) == list:
            # is sorted_block is a list of terms

            with open(os.path.join(SPIMI.INDEX_DIR, f"{TMP_BLK_PREFIX}{SPIMI.n_temp_blocks}"), "w") as tmp_blk:
                log.debug("Writing sorted temp block list in: %s", tmp_blk.name)
                for term, doclist in sorted_block:
                    tmp_blk.write(f"{term}{TERM_POSTINGS_SEP}{DOCIDS_SEP.join(doclist)}\n")

        else:  # it's a dictionary
            with open(os.path.join(SPIMI.INDEX_DIR, f"{TMP_BLK_PREFIX}{SPIMI.n_temp_blocks}"), "w") as tmp_blk:
                log.debug("Writing sorted temp block dict in: %s", tmp_blk.name)
                for term in sorted_block:
                    doclist = sorted_block[term]
                    tmp_blk.write(f"{term}{TERM_POSTINGS_SEP}{DOCIDS_SEP.join(doclist)}\n")

        log.debug("Block written to disk in %s seconds", time.process_time() - start_time)

    @staticmethod
    def merge_blocks():
        log.info("Merging %s blocks", SPIMI.n_temp_blocks)

        # buffers that will contain first `few` records of each block file
        READ_BUF_LEN = 10000
        WRITE_BUF_LEN = 100000

        secondary_index = []

        # readers
        tmp_blk_fps = [open(os.path.join(SPIMI.INDEX_DIR, f"{TMP_BLK_PREFIX}{i}"), "r")
                       for i in range(1, SPIMI.n_temp_blocks + 1)]

        # remaining_lines is a count of read but not processed(not popped from heap) lines from each block
        remaining_lines = [0 for _ in range(SPIMI.n_temp_blocks)]
        minheap = []

        # initialize the minheap with first `few` lines of the block files
        for block_idx, block_fp in enumerate(tmp_blk_fps):
            log.debug("Reading into memory %s", block_fp.name)
            try:
                for _ in range(READ_BUF_LEN):
                    line = next(block_fp)
                    minheap.append((line.rstrip("\n"), block_idx))
                    remaining_lines[block_idx] += 1
            except StopIteration:
                log.warning("Block file %s exhausted", tmp_blk_fps[block_idx].name)

        heapq.heapify(minheap)  # Convert list to minheap
        log.debug("minheap currently after init length = %s", len(minheap))

        write_buffer = OrderedDict()

        while minheap:  # while heap is not empty
            line, block_idx = heapq.heappop(minheap)

            term, docids = line.split(":", 1)
            docids = docids.split(",")

            if len(write_buffer) == 0:  # first entry in block
                # write name of this primary block file in secondary index
                # along with the first term to be put into that block
                secondary_index.append((term, f"{PRIMARY_BLK_PREFIX}{SPIMI.n_primary_blocks + 1}"))

            write_buffer[term] = write_buffer.get(term, []) + docids

            if len(write_buffer) >= WRITE_BUF_LEN:
                SPIMI.write_primary_block_to_disk(write_buffer)
                write_buffer.clear()

            remaining_lines[block_idx] -= 1
            if remaining_lines[block_idx] == 0:
                # load more lines from that block
                log.debug("Loading more lines from %s", tmp_blk_fps[block_idx].name)
                log.debug("remain_lines %s", remaining_lines)
                try:  # make raise StopIteration
                    for _ in range(READ_BUF_LEN):
                        newElem = next(tmp_blk_fps[block_idx])
                        # log.debug("newElem: %s", newElem)
                        heapq.heappush(minheap, (newElem.rstrip("\n"), block_idx))
                        remaining_lines[block_idx] += 1
                        # log.debug("minheap currently after push: %s", minheap)
                except StopIteration:
                    log.warning("Block file %s exhausted", tmp_blk_fps[block_idx].name)

        # Minheap empty but write buffer may have some items
        if write_buffer:
            SPIMI.write_primary_block_to_disk(write_buffer)
            write_buffer.clear()

        # write to secondary index files
        with open(os.path.join(SPIMI.INDEX_DIR, f"{SECONDARY_INDEX_FILE}"), "w") as secondary_index_fp:
            log.debug("Writing secondary index to: %s", secondary_index_fp.name)
            secondary_index_fp.write(str(secondary_index))

        for tmp_blk in tmp_blk_fps:
            filename = tmp_blk.name
            tmp_blk.close()
            # os.remove(f"{filename}")  # STOPSHIP uncomment

    @staticmethod
    def write_primary_block_to_disk(write_buffer):
        # Flush to index file
        SPIMI.n_primary_blocks += 1
        with open(os.path.join(SPIMI.INDEX_DIR, f"{PRIMARY_BLK_PREFIX}{SPIMI.n_primary_blocks}"),
                  "w") as primary_block_fp:
            log.debug("Writing primary block: %s", primary_block_fp.name)
            for term in write_buffer:
                primary_block_fp.write(
                    f"{term}{TERM_POSTINGS_SEP}{DOCIDS_SEP.join(write_buffer[term])}\n")
