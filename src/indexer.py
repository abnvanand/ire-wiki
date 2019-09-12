import heapq
import logging as log
import os
import sys
import time
from collections import defaultdict, OrderedDict

from src.constants import ZONES, FREQUENCY, \
    DEFAULT_INDEX_DIR, TMP_BLK_PREFIX, PRIMARY_BLK_PREFIX, SECONDARY_IDX_FILE, PRIMARY_IDX_FILE, \
    ZONES_SEP, ZONE_ZFREQ_SEP, DOCID_TF_ZONES_SEP, TERM_POSTINGLIST_SEP, DOCIDS_SEP, SECONDARY_IDX_FILE_OFFSETVERSION, \
    TERIARY_INDEX_FILE, TERTIARY_GAP

INDEX_BLOCK_MAX_SIZE = 20 * (2 ** 20)  # 20MB  # TODO: adjust


class SPIMI:
    n_temp_blocks = 0
    max_block_size = INDEX_BLOCK_MAX_SIZE
    block = defaultdict(list)
    INDEX_DIR = DEFAULT_INDEX_DIR  # FIXME
    n_primary_blocks = 0

    num_current_docs = 0
    PUSH_LIMIT = 1500000

    def __init__(self, block_size_limit=None):
        pass

    @staticmethod
    def spimi_invert(tokenstream=None, n_terms=1, docid=0, is_last_block=False):
        SPIMI.num_current_docs += 1
        # fill the block
        for term in tokenstream:  # tokenstream is a dict with unique terms
            # Structure of block=> {term1: ["docid1|45|BIT", "docid2|31|ITB"], term2:[....]}
            posting = ZONES_SEP.join([f"{k}{ZONE_ZFREQ_SEP}{v}" for k, v in tokenstream[term][ZONES].items()])
            # Append a tuple (tf, posting) tf will be used to sort postings of a term
            SPIMI.block[term].append(
                (tokenstream[term][FREQUENCY],
                 f"{docid}{DOCID_TF_ZONES_SEP}{tokenstream[term][FREQUENCY]}{DOCID_TF_ZONES_SEP}{posting}"))

        if len(SPIMI.block) >= SPIMI.PUSH_LIMIT \
                or is_last_block:
            SPIMI.num_current_docs = 0  # reset

            log.debug("sys.getsizeof(SPIMI.dictionary): %s", sys.getsizeof(SPIMI.block))

            start_time = time.process_time()
            sorted_block = SPIMI.sort_terms(SPIMI.block)
            SPIMI.block.clear()  # reset the block

            log.debug("Block sorted in %s seconds", time.process_time() - start_time)

            start_time = time.process_time()
            SPIMI.write_block_to_disk(sorted_block)
            log.debug("Block written to disk in %s seconds", time.process_time() - start_time)

    @staticmethod
    def sort_terms(dictionary):
        log.debug("sorting dictionary")
        # return sorted(dictionary.items())  # to get a sorted list of (key, value) pairs

        sorted_dict = OrderedDict()

        for term in sorted(dictionary):  # calling sorted on a dict returns list of keys in sorted order
            postings = dictionary[term]
            # Sort by the tf value
            postings = sorted(postings, key=lambda x: x[0], reverse=True)
            sorted_dict[term] = [x[1] for x in postings]

        return sorted_dict

    @staticmethod
    def write_block_to_disk(sorted_block):
        SPIMI.n_temp_blocks += 1

        if type(sorted_block) == list:
            # sorted_block is a list of terms
            with open(os.path.join(SPIMI.INDEX_DIR, f"{TMP_BLK_PREFIX}{SPIMI.n_temp_blocks}"), "w") as tmp_blk:
                log.debug("Writing sorted temp block list in: %s", tmp_blk.name)
                for term, doclist in sorted_block:
                    tmp_blk.write(f"{term}{TERM_POSTINGLIST_SEP}{DOCIDS_SEP.join(doclist)}\n")

        else:  # it's a dictionary
            with open(os.path.join(SPIMI.INDEX_DIR, f"{TMP_BLK_PREFIX}{SPIMI.n_temp_blocks}"), "w") as tmp_blk:
                log.debug("Writing sorted temp block dict in: %s", tmp_blk.name)
                for term in sorted_block:
                    doclist = sorted_block[term]
                    tmp_blk.write(f"{term}{TERM_POSTINGLIST_SEP}{DOCIDS_SEP.join(doclist)}\n")

    @staticmethod
    def merge_blocks():
        log.info("Merging %s blocks", SPIMI.n_temp_blocks)

        # buffers that will contain first `few` records of each block file
        READ_BUF_LEN = max(100000 // SPIMI.n_temp_blocks, 1000)
        WRITE_BUF_LEN = 20000

        secondary_index = []

        # writer
        primary_fp = open(os.path.join(SPIMI.INDEX_DIR, PRIMARY_IDX_FILE), "w")
        secondary_fp = open(os.path.join(SPIMI.INDEX_DIR, SECONDARY_IDX_FILE_OFFSETVERSION), "w")
        tertiary_fp = open(os.path.join(SPIMI.INDEX_DIR, TERIARY_INDEX_FILE), "w")

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

            term, docids = line.split(TERM_POSTINGLIST_SEP, 1)
            docids = docids.split(DOCIDS_SEP)

            write_buffer[term] = SPIMI.merge_postings_lists(write_buffer.get(term, []), docids)

            if len(write_buffer) >= WRITE_BUF_LEN:
                SPIMI.write_primary_block_to_disk(write_buffer, primary_fp, secondary_fp, tertiary_fp)
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
            SPIMI.write_primary_block_to_disk(write_buffer, primary_fp, secondary_fp, tertiary_fp)
            write_buffer.clear()

        # write to secondary index files
        # close primary main file
        primary_fp.close()
        secondary_fp.close()
        tertiary_fp.close()

        for tmp_blk in tmp_blk_fps:
            filename = tmp_blk.name
            tmp_blk.close()
            # os.remove(f"{filename}")  # STOPSHIP uncomment

    @staticmethod
    def write_primary_block_to_disk(write_buffer, primary_fp, offset_fp, tertiary_fp):
        log.debug("Writing primary block to disk")
        starttime = time.process_time()
        tertiary_limit = TERTIARY_GAP  # writes to tertiary for every n terms in secondary
        # Flush to index file
        for term in write_buffer:
            tertiary_limit -= 1
            if tertiary_limit <= 0:
                tertiary_limit = TERTIARY_GAP
                tertiary_fp.write(f"{term}={offset_fp.tell()}\n")

            offset_fp.write(f"{term}={primary_fp.tell()}\n")
            primary_fp.write(
                f"{term}{TERM_POSTINGLIST_SEP}{DOCIDS_SEP.join(write_buffer[term])}\n")
        log.debug("Written to primary in %s seconds", time.process_time() - starttime)

    @staticmethod
    def get_tf(posting):
        _, tf, __ = posting.split(DOCID_TF_ZONES_SEP)
        return int(tf)

    @staticmethod
    def merge_postings_lists(list1, list2):
        merge = []
        l1 = 0
        l2 = 0
        while l1 < len(list1) and l2 < len(list2):
            if SPIMI.get_tf(list1[l1]) > SPIMI.get_tf(list2[l2]):
                merge.append(list1[l1])
                l1 += 1
            else:
                merge.append(list2[l2])
                l2 += 1
        merge += list1[l1:]
        merge += list2[l2:]
        return merge
