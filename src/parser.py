import logging as log
import time
import xml.sax

from src.constants import *
from src.indexer import SPIMI
from src.tokenizer import Tokenizer


class WikipediaHandler(xml.sax.ContentHandler):
    def __init__(self, INDEX_DIR):
        super().__init__()
        self.index_dir = INDEX_DIR  # to be passed to indexer
        self.docCount = 0  # maintain count of docs
        self.tokenizer = None
        self.tag = None
        # title and text and id(documentID) are available as field
        self.id = ""
        self.title = ""
        self.text = []
        # need extra processing for following fields:-
        # infobox , categories , external , references

        # Some booleans to determine nesting
        # there is another tag named id which is nested inside revision tag
        # make sure we DONOT use that as the id
        self.insideRevision = False

        self.tokenstream = None

        self.doctitle_primary_fp = None
        self.doctitle_offset_fp = None
        self.doctermcount_fp = None

    def startDocument(self):
        log.debug("startDocument")
        self.doctitle_primary_fp = open(f"{self.index_dir}/{DOC_TITLES_FILE}", "w")
        self.doctitle_offset_fp = open(f"{self.index_dir}/{DOC_TITLEOFFSET_FILE}", "w")
        self.doctermcount_fp = open(f"{self.index_dir}/{DOC_NTERMS_FILE}", "w")
        SPIMI.INDEX_DIR = self.index_dir

    def startElement(self, tag, attributes):
        """
        Signals the start of an element in non-namespace mode.
        """
        self.tag = tag  # for identification in characters() method
        if tag == "title":
            self.title = ""  # reset for new title
        elif tag == "text":
            self.text.clear()  # reset for new text
        elif tag == "revision":
            self.insideRevision = True
        elif tag == "id":
            self.id = ""
            pass

    def endElement(self, tag):
        """
        Signals the end of an element in non-namespace mode.
        """
        if tag == "title":
            # initialize a new document with title ==self.title
            self.tokenizer = Tokenizer(self.title)
            self.tokenizer.set_title(self.title)

        elif tag == "text":
            # By now the document title and id fields must have been extracted
            docid = self.tokenizer.get_doc_id()

            # Tokenize the terms of current page(document)
            self.tokenstream, n_terms = self.tokenizer.tokenize(''.join(self.text))

            # Helpers.docid_docname_map[docid] = (self.tokenizer.get_title(), n_terms)
            # file.write() calls are automatically buffered so we don't need to handle that
            self.doctitle_offset_fp.write(f"{docid}{DOCID_NTERMS_SEP}{self.doctitle_primary_fp.tell()}\n")
            self.doctitle_primary_fp.write(f"{docid}{DOCID_NTERMS_SEP}{self.title}\n")
            self.doctermcount_fp.write(f"{docid}{DOCID_NTERMS_SEP}{n_terms}\n")

            # Control reaches here once for every page. (Precisely when the page ends)
            # So this is a good place to build a term docid mapping
            # Build a (term, freq, docid) tuple
            # and call indexer to build index of terms
            # self.tokenstream = [(term, term_map[term][FREQUENCY] / n_terms, docid) for term in term_map]

            # NOTE: indexer might delay indexing of terms if the memory block is not full
            SPIMI.spimi_invert(tokenstream=self.tokenstream, n_terms=n_terms, docid=docid, is_last_block=False)

            self.tokenstream.clear()  # clear for next iter

        elif tag == "id" and not self.insideRevision:
            # DoNOT set id if inside <revision> <id>XXX</id>
            self.tokenizer.set_doc_id(self.id)

        elif tag == "revision":
            self.insideRevision = False  # </revision> encountered
        elif tag == "page":
            self.docCount += 1
            if self.docCount % 1000 == 0:
                # Log processing of every 1000 docs
                log.debug("Parsed pages till %s, total: %s", self.title, self.docCount)
        self.tag = None

    def characters(self, content):
        """
        Receive notification of character data.

        The Parser will call this method to report each chunk of character data.
        SAX parsers may return all contiguous character data in a single chunk,
        or they may split it into several chunks;
        """
        if self.tag == "title":
            self.title += content
        elif self.tag == "text":
            # Using append instead of assignment to handle case where text is received in multiple chunks
            self.text += content
        elif self.tag == "id" and not self.insideRevision:
            self.id += content

    def endDocument(self):
        """Receive notification of the end of a document."""
        # create index over data
        # call for writing last block to disk
        log.debug("endDocument")

        # force flush last block to disk
        SPIMI.spimi_invert(tokenstream=[], is_last_block=True)  # True forces indexer to flush block to disk

        # call for merging all blocks
        start = time.process_time()
        SPIMI.merge_blocks()
        log.info("Merged in %s", time.process_time() - start)

        self.doctitle_primary_fp.close()
        self.doctitle_offset_fp.close()
        self.doctermcount_fp.close()


class XMLParser:
    def parse(self, DUMP_PATH, INDEX_DIR):
        # Create a XMLReader
        parser = xml.sax.make_parser()

        # turn off namespaces
        parser.setFeature(xml.sax.handler.feature_namespaces, 0)

        # override the default ContextHandler
        handler = WikipediaHandler(INDEX_DIR)
        parser.setContentHandler(handler)

        parser.parse(DUMP_PATH)
