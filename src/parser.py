import logging as log
import xml.sax

from src.helpers import Helpers
from src.indexer import SPIMI
from src.tokenizer import Tokenizer


class WikipediaHandler(xml.sax.ContentHandler):
    def __init__(self):
        super().__init__()
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

    def startElement(self, tag, attributes):
        """
        Signals the start of an element in non-namespace mode.
        """
        self.tag = tag  # for identification in characters() method
        if tag == "title":
            self.title = ""  # reset for new title
        elif tag == "text":
            self.text = ""  # reset for new text
        elif tag == "revision":
            self.insideRevision = True
        elif tag == "id":
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

            Helpers.docid_docname_map[docid] = self.tokenizer.get_title()

            terms = self.tokenizer.tokenize(self.text)

            # Control reaches here one for every page. (Precisely when the page ends)
            # So this is a good place to build a term docid mapping
            # Build a (term, docid) pair
            # and call indexer to build index of terms
            self.tokenstream = [(term, docid) for term in terms]
            # NOTE: indexer might delay indexing of terms if the memory block is not full
            SPIMI.spimi_invert(self.tokenstream)

        elif tag == "id" and not self.insideRevision:
            # DoNOT set id if inside <revision> <id>XXX</id>
            self.tokenizer.set_doc_id(self.id)

        elif tag == "revision":
            self.insideRevision = False  # </revision> encountered

        self.tag = None

    def characters(self, content):
        """
        Receive notification of character data.

        The Parser will call this method to report each chunk of character data.
        SAX parsers may return all contiguous character data in a single chunk,
        or they may split it into several chunks;
        """
        if self.tag == "title":
            self.title = content
        elif self.tag == "text":
            # Using append instead of assignment to handle case where text is received in multiple chunks
            self.text += content
        elif self.tag == "id" and not self.insideRevision:
            self.id = content

    def endDocument(self):
        """Receive notification of the end of a document."""
        # create index over data
        # call for writing last block to disk
        log.debug("endDocument")

        # write the last block if it is not empty
        if self.tokenstream:
            SPIMI.spimi_invert(self.tokenstream, is_last_block=True)  # True forces indexer to flush block to disk

        # TODO: call for merging all blocks
        SPIMI.merge_blocks()


class XMLParser:
    def parse(self, DUMP_PATH, INDEX_DIR):
        # Create a XMLReader
        parser = xml.sax.make_parser()

        # turn off namespaces
        parser.setFeature(xml.sax.handler.feature_namespaces, 0)

        # override the default ContextHandler
        handler = WikipediaHandler()
        parser.setContentHandler(handler)

        parser.parse(DUMP_PATH)
