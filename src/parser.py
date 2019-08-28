import logging as log
import xml.sax

from src.helpers import Helpers
from src.indexer import Indexer
from src.tokenizer import Tokenizer


class WikipediaHandler(xml.sax.ContentHandler):
    def __init__(self):
        self.tokenizer = None
        self.tag = None
        # title and text and id(documentID) are available as field
        self.title = ""
        self.text = []
        self.id = ""

        # need extra processing for following fields:-
        # infobox , categories , external , references

        # Some booleans to determine nesting
        # there is another tag named id which is nested inside revision tag
        # make sure we DONOT use that as the id
        self.insideRevision = False

    def startElement(self, tag, attributes):
        """
        Signals the start of an element in non-namespace mode.
        """
        self.tag = tag  # for identification in characters() method
        if tag == "title":
            log.debug("%s start", tag)
            self.title = ""  # reset for new title
        elif tag == "text":
            log.debug("%s start", tag)
            self.text = ""  # reset for new text
        elif tag == "revision":
            log.debug("%s start", tag)
            self.insideRevision = True
        elif tag == "id":
            log.debug("%s start", tag)

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
            Helpers.docid_docname_map[self.tokenizer.get_doc_id()] = self.tokenizer.get_title()
            # add text body to that document    # TODO: use append
            termid_freq_map = self.tokenizer.tokenize(self.text)

            # print("term_termid_map", Helpers.term_termid_map)
            for term in termid_freq_map:
                # accumulate (termid: docid) pairs
                Indexer.termid_docid_list.append((term, self.tokenizer.get_doc_id()))

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


class XMLParser:
    def parse(self, path):
        # Create a XMLReader
        parser = xml.sax.make_parser()

        # turn off namespaces
        parser.setFeature(xml.sax.handler.feature_namespaces, 0)

        # override the default ContextHandler
        handler = WikipediaHandler()
        parser.setContentHandler(handler)

        parser.parse(path)
