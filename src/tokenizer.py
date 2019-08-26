import re
from collections import defaultdict

from src.helpers import Helpers
from src.stemmer import PorterStemmer

infobox_pattern = "{{infobox"


class Tokenizer:

    def __init__(self, title):
        self.title = title
        self.doc_id = -1
        self.body_text = []

        self.termid_freq_map = defaultdict(int)

    def set_title(self, title):
        self.title = title

    def set_doc_id(self, doc_id):
        self.doc_id = doc_id

    def get_doc_id(self):
        return self.doc_id

    def get_title(self):
        return self.title

    def tokenize(self, body_text):
        # tokenize title
        self.extract_token(self.title.lower(), 'T')

        # tokenize body text
        # body_text = " ".join(body_text)  # TODO uncomment when receiving body text as list
        body_text = body_text.lower()

        # todo:
        self.extract_body(body_text)
        self.extract_infobox(body_text)
        # extract_links(body_text)
        # extract_categories(body_text)
        # extract_references(body_text)
        return self.termid_freq_map

    def extract_token(self, content, field_type):
        text = re.sub(r'[^a-z0-9 ]', ' ', content)  # replaces all non-alphanumeric chars by spaces
        # split to tokens
        tokens = re.split("[ ]", text)

        # stopwords removal
        tokens = [term for term in tokens if term not in Helpers.stopwords and not term.isspace()]

        # stemming
        stemmer = PorterStemmer()
        terms = [stemmer.stem(word, 0, len(word) - 1) for word in tokens]
        # Add term to global dict
        # add no of occurrences in current doc in a map
        for term in terms:
            term_with_field = f"{term}+{field_type}"
            Helpers.addto_term_termid_map(term_with_field)
            termid = Helpers.get_termid(term_with_field)
            self.termid_freq_map[termid] += 1
            # self.termid_freq_map[term_with_field] += 1

    def extract_body(self, body_text):
        body_text = re.sub("<ref>.*?</ref>", "", body_text)
        body_text = re.sub("</?.*?>", "", body_text)
        body_text = re.sub("{{.*?\\}\\}", "", body_text)
        body_text = re.sub("\\[\\[.*?:.*?\\]\\]", "", body_text)
        body_text = re.sub("\\[\\[(.*?)\\]\\]", "", body_text)
        body_text = re.sub("\\s(.*?)\\|(\\w+\\s)", " $2", body_text)
        body_text = re.sub("\\[.*?\\]", " ", body_text)
        body_text = re.sub("(?s)<!--.*?-->", "", body_text)  # Remove all NOTE tags
        self.extract_token(body_text, "B")

    def extract_infobox(self, text):
        # find  all infoboxes
        start = 0
        while True:
            start = text.find(infobox_pattern, start)
            if start == -1:
                return

            end = self.find_infobox_end(text, start)

            if end < start:
                # invalid infobox. eg: on page with title: "Wikipedia:Templates for discussion/Log/2014 May 2"
                return

            # log.debug("extracted infobox content: start:%d, end:%d, content %s", start, end, text[start:end])
            # self.infoboxes.append(text[start:end])
            self.extract_token(text[start:end], "I")

            start = end + 1  # look for other infoboxes

    @staticmethod
    def find_infobox_end(text, start):
        search_pos = start + len(infobox_pattern)
        end = search_pos

        while True:
            # if we encounter closing braces before any new opening braces
            # then it means infobox closed
            next_opening_pos = text.find("{{", search_pos)
            next_closing_pos = text.find("}}", search_pos)

            if next_closing_pos <= next_opening_pos or next_opening_pos == -1:
                # if closing braces come before opening braces (or) opening braces do not exist
                end = next_closing_pos + 2
                break
            search_pos = next_closing_pos + 2
        return end
