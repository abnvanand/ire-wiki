class Helpers:
    stopwords = set()
    docid_docname_map = {}
    term_termid_map = {}
    termid_term_map = {}

    @staticmethod
    def load_stopwords(path):
        with open(path, "r") as fp:
            for line in fp:
                Helpers.stopwords.add(line.strip("\n"))

    @staticmethod
    def get_stopwords():
        return Helpers.stopwords

    @staticmethod
    def addto_term_termid_map(term):
        if term not in Helpers.term_termid_map:
            Helpers.term_termid_map[term] = len(Helpers.term_termid_map) + 1

    @staticmethod
    def get_termid(term):
        return Helpers.term_termid_map[term]

    @staticmethod
    def get_wiki_url(docid):
        docname = Helpers.docid_docname_map[docid]
        url = f"https://en.wikipedia.org/wiki/{docname.replace(' ', ' ')}"
        return url


if __name__ == "__main__":
    Helpers.addto_term_termid_map("hello")
    Helpers.addto_term_termid_map("hi")
    Helpers.addto_term_termid_map("hello")
    Helpers.addto_term_termid_map("hello")
    Helpers.addto_term_termid_map("you")
    Helpers.addto_term_termid_map("hello")
    Helpers.addto_term_termid_map("hello")
    Helpers.addto_term_termid_map("you")
    Helpers.addto_term_termid_map("you")
    print(Helpers.term_termid_map)
