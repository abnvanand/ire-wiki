class Helpers:
    stopwords = set()
    docid_docname_map = {}

    @staticmethod
    def load_stopwords(path):
        with open(path, "r") as fp:
            for line in fp:
                Helpers.stopwords.add(line.strip("\n"))

    @staticmethod
    def get_stopwords():
        return Helpers.stopwords
