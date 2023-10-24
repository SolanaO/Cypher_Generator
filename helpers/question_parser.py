import spacy
from spacy.matcher import PhraseMatcher
from fuzzywuzzy import fuzz

nlp = spacy.load("en_core_web_sm")

def extract_relevant_nodes(question, node_labels, 
                           method="phrase_match", 
                           threshold=65):
    """
    Extract relevant nodes from a question.

    Parameters:
    - question (str): User question.
    - node_labels (list): List of node labels.
    - method (str): Matching method. Either "phrase_match" or "fuzzy".
    - threshold (int): Similarity threshold for fuzzy matching. Defaults to 85.

    Returns:
    - list: Matched node labels.
    """

    doc = nlp(question.lower())
    question_nouns = [token.text for token in doc if token.pos_ == "NOUN"]
    lowercase_node_labels = [label.lower() for label in node_labels]

    if method == "phrase_match":
        matcher = PhraseMatcher(nlp.vocab, attr="LOWER")
        patterns = [nlp.make_doc(text) for text in lowercase_node_labels]
        matcher.add("NODELABEL", None, *patterns)

        matches = matcher(doc)
        matched_labels = [node_labels[lowercase_node_labels.index(doc[start:end].text)] for _, start, end in matches]

    elif method == "fuzzy":
        matched_labels = []
        for noun in question_nouns:
            for label in lowercase_node_labels:
                if fuzz.ratio(noun, label) >= threshold:
                    matched_labels.append(node_labels[lowercase_node_labels.index(label)])
        matched_labels = list(set(matched_labels))  # Remove potential duplicates

    else:
        raise ValueError("Unknown method. Choose either 'phrase_match' or 'fuzzy'.")

    return matched_labels