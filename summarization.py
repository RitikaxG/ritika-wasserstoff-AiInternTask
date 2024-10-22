import os
import re
import math
from collections import Counter
import html

# Optional: Importing NLP libraries for enhanced summary generation
try:
    import nltk
    from nltk.tokenize import sent_tokenize
    nltk.download('punkt', quiet=True)
except ImportError:
    nltk = None

# Function to generate a dynamic summary based on document length
def generate_summary(text):
    # Unescape HTML entities and remove unnecessary characters like line breaks and excess whitespace
    text = html.unescape(text)
    text = re.sub(r'\s+', ' ', text.replace("\n", " ").replace("\\", "").replace(";", "").replace(":", "")).strip()
    text = re.sub(r'[\u201c\u201d\u2014\u2026]+', '', text)  # Remove unwanted unicode characters
    text = re.sub(r'\b\d+\b', '', text)  # Remove numbers
    
    # Split the text into sentences using nltk if available, otherwise use regex
    if nltk:
        sentences = sent_tokenize(text)
    else:
        sentences = re.split(r'(?<!\w\.\w.)(?<![A-Z][a-z]\.)((?<=\.|\?|!))\s', text)
    
    if len(sentences) <= 4:
        # If there are only a few sentences, use the entire text as the summary
        return text

    # Custom sentence scoring based on word frequency (ignoring common stop words)
    stop_words = set([
        "the", "and", "is", "in", "to", "of", "a", "that", "it", "on", "for", 
        "with", "as", "by", "this", "an", "be", "at", "which", "or", "from", 
        "was", "were", "their", "there", "can", "will", "would"
    ])
    
    word_freq = Counter(
        word.lower() for word in re.findall(r'\w+', text) if word.lower() not in stop_words and len(word) > 4
    )
    
    # Score sentences based on the frequency of significant words they contain
    sentence_scores = {}
    for i, sentence in enumerate(sentences):
        words = re.findall(r'\w+', sentence)
        score = sum(word_freq[word.lower()] for word in words if word.lower() in word_freq)
        sentence_scores[i] = score
    
    # Adjust the number of sentences selected based on document length
    if len(sentences) <= 10:
        num_sentences = 2  # Short document
    elif len(sentences) <= 30:
        num_sentences = 3  # Medium document
    else:
        num_sentences = 5  # Long document
    
    # Select the top-scoring sentences for the summary
    top_sentences_indices = sorted(sentence_scores, key=sentence_scores.get, reverse=True)[:num_sentences]
    top_sentences = [sentences[i] for i in sorted(top_sentences_indices)]
    
    # Return the final summary
    summary = ' '.join(top_sentences)
    return summary.strip()

# Function to extract domain-specific keywords using a custom TF-IDF implementation
def extract_keywords(text):
    # Remove unnecessary characters and filter out stop words
    stop_words = set([
        "the", "and", "is", "in", "to", "of", "a", "that", "it", "on", "for", 
        "with", "as", "by", "this", "an", "be", "at", "which", "or", "from", 
        "was", "were", "their", "there", "can", "will", "would"
    ])
    
    # Preprocess the text to extract significant words
    words = [word.lower() for word in re.findall(r'\w+', text) if word.lower() not in stop_words and len(word) > 4]
    
    # Calculate term frequency (TF)
    word_counts = Counter(words)
    total_words = len(words)
    tf_scores = {word: count / total_words for word, count in word_counts.items()}
    
    # Assume we have a larger document corpus and each word appears in multiple documents
    num_docs = 20
    df_scores = {word: min(5, max(1, word_counts[word] // 2)) for word in word_counts}
    
    # Calculate inverse document frequency (IDF)
    idf_scores = {word: math.log(num_docs / (1 + df_scores[word])) for word in word_counts}
    
    # Calculate TF-IDF scores
    tf_idf_scores = {word: tf_scores[word] * idf_scores[word] for word in word_counts}
    
    # Sort and select top 10 keywords, focusing on domain-specific terms
    sorted_keywords = sorted(tf_idf_scores, key=tf_idf_scores.get, reverse=True)[:10]
    
    # Refine keywords to make them domain-specific and non-generic
    domain_specific_keywords = [
        word for word in sorted_keywords if word not in stop_words and len(word) > 4
    ]
    
    return domain_specific_keywords

# Functions `generate_summary` and `extract_keywords` have been updated to work with text rather than requiring file paths.

