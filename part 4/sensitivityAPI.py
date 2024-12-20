# -*- coding: utf-8 -*-
"""
@author: Xueyao Zhao
"""

from textblob import TextBlob
import pandas as pd
from datetime import datetime

class SensitivityAPI:
    def __init__(self):
        """Initialize Sensitivity API."""
        pass

    def prepare_conversation_dataframe(self, cust_ssn, conversation_history):
        """
        Prepare a DataFrame for inserting conversation history into the database.
        Args:
            cust_ssn (int): Customer SSN.
            conversation_history (list of dict): List of conversation messages with question type and content.
        Returns:
            pd.DataFrame: DataFrame formatted for UserNotes table.
        """
        data = []
        for entry in conversation_history:
            # Only include user responses with their corresponding question type
            if entry["role"] == "user":  # Ensure we only process user responses
                data.append({
                    "CustSsn": cust_ssn,
                    "NoteDate": datetime.now().date(),
                    "QuestionType": entry["question_type"],  # e.g., 'mental', 'physical', 'happiness'
                    "NoteContent": entry["message"]
                })
        return pd.DataFrame(data)

    def process_responses(self, responses):
        """
        Process unstructured user responses to compute sentiment-based scores.
        Args:
            responses (dict): A dictionary of user responses keyed by question type.
        Returns:
            dict: Sentiment scores for each response.
        """
        sentiment_scores = {}
        for question, response in responses.items():
            blob = TextBlob(response)
            sentiment = blob.sentiment.polarity  # Sentiment polarity (-1 to 1)
            sentiment_scores[question] = sentiment
        return sentiment_scores


    def calculate_sensitivity_score(self, sentiment_scores):
        """
        Compute an overall sensitivity score from sentiment scores.
        Args:
            sentiment_scores (dict): Sentiment scores for each question.
        Returns:
            float: The overall sensitivity score.
        """
        if not sentiment_scores:
            return 0.0
        total_score = sum(sentiment_scores.values())
        sensitivity_score = total_score / len(sentiment_scores)
        return sensitivity_score
