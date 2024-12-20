# -*- coding: utf-8 -*-
"""
@author: Xueyao Zhao
"""

from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report
from sklearn.utils import shuffle



class MLAPI:
    def __init__(self):
        """Initialize an empty RandomForestClassifier."""
        self.model = RandomForestClassifier(random_state=42)

    def train_model(self, training_data):
        """Train the random forest model."""
        feature_columns = [
            "Age", "BMI", "SmokingHabit",
            "DrinkingHabit", "ExerciseLevel", "SleepQuality", "HeartRate", "BloodPressure",
            "Mental", "Physical", "Happiness"
        ]
        
        training_data = shuffle(training_data, random_state=42)
        X = training_data[feature_columns]
        y = training_data["HasChronicDisease"]
        

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

        self.model.fit(X_train, y_train)
        predictions = self.model.predict(X_test)
        # print("Model Training Completed.")
        # print(classification_report(y_test, predictions))

    def predict_risk(self, unlabeled_data):
        """Predict chronic disease risk."""
        feature_columns = [
            "Age", "BMI", "SmokingHabit",
            "DrinkingHabit", "ExerciseLevel", "SleepQuality", "HeartRate", "BloodPressure",
            "Mental", "Physical", "Happiness"
        ]
        X = unlabeled_data[feature_columns]
        predictions = self.model.predict(X)
        probabilities = self.model.predict_proba(X)

        results = unlabeled_data.copy()
        results["AtRisk"] = predictions
        results["RiskLevel"] = probabilities[:, 1] * 100  # Use the probability of being 'AtRisk'
        results["ConfidenceScore"] = probabilities[:, 1]
        return results