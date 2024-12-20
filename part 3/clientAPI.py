# -*- coding: utf-8 -*-
"""
@author: Xueyao Zhao
"""

import random
import pandas as pd
from faker import Faker
from datetime import date

class ClientAPI:
    def __init__(self):
        """Initialize the Faker instance."""
        self.fake = Faker()


    def generate_customer_df(self, count_with_disease=10, count_without_disease=10, count_tbd=10):
        """
        Generate Customer DataFrame with HasChronicDisease flag.
        This will prepare a base dataset to derive other DataFrames.
        """
        customers = []

        # Generate clients with chronic diseases
        for _ in range(count_with_disease):
            customers.append(self._generate_customer(has_chronic_disease=True))

        # Generate clients without chronic diseases
        for _ in range(count_without_disease):
            customers.append(self._generate_customer(has_chronic_disease=False))

        # Generate clients to be determined (TBD)
        for _ in range(count_tbd):
            customers.append(self._generate_customer(has_chronic_disease=None))
    

        # Return as a DataFrame
        return pd.DataFrame(customers)


    def _generate_customer(self, has_chronic_disease):
        """Generate a single customer's basic data."""
        return {
            "CustLastName": self.fake.last_name(),
            "CustFirstName": self.fake.first_name(),
            "Gender": random.choice(["M", "F"]),
            "CustDOB": self.fake.date_of_birth(minimum_age=18, maximum_age=80),
            "HasChronicDisease": has_chronic_disease  # Used to derive related tables
        }


    def generate_health_metrics_df(self, customer_df):
        """Generate health metrics for customers based on their chronic disease status."""
        health_metrics = []

        for cust_id, row in customer_df.iterrows():
            # Generate health metrics based on HasChronicDisease status
            health_metrics.append({
                "CustSsn": cust_id + 1,  # Assume auto-increment starts at 1
                "MetricDate": self.fake.date_this_decade(),
                "Age": self._calculate_age(row["CustDOB"]),
                "Weight": self._generate_weight(row["HasChronicDisease"]),
                "Height": self._generate_height(),
                "BMI": self._generate_bmi(row["HasChronicDisease"]),
                "SmokingHabit": self._generate_smoking_habit(row["HasChronicDisease"]),
                "DrinkingHabit": self._generate_drinking_habit(row["HasChronicDisease"]),
                "ExerciseLevel": self._generate_exercise_level(row["HasChronicDisease"]),
                "SleepQuality": self._generate_sleep_quality(row["HasChronicDisease"]),
                "HeartRate": self._generate_heart_rate(row["HasChronicDisease"]),
                "BloodPressure": self._generate_blood_pressure(row["HasChronicDisease"]),
            })

        return pd.DataFrame(health_metrics)


    def generate_chronic_disease_history_df(self, customer_df):
        """Generate chronic disease history for customers with a chronic disease."""
        disease_history = []

        for cust_id, row in customer_df.iterrows():
            if row["HasChronicDisease"] is True:
                disease_history.append({
                    "CustSsn": cust_id + 1,
                    "DiagnosisDate": self.fake.date_this_decade(),
                    "HasChronicDisease": True,
                    "Severity": random.randint(1, 5)  # Severity level from 1 to 5
                })
            if row["HasChronicDisease"] is False:
                disease_history.append({
                    "CustSsn": cust_id + 1,
                    "DiagnosisDate": self.fake.date_this_decade(),
                    "HasChronicDisease": False,
                    "Severity": 0
                })
                

        return pd.DataFrame(disease_history)

    # BMI suppose to be derived from height and weight
    # This is generated for ease
    def _generate_bmi(self, has_chronic_disease):
        """Generate BMI based on chronic disease status."""
        if has_chronic_disease is True:
            return round(random.uniform(30.0, 40.0), 1)  # High BMI
        elif has_chronic_disease is False:
            return round(random.uniform(18.0, 24.9), 1)  # Healthy BMI
        else:
            return round(random.uniform(18.0, 30.0), 1)  # undetermined


    def _generate_blood_pressure(self, has_chronic_disease):
        """Generate blood pressure based on chronic disease status."""
        if has_chronic_disease is True:
            return random.randint(140, 180)  # High blood pressure
        elif has_chronic_disease is False:
            return random.randint(90, 120)  # Normal blood pressure
        else:
            return random.randint(90, 180)  # undetermined


    def _calculate_age(self, dob):
        """Calculate age based on date of birth."""
        today = date.today()
        return today.year - dob.year - ((today.month, today.day) < (dob.month, dob.day))


    def _generate_weight(self, has_chronic_disease):
        """Generate weight based on chronic disease status."""
        if has_chronic_disease is True:
            return round(random.uniform(80.0, 120.0), 1)  # Heavier weight
        elif has_chronic_disease is False:
            return round(random.uniform(50.0, 75.0), 1)  # Healthy weight
        else:
            return round(random.uniform(50.0, 120.0), 1)  # Undetermined


    def _generate_height(self):
        """Generate height (randomly distributed)."""
        return round(random.uniform(150.0, 200.0), 1)  # Height in cm


    def _generate_smoking_habit(self, has_chronic_disease):
        """Generate smoking habit based on chronic disease status."""
        if has_chronic_disease is True:
            return random.choice([1, 1, 0])  # More likely to smoke
        elif has_chronic_disease is False:
            return random.choice([0, 0, 1])  # Less likely to smoke
        else:
            return random.randint(0, 1)  # Undetermined


    def _generate_drinking_habit(self, has_chronic_disease):
        """Generate drinking habit based on chronic disease status."""
        if has_chronic_disease is True:
            return random.choice([1, 1, 0])  # More likely to drink
        elif has_chronic_disease is False:
            return random.choice([0, 0, 1])  # Less likely to drink
        else:
            return random.randint(0, 1)  # Undetermined


    def _generate_exercise_level(self, has_chronic_disease):
        """Generate exercise level based on chronic disease status."""
        if has_chronic_disease is True:
            return random.randint(1, 4)  # Low exercise level
        elif has_chronic_disease is False:
            return random.randint(7, 10)  # High exercise level
        else:
            return random.randint(1, 10)  # Undetermined


    def _generate_sleep_quality(self, has_chronic_disease):
        """Generate sleep quality based on chronic disease status."""
        if has_chronic_disease is True:
            return random.randint(1, 5)  # Poor sleep quality
        elif has_chronic_disease is False:
            return random.randint(7, 10)  # Good sleep quality
        else:
            return random.randint(1, 10)  # Undetermined


    def _generate_heart_rate(self, has_chronic_disease):
        """Generate heart rate based on chronic disease status."""
        if has_chronic_disease is True:
            return random.randint(80, 100)  # Elevated heart rate
        elif has_chronic_disease is False:
            return random.randint(60, 80)  # Normal heart rate
        else:
            return random.randint(60, 100)  # Undetermined
        
    def generate_products(self, num_products):
        """
        Generate a specified number of random products.
        :param num_products: Number of products to generate.
        :return: A pandas DataFrame with product details.
        """
        products = []
    
        for _ in range(num_products):
            products.append({
                "LineOfBusiness": None,  # Auto-increment field
                "SeriesName": self.fake.bs().split(" ")[-1].capitalize(),
                "PlanName": self.fake.catch_phrase(),
                "RiskLevel": random.randint(0, 5)  # Random risk level on a scale of 0–5
            })
    
        return pd.DataFrame(products)

    
