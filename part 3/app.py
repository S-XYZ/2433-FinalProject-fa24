# -*- coding: utf-8 -*-
"""
@author: Xueyao Zhao
"""

from insuranceAPI import InsuranceAPI
from clientAPI import ClientAPI
from MLAPI import MLAPI


def main():
    # MySQL credentials
    # Sorry for hardcoding as OS failed to work on my end
    host = "localhost"
    user = "root" 
    password = "Password1234" # change as necessary

    # Initialize APIs
    insurance_api = InsuranceAPI(host, user, password)
    client_api = ClientAPI()
    ml_api = MLAPI()
    
    db_name = "insurance"
    
    # Database setup
    print("Setting up the database...")
    insurance_api.drop_database(db_name)
    insurance_api.create_database(db_name)
    insurance_api.create_tables(db_name)
    print("Database setup completed.")

    # Data generation
    print("Generating customer data...")
    customer_df = client_api.generate_customer_df(100, 100, 50)
    health_metrics_df = client_api.generate_health_metrics_df(customer_df)
    chronic_disease_history_df = client_api.generate_chronic_disease_history_df(customer_df)

    # Insert data into the database
    print("Inserting customer data...")
    insurance_api.insert_dataframe("Customer", customer_df[["CustLastName", "CustFirstName", "Gender", "CustDOB"]], db_name)
    print("Inserting health metrics...")
    insurance_api.insert_dataframe("HealthMetrics", health_metrics_df, db_name)
    print("Inserting chronic disease history...")
    insurance_api.insert_dataframe("ChronicDiseaseHistory", chronic_disease_history_df, db_name)
    print("Data insertion completed.")

    # Train model
    print("Fetching training data...")
    training_data = insurance_api.fetch_training_data(db_name)
    print("Training the model...")
    ml_api.train_model(training_data)

    # Predict
    print("Fetching data for prediction...")
    unlabeled_data = insurance_api.fetch_unlabeled_data(db_name)
    # print(unlabeled_data)
    print("Predicting chronic disease risk...")
    predictions = ml_api.predict_risk(unlabeled_data)

    # Insert predictions into the database
    print("Inserting prediction results...")
    insurance_api.insert_clustering_results(predictions, db_name)
    
    # Generate products
    product_df = client_api.generate_products(10)
    
    # Insert into the database
    insurance_api.insert_dataframe("Product", product_df, db_name)
    
    # Fetch a random undetermined client
    client_ssn = insurance_api.get_random_undetermined_client(db_name)

    # Recommend a product for the fetched client
    if client_ssn is not None:
        recommendation_message = insurance_api.recommend_products(client_ssn, db_name)

    print("Process completed.")

if __name__ == "__main__":
    main()
