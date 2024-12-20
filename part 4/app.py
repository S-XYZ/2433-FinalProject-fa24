# -*- coding: utf-8 -*-
"""
@author: Xueyao Zhao
"""

from insuranceAPI import InsuranceAPI
from clientAPI import ClientAPI
from MLAPI import MLAPI
from sensitivityAPI import SensitivityAPI


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
    sensitivity_api = SensitivityAPI()
    
    db_name = "insurance"
    
    # Database setup
    insurance_api.drop_database(db_name)
    insurance_api.create_database(db_name)
    insurance_api.create_tables(db_name)
    # print("Database setup completed.")

    # Data generation
    customer_df = client_api.generate_customer_df(100, 100, 50)
    health_metrics_df = client_api.generate_health_metrics_df(customer_df)
    chronic_disease_history_df = client_api.generate_chronic_disease_history_df(customer_df)

    # Insert data into the database
    insurance_api.insert_dataframe("Customer", customer_df[["CustLastName", "CustFirstName", "Gender", "CustDOB"]], db_name)
    insurance_api.insert_dataframe("HealthMetrics", health_metrics_df, db_name)
    insurance_api.insert_dataframe("ChronicDiseaseHistory", chronic_disease_history_df, db_name)
    # print("Data insertion completed.")

    # Train model
    training_data = insurance_api.fetch_training_data(db_name)
    ml_api.train_model(training_data)

    # Predict
    unlabeled_data = insurance_api.fetch_unlabeled_data(db_name)
    predictions = ml_api.predict_risk(unlabeled_data)

    # Insert predictions into the database
    insurance_api.insert_clustering_results(predictions, db_name)
    
    # Generate products
    product_df = client_api.generate_products(10)
    
    # Insert into the database
    insurance_api.insert_dataframe("Product", product_df, db_name)
    
    # Fetch a random undetermined client
    client_ssn = insurance_api.get_random_undetermined_client(db_name)
    
    # Recommend a product for the fetched client
    #if client_ssn is not None:
    #    recommendation_message = insurance_api.recommend_products(client_ssn, db_name)
    
    # Fetch user name
    client_name_query = f"SELECT CustFirstName, CustLastName FROM Customer WHERE CustSsn = {client_ssn}"
    client_name = insurance_api.run_custom_query(client_name_query, db_name)
    full_name = f"{client_name[0][0]} {client_name[0][1]}"
    

    print(f"\nHello {full_name}! How are you doing today?")
    print("\nBefore we get you a recommendation of insurance (or quote), let's start with some questions first!")
    

    # Step 1: Ask Health-Related Questions
    conversation_history = []
    questions = [
        {"type": "mental", "question": "Can you share how you've been feeling mentally lately?"},
        {"type": "physical", "question": "How would you describe your physical energy and well-being today?"},
        {"type": "happiness", "question": "What’s been bringing you joy recently, or how have you been feeling overall?"}
    ]
    
    responses = {}
    for q in questions:
        print(f"\nSystem: {q['question']}")
        user_input = input("User: ")
        conversation_history.append({
            "role": "user",                # Capturing role for processing (filtered later)
            "question_type": q["type"],    # Question type (mental, physical, happiness)
            "message": user_input          # User response
        })
        responses[q["type"]] = user_input
    
    # End the conversation 
    closing_message = "Thank you for sharing! Let me take a moment to find the best quote or insurance plan tailored for you."
    print(f"\nSystem: {closing_message}")
    
    # Step 2: Analyze Responses
    sentiment_scores = sensitivity_api.process_responses(responses)
    # print("Sentiment Scores:", sentiment_scores)
    
    # Normalize sentiment scores from -1 to 1 to a 1 -100 to 100 scale
    mental_score = sentiment_scores['mental'] * 10
    physical_score = sentiment_scores['physical'] * 10
    happiness_score = sentiment_scores['happiness'] * 10
    
    # Step 3: Update Health Metrics in the Database
    insurance_api.update_health_metrics(
        cust_ssn = client_ssn,
        mental = int(mental_score),
        physical = int(physical_score),
        happiness = int(happiness_score),
        db_name = db_name
    )
    
    # Step 4: Prepare DataFrame for UserNotes Table
    conversation_df = sensitivity_api.prepare_conversation_dataframe(client_ssn, conversation_history)
    
    # Step 5: Insert Conversation (Unstructured Data) into the Database
    insurance_api.insert_dataframe("UserNotes", conversation_df, db_name)
    
    
    # Step 6: Retrain the model
    # Retrain the model
    training_data = insurance_api.fetch_training_data(db_name)
    ml_api.train_model(training_data)
    
    # Fetch data for the specific user whose metrics were updated
    user_data = insurance_api.fetch_unlabeled_data_for_user(db_name, client_ssn)
    
    # Predict risk for the specific user
    predictions = ml_api.predict_risk(user_data)
    
    # Update prediction results for this user
    insurance_api.update_clustering_results(predictions, db_name)


    # Recommend a product for the fetched client
    if client_ssn is not None:
        recommendation_message = insurance_api.recommend_products(client_ssn, db_name)

if __name__ == "__main__":
    main()
