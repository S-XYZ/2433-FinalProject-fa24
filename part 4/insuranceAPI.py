# -*- coding: utf-8 -*-
"""
@author: Xueyao Zhao
"""

import mysql.connector
import pandas as pd
import random

class InsuranceAPI:
    def __init__(self, host, user, password):
        """Initialize the connection settings."""
        self.host = host
        self.user = user
        self.password = password
        self.connection = None
        self.cursor = None

    def connect(self, db_name=None):
        """Connect to MySQL server and optionally to a specific database."""
        if db_name:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=db_name  # Connect directly to the specified database
            )
        else:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password
            )
        self.cursor = self.connection.cursor()

    def create_database(self, db_name):
        """Create a database if it doesn't exist."""
        self.connect()
        try:
            self.cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
            # print(f"Database '{db_name}' created successfully!")
            self.cursor.execute(f"USE {db_name}")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            self.close_connection()

    def drop_database(self, db_name):
        """Drop the database if it exists."""
        self.connect()
        try:
            self.cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
            # print(f"Database '{db_name}' dropped successfully!")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            self.close_connection()

    def create_tables(self, db_name):
        """Create tables in the specified database."""
        self.connect()
        try:
            self.cursor.execute(f"USE {db_name}")
            table_creation_queries = table_creation_queries = [
    """
    CREATE TABLE Company (
        CompanyCode INT AUTO_INCREMENT PRIMARY KEY,
        CompanyName VARCHAR(50)
    )
    """,
    """
    CREATE TABLE Product (
    LineOfBusiness INT AUTO_INCREMENT PRIMARY KEY,
    SeriesName VARCHAR(100),
    PlanName VARCHAR(100),
    RiskLevel TINYINT CHECK (RiskLevel BETWEEN 0 AND 5)  -- Risk level on a scale of 0–5
    )
    """,
    """
    CREATE TABLE Contract (
        ContractNum INT AUTO_INCREMENT PRIMARY KEY,
        CoverageType VARCHAR(200),
        ItemName VARCHAR(200),
        Quantity INT,
        ReorderLevel INT,
        LastUpdated DATE
    )
    """,
    """
    CREATE TABLE Contract_Product_Asso (
        ContractNum INT NOT NULL,
        LineOfBusiness INT NOT NULL,
        PRIMARY KEY (ContractNum, LineOfBusiness),
        FOREIGN KEY (ContractNum) REFERENCES Contract(ContractNum),
        FOREIGN KEY (LineOfBusiness) REFERENCES Product(LineOfBusiness)
    )
    """,
    """
    CREATE TABLE Customer (
        CustSsn INT AUTO_INCREMENT PRIMARY KEY,
        CustLastName VARCHAR(50),
        CustFirstName VARCHAR(50),
        CustMidInit VARCHAR(1),
        CustDOB DATE,
        Gender VARCHAR(10),
        CustSalutation VARCHAR(50)
    )
    """,
    """
    CREATE TABLE Address (
        Address1 VARCHAR(200),
        Address2 VARCHAR(200),
        City VARCHAR(50),
        State VARCHAR(50),
        Zip VARCHAR(10),
        CustSsn INT NOT NULL,
        Type VARCHAR(50),
        PRIMARY KEY (CustSsn, Address1),
        FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn)
    )
    """,
    """
    CREATE TABLE CustomerNotes (
        NoteID INT AUTO_INCREMENT PRIMARY KEY,
        CustSsn INT NOT NULL,
        NoteDate DATE,
        NoteType VARCHAR(50),
        NoteContent TEXT,
        FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn)
    )
    """,
    """
    CREATE TABLE Customer_Contract_Claims (
        CustSsn INT NOT NULL,
        ContractNum INT NOT NULL,
        ClaimDate DATE,
        SettlementDate DATE,
        PRIMARY KEY (CustSsn, ContractNum),
        FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn),
        FOREIGN KEY (ContractNum) REFERENCES Contract(ContractNum)
    )
    """,
    """
    CREATE TABLE Customer_Contract_Benefits (
        CustSsn INT NOT NULL,
        ContractNum INT NOT NULL,
        BenefitName VARCHAR(50),
        SettlementDate DATE,
        PRIMARY KEY (CustSsn, ContractNum, BenefitName),
        FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn),
        FOREIGN KEY (ContractNum) REFERENCES Contract(ContractNum)
    )
    """,
    """
    CREATE TABLE Account (
        AccountID INT AUTO_INCREMENT PRIMARY KEY,
        AccountName VARCHAR(50),
        StartDate DATE,
        AccountType VARCHAR(50)
    )
    """,
    """
    CREATE TABLE BillingAccount (
        BillingAccountID INT AUTO_INCREMENT PRIMARY KEY,
        CustSsn INT NOT NULL,
        AccountID INT NOT NULL,
        BillingAccountName VARCHAR(50),
        FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn),
        FOREIGN KEY (AccountID) REFERENCES Account(AccountID)
    )
    """,
    """
    CREATE TABLE Policy (
        PolicyID INT AUTO_INCREMENT PRIMARY KEY,
        CustSsn INT NOT NULL,
        ContractNum INT NOT NULL,
        StartDate DATE,
        EndDate DATE,
        PremiumAmount INT,
        FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn),
        FOREIGN KEY (ContractNum) REFERENCES Contract(ContractNum)
    )
    """,
    """
    CREATE TABLE HealthMetrics (
        MetricID INT AUTO_INCREMENT PRIMARY KEY,
        CustSsn INT NOT NULL,
        MetricDate DATE,
        Age INT,
        Weight FLOAT,
        Height FLOAT,
        BMI FLOAT,
        SmokingHabit INT,
        DrinkingHabit INT,
        ExerciseLevel INT,
        SleepQuality INT,
        HeartRate INT,
        BloodPressure INT,
        Mental INT DEFAULT NULL,         -- New column for mental health score
        Physical INT DEFAULT NULL,       -- New column for physical health score
        Happiness INT DEFAULT NULL,      -- New column for happiness score
        FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn)
    )
    """,
    """
    CREATE TABLE ChronicDiseaseHistory (
        HistoryID INT AUTO_INCREMENT PRIMARY KEY,
        CustSsn INT NOT NULL,
        DiagnosisDate DATE,
        HasChronicDisease BOOLEAN,
        Severity INT,
        FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn)
    )
    """,
    """
    CREATE TABLE ChronicDiseaseRisk (
        RiskID INT AUTO_INCREMENT PRIMARY KEY,
        CustSsn INT NOT NULL,
        PredictionDate DATE,
        AtRisk BOOLEAN,
        RiskLevel INT,
        ConfidenceScore FLOAT,
        FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn)
    )
    """,
    """
    CREATE TABLE Associate (
        AssoSsn INT AUTO_INCREMENT PRIMARY KEY,
        AssoLastName VARCHAR(50),
        AssoFirstName VARCHAR(50),
        AssoMidInit VARCHAR(1),
        AssoDOB DATE,
        AssoPhone VARCHAR(20)
    )
    """,
    """
    CREATE TABLE Asso_Sell_Service (
        AssoSsn INT NOT NULL,
        AccountID INT NOT NULL,
        ServiceType VARCHAR(50),
        ServiceDate DATE,
        PRIMARY KEY (AssoSsn, AccountID),
        FOREIGN KEY (AssoSsn) REFERENCES Associate(AssoSsn),
        FOREIGN KEY (AccountID) REFERENCES Account(AccountID)
    )
    """,
    """
    CREATE TABLE UserNotes (
    NoteID INT AUTO_INCREMENT PRIMARY KEY,
    CustSsn INT NOT NULL,
    NoteDate DATE NOT NULL,
    QuestionType VARCHAR(50) NOT NULL,  -- Represents the type of question (mental, physical, happiness)
    NoteContent TEXT NOT NULL,          -- Stores the user's response
    FOREIGN KEY (CustSsn) REFERENCES Customer(CustSsn)
    )
    """
]

            for query in table_creation_queries:
                self.cursor.execute(query)
                # print(f"Table created successfully with query: {query.split()[2]}")

        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            self.close_connection()


    def add_indexes(self, db_name):
        """Add indexes to optimize schema."""
        self.connect()
        try:
            self.cursor.execute(f"USE {db_name}")
            index_queries = [
                "CREATE INDEX idx_cust_ssn ON Customer(CustSsn)",
                "CREATE INDEX idx_contract_num ON Contract(ContractNum)",
                "CREATE INDEX idx_health_metrics_date ON HealthMetrics(MetricDate)",
                "CREATE INDEX idx_diagnosis_date ON ChronicDiseaseHistory(DiagnosisDate)"
            ]
            for query in index_queries:
                self.cursor.execute(query)
                # print(f"Index added: {query.split()[2]}")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            self.close_connection()


    def create_materialized_views(self, db_name):
        """Create materialized views for frequent aggregations."""
        self.connect()
        try:
            self.cursor.execute(f"USE {db_name}")
            view_queries = [
                """
                CREATE TABLE CustomerPolicyCounts AS
                SELECT CustSsn, COUNT(*) AS PolicyCount
                FROM Policy
                GROUP BY CustSsn
                """
            ]
            for query in view_queries:
                self.cursor.execute(query)
                # print(f"Materialized view created: {query.split()[2]}")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            self.close_connection()


    def optimize_data_types(self, db_name):
        """Optimize data types for storage efficiency."""
        self.connect()
        try:
            self.cursor.execute(f"USE {db_name}")
            data_type_queries = [
                "ALTER TABLE Customer MODIFY Gender ENUM('M', 'F')",
                "ALTER TABLE ChronicDiseaseHistory MODIFY HasChronicDisease BOOLEAN",
                "ALTER TABLE ChronicDiseaseRisk MODIFY AtRisk BOOLEAN",
                "ALTER TABLE ChronicDiseaseRisk MODIFY ConfidenceScore FLOAT(5, 2)"
            ]
            for query in data_type_queries:
                self.cursor.execute(query)
                # print(f"Data type optimized: {query.split()[2]}")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            self.close_connection()


    def insert_dataframe(self, table_name, dataframe, db_name):
        """Insert data from a pandas DataFrame into a specified table."""
        self.connect(db_name)  # Ensure the correct database is selected
        try:
            # Prepare the insert query dynamically
            columns = ", ".join(dataframe.columns)
            placeholders = ", ".join(["%s"] * len(dataframe.columns))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            # Execute the query
            self.cursor.executemany(query, dataframe.values.tolist())
            self.connection.commit()
            # print(f"Inserted {self.cursor.rowcount} records into {table_name}.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            self.close_connection()
               

    def fetch_health_metrics_with_disease_status(self, db_name):
        """
        Fetch health metrics data with the HasChronicDisease status.
        - db_name: The name of the database.
        Returns: A pandas DataFrame containing health metrics and HasChronicDisease.
        """
        self.connect(db_name)
        try:
            # Join HealthMetrics with ChronicDiseaseHistory
            query = """
            SELECT 
                hm.CustSsn, hm.MetricDate, hm.Age, hm.Weight, hm.Height, 
                hm.BMI, hm.SmokingHabit, hm.DrinkingHabit, hm.ExerciseLevel, 
                hm.SleepQuality, hm.HeartRate, hm.BloodPressure,
                hm.Mental, hm.Physical, hm.Happiness,  -- New features added
                IFNULL(cd.HasChronicDisease, 0) AS HasChronicDisease
            FROM HealthMetrics hm
            LEFT JOIN ChronicDiseaseHistory cd
            ON hm.CustSsn = cd.CustSsn
            """
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            columns = [col[0] for col in self.cursor.description]
            return pd.DataFrame(result, columns=columns)
        except mysql.connector.Error as err:
            print(f"Error fetching health metrics with disease status: {err}")
            return pd.DataFrame()  # Return an empty DataFrame if an error occurs
        finally:
            self.close_connection()



    def fetch_training_data(self, db_name):
        """
        Fetch training data by joining HealthMetrics and ChronicDiseaseHistory.
        Excludes undetermined entries (customers without entries in ChronicDiseaseHistory).
        """
        self.connect(db_name)
        try:
            query = """
            SELECT 
                hm.CustSsn, hm.Age, hm.Weight, hm.Height, hm.BMI, 
                hm.SmokingHabit, hm.DrinkingHabit, hm.ExerciseLevel, 
                hm.SleepQuality, hm.HeartRate, hm.BloodPressure,
                hm.Mental, hm.Physical, hm.Happiness,  -- New features added
                cd.HasChronicDisease
            FROM HealthMetrics hm
            INNER JOIN ChronicDiseaseHistory cd
            ON hm.CustSsn = cd.CustSsn
            WHERE cd.HasChronicDisease IN (1, 0)
            """
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            columns = [col[0] for col in self.cursor.description]
            return pd.DataFrame(result, columns=columns)
        except mysql.connector.Error as err:
            print(f"Error fetching training data: {err}")
            return pd.DataFrame()
        finally:
            self.close_connection()

            

    def fetch_unlabeled_data(self, db_name):
        """
        Fetch data for prediction (unlabeled data).
        """
        self.connect(db_name)
        try:
            query = """
            SELECT 
                CustSsn, Age, BMI, Weight, Height, SmokingHabit, 
                DrinkingHabit, ExerciseLevel, SleepQuality, HeartRate, BloodPressure,
                Mental, Physical, Happiness  -- New features added
            FROM HealthMetrics hm
            WHERE NOT EXISTS (
                SELECT 1
                FROM ChronicDiseaseHistory cd
                WHERE cd.CustSsn = hm.CustSsn
            )
            """
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            columns = [col[0] for col in self.cursor.description]
            return pd.DataFrame(result, columns=columns)
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return pd.DataFrame()
        finally:
            self.close_connection()



    def insert_clustering_results(self, data, db_name):
        """Insert prediction results into the database."""
        self.connect(db_name)
        try:
            query = """
            INSERT INTO ChronicDiseaseRisk (CustSsn, PredictionDate, AtRisk, RiskLevel, ConfidenceScore)
            VALUES (%s, CURDATE(), %s, %s, %s)
            """
            insert_data = [
                (row['CustSsn'], row['AtRisk'], row['RiskLevel'], row['ConfidenceScore'])
                for _, row in data.iterrows()
            ]
            self.cursor.executemany(query, insert_data)
            self.connection.commit()
            # print(f"Inserted {self.cursor.rowcount} records into ChronicDiseaseRisk.")
        except mysql.connector.Error as err:
            print(f"Error: {err}")
        finally:
            self.close_connection()


    def get_random_undetermined_client(self, db_name):
        """
        Helper function: Get a random client with undetermined chronic disease status.
        Returns the CustSsn of the random client. (adjust when necessary)
        """
        self.connect(db_name)
        try:
            # Fetch undetermined clients
            query_client = """
            SELECT hm.CustSsn, c.CustFirstName, c.CustLastName
            FROM HealthMetrics hm
            LEFT JOIN ChronicDiseaseHistory cd ON hm.CustSsn = cd.CustSsn
            JOIN Customer c ON hm.CustSsn = c.CustSsn
            WHERE cd.CustSsn IS NULL
            """
            self.cursor.execute(query_client)
            clients = self.cursor.fetchall()
    
            if not clients:
                print("No undetermined clients available.")
                return None
    
            # Choose a random client
            random_client = random.choice(clients)
            cust_ssn, first_name, last_name = random_client
            
            return cust_ssn  # Return only the CustSsn
        
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None
        finally:
            self.close_connection()


    def recommend_products(self, client_ssn, db_name):
        """
        Recommend a product for the given client based on their risk level.
        - client_ssn: The SSN of the client.
        """
        self.connect(db_name)
        try:
            # Fetch the risk level for the client
            query_risk = f"""
            SELECT RiskLevel
            FROM ChronicDiseaseRisk
            WHERE CustSsn = {client_ssn}
            """
            self.cursor.execute(query_risk)
            risk_level_data = self.cursor.fetchone()
    
            if not risk_level_data:
                print(f"No risk level found for client {client_ssn}.")
                return None
    
            risk_level = risk_level_data[0]
    
            # Map risk level (0-100) to 0-5
            mapped_risk_level = risk_level // (100/6)  
    
            # Fetch product based on mapped risk level
            query_product = f"""
            SELECT SeriesName, PlanName
            FROM Product
            WHERE RiskLevel = {mapped_risk_level}
            LIMIT 1
            """
            self.cursor.execute(query_product)
            product = self.cursor.fetchone()
    
            if not product:
                print(f"No products available for mapped risk level {mapped_risk_level}.")
                return None
    
            # Fetch the customer's name
            query_customer = f"""
            SELECT CustFirstName, CustLastName
            FROM Customer
            WHERE CustSsn = {client_ssn}
            """
            self.cursor.execute(query_customer)
            customer_name = self.cursor.fetchone()
    
            if not customer_name:
                print(f"No customer found with SSN {client_ssn}.")
                return None
    
            # Format the output
            full_name = f"{customer_name[0]} {customer_name[1]}"
            series_name, plan_name = product
            recommendation_message = f"\nHi {full_name}. Thanks for your patience! The recommendation for you is the product '{series_name}' under the plan '{plan_name}'."
    
            print(recommendation_message)
            return recommendation_message
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            return None
        finally:
            self.close_connection()
            
    
    def run_custom_query(self, query, db_name):
        """
        Execute a custom query and return the results.
        Args:
            query (str): The SQL query to execute.
            db_name (str): The name of the database.
        Returns:
            list: The fetched results of the query.
        """
        self.connect(db_name)
        try:
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            return results
        except mysql.connector.Error as err:
            print(f"Error executing custom query: {err}")
            return []
        finally:
            self.close_connection()
    
    def update_health_metrics(self, cust_ssn, mental, physical, happiness, db_name):
        """
        Update the health metrics (mental, physical, happiness) for a specific customer.
        Args:
            cust_ssn (int): Customer SSN.
            mental (int): Mental health score.
            physical (int): Physical health score.
            happiness (int): Happiness score.
            db_name (str): Database name.
        """
        self.connect(db_name)
        try:
            query = f"""
                UPDATE HealthMetrics
                SET Mental = %s,
                    Physical = %s,
                    Happiness = %s
                WHERE CustSsn = %s
            """
            self.cursor.execute(query, (mental, physical, happiness, cust_ssn))
            self.connection.commit()
            # print(f"Health metrics updated for customer {cust_ssn}.")
        except mysql.connector.Error as err:
            print(f"Error updating health metrics: {err}")
        finally:
            self.close_connection()
            
            
    def update_clustering_results(self, data, db_name):
        """
        Update prediction results in the database.
        Args:
            data (DataFrame): DataFrame containing the predictions with CustSsn as the key.
            db_name (str): Database name.
        """
        self.connect(db_name)
        try:
            query = """
            UPDATE ChronicDiseaseRisk
            SET 
                PredictionDate = CURDATE(),
                AtRisk = %s,
                RiskLevel = %s,
                ConfidenceScore = %s
            WHERE CustSsn = %s
            """
            update_data = [
                (row['AtRisk'], row['RiskLevel'], row['ConfidenceScore'], row['CustSsn'])
                for _, row in data.iterrows()
            ]
            self.cursor.executemany(query, update_data)
            self.connection.commit()
            # print(f"Updated {self.cursor.rowcount} records in ChronicDiseaseRisk.")
        except mysql.connector.Error as err:
            print(f"Error updating clustering results: {err}")
        finally:
            self.close_connection()
            
    
    def fetch_unlabeled_data_for_user(self, db_name, cust_ssn):
        """
        Fetch data for prediction for a specific user.
        Args:
            db_name (str): Database name.
            cust_ssn (int): Customer SSN to filter.
        Returns:
            DataFrame: DataFrame containing health metrics for the specified user.
        """
        self.connect(db_name)
        try:
            query = f"""
            SELECT 
                CustSsn, Age, BMI, Weight, Height, SmokingHabit, 
                DrinkingHabit, ExerciseLevel, SleepQuality, HeartRate, BloodPressure,
                Mental, Physical, Happiness
            FROM HealthMetrics
            WHERE CustSsn = {cust_ssn}
            """
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            columns = [col[0] for col in self.cursor.description]
            return pd.DataFrame(result, columns=columns)
        except mysql.connector.Error as err:
            print(f"Error fetching data for user {cust_ssn}: {err}")
            return pd.DataFrame()
        finally:
            self.close_connection()


        
    def close_connection(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
