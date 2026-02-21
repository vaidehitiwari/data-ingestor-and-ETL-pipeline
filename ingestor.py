import pandas as pd
import psycopg2
from psycopg2 import extras
from mask import mask_dataframe
from getpass import getpass
from datetime import datetime

# Define the schema outside functions for easy access
SCHEMA = {
    'customers': ['customer_id', 'full_name', 'aadhaar_number', 'email', 'phone_number', 'address', 'date_of_birth', 'gender'],
    'loans': ['loan_id', 'customer_id', 'loan_amount', 'interest_rate', 'term_months', 'loan_type', 'approval_date'],
}

def connect():
    """Establishes a database connection with hardcoded credentials."""
    try:
        conn = psycopg2.connect(
            host='localhost',
            dbname='fintech',
            user='postgres',
            # WARNING: Hardcoding passwords is a security risk. Use environment variables in a production environment.
            password='080804',
            port='5432'
        )
        return conn
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        return None

def login(cursor):
    """Handles user authentication with improved error handling."""
    username = input("Enter username: ")
    password = getpass("Enter password: ")
    
    try:
        cursor.execute("SELECT role FROM users WHERE username = %s AND password = %s", (username, password))
        result = cursor.fetchone()
        
        if result:
            role = result[0]
            allowed_roles = ['data_engineer', 'senior_dev']
            if role in allowed_roles:
                print(f"Login successful. Role: {role}")
                return username, role
            else:
                print("Unauthorized access. Your role does not have the necessary permissions.")
                return None, None
        else:
            print("Invalid username or password.")
            return None, None
    except psycopg2.Error as e:
        print(f"Authentication error: {e}")
        return None, None

def validate_columns(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """Validates and selects the correct columns based on the predefined SCHEMA."""
    expected_columns = SCHEMA.get(table)
    if not expected_columns:
        print(f"No schema defined for table {table}.")
        return None

    available_columns = [col for col in expected_columns if col in df.columns]

    if not available_columns:
        print(f"No valid columns found for table {table}.")
        return None 
    
    missing = set(expected_columns) - set(available_columns)
    if missing:
        print(f"Warning: Missing columns for table {table}: {', '.join(missing)}")
    
    return df[available_columns]

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Performs data cleaning and imputation."""
    df = df.copy()
    df.dropna(how='all', inplace=True)
    df.dropna(axis=1, how='all', inplace=True)
    df.drop_duplicates(inplace=True)

    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]):
            df[col] = df[col].astype(str).str.strip()
            df[col].replace('', 'unknown', inplace=True)
            df[col].fillna('unknown', inplace=True)

        elif pd.api.types.is_numeric_dtype(df[col]):
            df[col].fillna(df[col].median(), inplace=True)
        
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col].fillna(pd.Timestamp('1900-01-01'), inplace=True)

    return df

def insert_data_in_bulk(cursor, df: pd.DataFrame, table_name: str, page_size: int = 1000):
    """
    Inserts a pandas DataFrame into a PostgreSQL table using bulk insertion.
    
    Args:
        cursor: The database cursor.
        df (pd.DataFrame): The DataFrame to insert.
        table_name (str): The name of the target table.
        page_size (int): The number of rows to insert in each batch.
    """
    if df.empty:
        print(f"DataFrame is empty, skipping insertion for {table_name}.")
        return

    columns = ','.join(df.columns)
    placeholders = ','.join(['%s'] * len(df.columns))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    data_tuples = [tuple(row) for row in df.itertuples(index=False)]
    
    try:
        extras.execute_batch(cursor, insert_query, data_tuples, page_size=page_size)
        print(f"Successfully inserted {len(df)} records into {table_name}.")
    except psycopg2.Error as e:
        print(f"Bulk insertion failed for {table_name}: {e}")
        raise

def insert_cleaned(cursor, df: pd.DataFrame, table: str):
    target_table = f"{table}_raw"
    insert_data_in_bulk(cursor, df, target_table)


def insert_masked(cursor, df: pd.DataFrame, table: str):
    target_table = f"{table}_masked"
    insert_data_in_bulk(cursor, df, target_table)

def log(cursor, username: str, action: str, table: str):
    """Logs an action to the access_logs table."""
    table_name = f"{table}_raw" if 'Clean' in action else f"{table}_masked" if 'Mask' in action else (table or 'system')
    query = """
        INSERT INTO access_logs (username, action, table_name, timestamp)
        VALUES (%s, %s, %s, %s)
    """
    try:
        cursor.execute(query, (username, action, table_name, datetime.now()))
        print(f"Logged action: {action} on '{table_name}' by {username}")
    except psycopg2.Error as e:
        print(f"Failed to log action: {e}")
        raise

def main():
    """Main function to run the data pipeline."""
    conn = connect()
    if not conn:
        print("Exiting.")
        return

    try:
        curr = conn.cursor()
        username, role = login(curr)

        if not username:
            print("Exiting.")
            return

        log(curr, username, "Login", None)
        conn.commit()

        # --- Example data simulation (replace with your actual data loading) ---
        data = {
            'customer_id': [1, 2, 3],
            'full_name': ['John Doe', 'Jane Smith', 'Ram Kumar'],
            'aadhaar_number': ['1234-5678-9012', '9876-5432-1098', '1111-2222-3333'],
            'email': ['john.doe@example.com', 'jane.smith@example.com', 'ram.kumar@example.com'],
            'phone_number': ['9876543210', '9876543211', '8765432109'],
            'address': ['123 Main St', '456 Side St', '789 High St'],
            'date_of_birth': ['1990-01-01', '1995-05-05', '1985-11-20'],
            'gender': ['Male', 'Female', 'Male']
        }
        df = pd.DataFrame(data)
        # --- End of example data ---

        table = 'customers'

        # Step 1: Validate and Clean Data
        print("\n--- Step 1: Cleaning and inserting raw data ---")
        validated_df = validate_columns(df, table)
        if validated_df is None or validated_df.empty:
            print("Validation failed or DataFrame is empty. Skipping insertion.")
            return
            
        cleaned_df = clean(validated_df)
        insert_cleaned(curr, cleaned_df, table)
        log(curr, username, "Clean and Insert Raw Data", table)

        # Step 2: Mask Data and Insert
        # Mask the validated data
        masked_df = mask_dataframe(validated_df)

        # Insert masked version of cleaned + validated data
        insert_masked(curr, masked_df, table)

        # Log action
        log(curr, username, "Mask and Insert Masked Data", table)

        conn.commit()
        print("\nPipeline completed successfully.")

    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        conn.rollback() # Rollback all changes if any error occurs
    finally:
        if 'conn' in locals() and conn:
            curr.close()
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()