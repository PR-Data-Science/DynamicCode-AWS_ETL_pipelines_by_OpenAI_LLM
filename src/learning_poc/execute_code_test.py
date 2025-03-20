import pandas as pd

def execute_dynamic_code(df, code_str):
    """
    Executes the given Python code string dynamically on the DataFrame.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    code_str (str): The Python code to be executed.

    Returns:
    tuple: (Updated DataFrame, Message)
    """
    try:
        # Create a dictionary to hold variables for exec execution
        local_vars = {"df": df, "pd": pd}
        
        # Execute the dynamic code
        exec(code_str, {}, local_vars)
        
        # Ensure the function is defined
        if "clean_dataframe" not in local_vars:
            raise ValueError("Error: The executed code did not define 'clean_dataframe'.")

        # Call the function dynamically
        df, message = local_vars["clean_dataframe"](df)

        return df, message  # Returning both values
    
    except Exception as e:
        return None, f"❌ Error executing code: {e}"

# Example DataFrame
df = pd.DataFrame({
    "Name": [" Alice ", "Bob", " Charlie"],
    "Age": [25, None, 30],
    "Salary": [50000, 60000, None]
})

# Example Python code string with two return values
code_string = """
def clean_dataframe(df):
    df['Name'] = df['Name'].str.strip()
    df.fillna({'Age': df['Age'].median(), 'Salary': df['Salary'].median()}, inplace=True)
    
    message = f"✅ Cleaning completed. Rows: {df.shape[0]}, Columns: {df.shape[1]}"
    return df, message
"""

# Execute the dynamic function on DataFrame
updated_df, msg = execute_dynamic_code(df, code_string)

# Print the message and output DataFrame
print(msg)  # ✅ Cleaning completed. Rows: 3, Columns: 3
print(updated_df)

# Save output DataFrame to CSV
updated_df.to_csv("updated_output.csv", index=False)
