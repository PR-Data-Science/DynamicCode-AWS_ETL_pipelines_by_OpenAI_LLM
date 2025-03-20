import os

def save_cleaning_code_to_file(cleaning_code, filename):
    """Save cleaning code to a .py file inside `src/llm_generated_codes/`."""
    
    # Define the new folder path
    save_directory = "src/llm_generated_codes"
    
    # Ensure the directory exists
    os.makedirs(save_directory, exist_ok=True)

    # Generate the new file name (convert CSV name to Python file)
    code_filename = filename.replace('.csv', '_cleaning_code.py')
    full_path = os.path.join(save_directory, code_filename)

    # Write the generated code to the Python file
    with open(full_path, 'w', encoding='utf-8') as file:
        file.write(cleaning_code)
    
    print(f"✅ Saved LLM-generated cleaning code to {full_path}")


def save_verification_code_to_file(cleaning_code, filename):
    """Save verification code to a .py file inside `src/llm_generated_codes/`."""
    
    # Define the new folder path
    save_directory = "src/llm_generated_codes"
    
    # Ensure the directory exists
    os.makedirs(save_directory, exist_ok=True)

    full_path = os.path.join(save_directory, filename)

    # Write the generated code to the Python file
    with open(full_path, 'w', encoding='utf-8') as file:
        file.write(cleaning_code)
    
    print(f"✅ Saved LLM-generated cleaning code to {full_path}")
