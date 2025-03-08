# Reading/ Saving local files
def save_cleaning_code_to_file(cleaning_code, filename):
    """Save cleaning code to file for audit."""
    code_filename = filename.replace('.csv', '_cleaning_code.txt')
    with open(code_filename, 'w', encoding='utf-8') as file:
        file.write(cleaning_code)
    print(f"Saved cleaning code to {code_filename}")
