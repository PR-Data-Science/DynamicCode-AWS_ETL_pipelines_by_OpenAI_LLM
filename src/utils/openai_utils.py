import openai
from config.config import OPENAI_API_KEY

openai.api_key = OPENAI_API_KEY

def call_openai_with_system_user_prompt(system_prompt, user_prompt):
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        if response.choices and len(response.choices) > 0:
            return response.choices[0].message.content
        else:
            raise ValueError("No valid response received from OpenAI.")
    except Exception as e:
        raise RuntimeError(f"OpenAI API call failed: {e}")
    

def clean_openai_code_response(response_text):
    """Remove markdown-like ```python and ``` from LLM code blocks."""

    if response_text is None:
        raise ValueError("Received no cleaning code from OpenAI.")
    
    if response_text.startswith("```"):
        response_text = response_text.split("\n", 1)[1]
    if response_text.endswith("```"):
        response_text = response_text.rsplit("\n", 1)[0]
    return response_text
