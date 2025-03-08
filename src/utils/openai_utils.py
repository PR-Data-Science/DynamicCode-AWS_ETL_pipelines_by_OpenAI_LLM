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
