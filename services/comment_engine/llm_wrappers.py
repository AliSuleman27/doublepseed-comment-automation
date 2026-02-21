"""LLM Wrappers — Claude Haiku, GPT-4o-mini, Gemini Flash.
Each wrapper takes system + user prompt and returns raw text.
Includes retry with backoff for rate limit (429) errors."""

import os
import json
import time


MAX_RETRIES = 3
BASE_DELAY = 4  # seconds


def call_llm(model: str, system_prompt: str, user_prompt: str, temperature: float = 0.9) -> str:
    """Route to the correct LLM provider and return raw response text.
    Retries up to MAX_RETRIES times on rate limit errors with exponential backoff.
    """
    if model == "claude-haiku":
        fn = _call_anthropic
    elif model == "gpt-4o-mini":
        fn = _call_openai
    elif model == "gemini-flash":
        fn = _call_gemini
    else:
        raise ValueError(f"Unknown model: {model}. Use 'claude-haiku', 'gpt-4o-mini', or 'gemini-flash'.")

    last_err = None
    for attempt in range(MAX_RETRIES):
        try:
            return fn(system_prompt, user_prompt, temperature)
        except Exception as e:
            err_str = str(e).lower()
            is_rate_limit = any(k in err_str for k in ["429", "rate_limit", "resource_exhausted", "too many requests"])
            if is_rate_limit and attempt < MAX_RETRIES - 1:
                delay = BASE_DELAY * (2 ** attempt)
                print(f"[CE] Rate limited ({model}), retrying in {delay}s (attempt {attempt + 1}/{MAX_RETRIES})...")
                time.sleep(delay)
                last_err = e
            else:
                raise

    raise last_err


def _call_anthropic(system_prompt: str, user_prompt: str, temperature: float) -> str:
    """Call Claude Haiku via Anthropic SDK."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set in .env")

    from anthropic import Anthropic
    client = Anthropic(api_key=api_key)

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=1024,
        temperature=temperature,
        system=system_prompt,
        messages=[{"role": "user", "content": user_prompt}],
    )

    return response.content[0].text


def _call_openai(system_prompt: str, user_prompt: str, temperature: float) -> str:
    """Call GPT-4o-mini via OpenAI SDK."""
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY not set in .env")

    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        temperature=temperature,
        max_tokens=1024,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )

    return response.choices[0].message.content


def _call_gemini(system_prompt: str, user_prompt: str, temperature: float) -> str:
    """Call Gemini Flash via Google GenAI SDK."""
    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set in .env")

    from google import genai
    from google.genai import types

    client = genai.Client(api_key=api_key)

    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=user_prompt,
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=temperature,
            max_output_tokens=1024,
        ),
    )

    return response.text


def parse_llm_response(raw: str) -> list[dict]:
    """Parse LLM response text into a list of comment dicts.

    Handles: raw JSON, markdown-wrapped JSON, partial JSON.

    Returns:
        List of {"post_index": int, "comment": str}
    """
    text = raw.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        lines = text.split("\n")
        lines = [l for l in lines if not l.strip().startswith("```")]
        text = "\n".join(lines).strip()

    # Try direct JSON parse
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            return parsed
    except json.JSONDecodeError:
        pass

    # Try to find JSON array in the text
    start = text.find("[")
    end = text.rfind("]")
    if start != -1 and end != -1 and end > start:
        try:
            parsed = json.loads(text[start:end + 1])
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Could not parse LLM response as JSON array: {text[:200]}")
