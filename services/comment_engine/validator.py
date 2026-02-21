"""Validator + Dedup — Deterministic, no LLM.
Validates comments against rules and deduplicates within batches."""

import re


def validate_comment(comment_text: str, config: dict, brand_config: dict,
                     brand_mention_expected: bool) -> dict:
    """Validate a single comment against all rules.

    Returns:
        {
            "text": str (cleaned text),
            "valid": bool,
            "checks": [{"label": str, "status": "pass"|"fail"|"warn"}],
            "hard_fail": bool,
        }
    """
    rules = config.get("comment_rules", {})
    word_min, word_max = rules.get("word_count_range", [8, 18])
    banned_words = brand_config.get("global_banned_words", [])
    brand_name = brand_config.get("name", "")
    brand_variations = brand_config.get("name_variations", [brand_name])

    text = comment_text.strip()
    checks = []
    hard_fail = False

    # ─── Auto-fixes (soft) ──────────────────────
    # Strip emoji
    had_emoji = bool(re.search(r'[\U0001f600-\U0001f9ff\U0001fa00-\U0001faff\u2600-\u27bf\ufe0f]', text))
    text = re.sub(r'[\U0001f600-\U0001f9ff\U0001fa00-\U0001faff\u2600-\u27bf\ufe0f]', '', text).strip()
    checks.append({"label": "No emoji", "status": "warn" if had_emoji else "pass"})

    # Strip hashtags
    had_hashtag = bool(re.search(r'#\w+', text))
    text = re.sub(r'#\w+', '', text).strip()
    checks.append({"label": "No hashtags", "status": "warn" if had_hashtag else "pass"})

    # Strip trailing period
    if text.endswith('.'):
        text = text[:-1].rstrip()

    # Clean up extra spaces
    text = re.sub(r'\s{2,}', ' ', text).strip()

    # ─── Hard checks ──────────────────────
    # Word count
    word_count = len(text.split())
    wc_pass = word_min <= word_count <= word_max
    checks.append({
        "label": f"{word_count} words",
        "status": "pass" if wc_pass else "fail",
    })
    if not wc_pass:
        # Use wider tolerance before hard failing (6-25 is the outer bound)
        if word_count < 6 or word_count > 25:
            hard_fail = True

    # Ad language — use word-boundary matching for short phrases
    text_lower = text.lower()
    found_ad = None
    for phrase in banned_words:
        p = phrase.lower().strip()
        # Use regex word boundaries to avoid false positives (e.g. "ad " inside "head ")
        pattern = r'\b' + re.escape(p.rstrip()) + r'\b'
        if re.search(pattern, text_lower):
            found_ad = phrase
            break
    checks.append({
        "label": "No ad language",
        "status": "fail" if found_ad else "pass",
    })
    if found_ad:
        hard_fail = True

    # Brand mention check
    brand_present = any(v.lower() in text_lower for v in brand_variations)
    if brand_mention_expected and not brand_present:
        checks.append({"label": "Brand mentioned", "status": "warn"})
    elif not brand_mention_expected and brand_present:
        checks.append({"label": "No brand (mystery)", "status": "warn"})
    else:
        expected_label = "Brand mentioned" if brand_mention_expected else "No brand (mystery)"
        checks.append({"label": expected_label, "status": "pass"})

    # Excessive caps
    caps_words = [w for w in text.split() if w.isupper() and len(w) >= 3]
    if len(caps_words) > 3:
        checks.append({"label": "Excessive caps", "status": "warn"})

    valid = not hard_fail
    return {"text": text, "valid": valid, "checks": checks, "hard_fail": hard_fail}


def dedup_batch(comments: list[dict], threshold: float = 0.5) -> list[dict]:
    """Check for duplicate comments in a batch using Jaccard bigram similarity.

    Args:
        comments: List of {"post_id": ..., "text": ..., ...}
        threshold: Similarity threshold (0.0-1.0). Above this = duplicate.

    Returns:
        Updated comments list with "is_duplicate" flag added.
    """
    for i, c in enumerate(comments):
        c["is_duplicate"] = False

    for i in range(len(comments)):
        if comments[i].get("is_duplicate"):
            continue
        bigrams_i = _get_bigrams(comments[i]["text"])

        for j in range(i + 1, len(comments)):
            if comments[j].get("is_duplicate"):
                continue
            bigrams_j = _get_bigrams(comments[j]["text"])
            sim = _jaccard(bigrams_i, bigrams_j)

            if sim > threshold:
                comments[j]["is_duplicate"] = True
                comments[j]["dup_similarity"] = round(sim, 3)

    return comments


def _get_bigrams(text: str) -> set:
    """Get set of word bigrams from text."""
    words = text.lower().split()
    if len(words) < 2:
        return set(words)
    return {(words[i], words[i + 1]) for i in range(len(words) - 1)}


def _jaccard(set_a: set, set_b: set) -> float:
    """Jaccard similarity between two sets."""
    if not set_a and not set_b:
        return 0.0
    intersection = set_a & set_b
    union = set_a | set_b
    return len(intersection) / len(union) if union else 0.0
