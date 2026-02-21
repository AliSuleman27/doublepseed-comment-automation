"""Validator + Dedup — Deterministic, no LLM.
Validates comments against rules and deduplicates within batches.
Enhanced with: structural quality checks, banned pattern detection,
opener diversity enforcement, structural skeleton dedup."""

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
    word_min, word_max = rules.get("word_count_range", [6, 18])
    banned_words = brand_config.get("global_banned_words", [])
    brand_name = brand_config.get("name", "")
    brand_variations = brand_config.get("name_variations", [brand_name])
    banned_patterns = config.get("banned_patterns", [])

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
        # Use wider tolerance before hard failing (5-25 is the outer bound)
        if word_count < 5 or word_count > 25:
            hard_fail = True

    # Ad language — use word-boundary matching for short phrases
    text_lower = text.lower()
    found_ad = None
    for phrase in banned_words:
        p = phrase.lower().strip()
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

    # Banned patterns (template-specific)
    found_banned = None
    for bp in banned_patterns:
        if bp.lower() in text_lower:
            found_banned = bp
            break
    checks.append({
        "label": "No banned patterns",
        "status": "fail" if found_banned else "pass",
    })
    if found_banned:
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

    # ─── Structural quality checks (NEW) ──────────────────────
    # Mid-sentence period (two sentences glued together)
    mid_period = _has_mid_sentence_period(text)
    checks.append({
        "label": "No mid-sentence period",
        "status": "fail" if mid_period else "pass",
    })
    if mid_period:
        hard_fail = True

    # Compound "and" clause detection
    compound_and = _has_compound_and(text)
    checks.append({
        "label": "No compound 'and'",
        "status": "fail" if compound_and else "pass",
    })
    if compound_and:
        hard_fail = True

    valid = not hard_fail
    return {"text": text, "valid": valid, "checks": checks, "hard_fail": hard_fail}


def _has_mid_sentence_period(text: str) -> bool:
    """Detect period in the middle of a comment creating two sentences.
    Allows periods in abbreviations and numbers."""
    # Remove common abbreviations that contain periods
    cleaned = text
    for abbr in ["mr.", "mrs.", "dr.", "etc.", "vs.", "st.", "ft."]:
        cleaned = cleaned.lower().replace(abbr, abbr.replace(".", ""))

    # Check for period followed by space and a capital letter or word
    if re.search(r'\.\s+[A-Z]', cleaned):
        return True
    # Check for period followed by space and lowercase word (run-on)
    if re.search(r'\.\s+[a-z]', cleaned):
        # But not if it's at the very end (trailing period already stripped)
        parts = cleaned.split('. ')
        if len(parts) >= 2 and len(parts[-1].split()) >= 2:
            return True
    return False


def _has_compound_and(text: str) -> bool:
    """Detect compound sentences joined by 'and' with two independent clauses.
    e.g. 'ClickUp gave me my mornings back and my creativity feels so good'
    but NOT 'templates workflows all in ClickUp' (no 'and' compound)."""
    # Look for pattern: [subject+verb] and [subject+verb]
    # Simple heuristic: "and" preceded by 3+ words AND followed by possessive/pronoun + verb-like word
    text_lower = text.lower()

    # Split on ' and '
    parts = text_lower.split(' and ')
    if len(parts) < 2:
        return False

    # Check if both sides look like independent clauses (have a verb-like structure)
    # Heuristic: left side has 4+ words, right side has 3+ words with a pronoun/noun start
    left = parts[0].strip()
    right = ' and '.join(parts[1:]).strip()

    left_words = left.split()
    right_words = right.split()

    if len(left_words) < 4 or len(right_words) < 3:
        return False

    # Right side starts with pronoun/possessive/noun suggesting independent clause
    clause_starters = [
        "my", "i", "it", "they", "he", "she", "we", "you", "the", "this",
        "that", "his", "her", "our", "your", "its", "no", "every"
    ]
    if right_words[0] in clause_starters:
        return True

    return False


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


def structural_dedup_batch(comments: list[dict], max_per_skeleton: int = 2) -> list[dict]:
    """Detect structural duplicates — same sentence skeleton with different slot values.
    Extracts skeleton by replacing capitalized words and quoted content with slots.

    Args:
        comments: List of {"post_id": ..., "text": ..., ...}
        max_per_skeleton: Max comments allowed per structural skeleton.

    Returns:
        Updated comments with "is_structural_dup" flag.
    """
    skeletons = {}

    for c in comments:
        c["is_structural_dup"] = False
        skel = _extract_skeleton(c["text"])
        c["_skeleton"] = skel

        if skel not in skeletons:
            skeletons[skel] = []
        skeletons[skel].append(c)

    # Mark excess as structural dups
    for skel, group in skeletons.items():
        if len(group) > max_per_skeleton:
            for c in group[max_per_skeleton:]:
                c["is_structural_dup"] = True

    return comments


def check_opener_diversity(comments: list[dict], max_same_opener: int = 1) -> list[dict]:
    """Check that no two comments in a batch start with the same opener.

    Args:
        comments: List of comment dicts with "text" field
        max_same_opener: Max comments allowed with same 2-word opener

    Returns:
        Updated comments with "duplicate_opener" flag.
    """
    openers = {}
    for c in comments:
        c["duplicate_opener"] = False
        words = c["text"].lower().split()
        if len(words) >= 2:
            opener = f"{words[0]} {words[1]}"
        elif words:
            opener = words[0]
        else:
            continue

        if opener not in openers:
            openers[opener] = []
        openers[opener].append(c)

    for opener, group in openers.items():
        if len(group) > max_same_opener:
            for c in group[max_same_opener:]:
                c["duplicate_opener"] = True

    return comments


def _extract_skeleton(text: str) -> str:
    """Extract structural skeleton by replacing specific nouns with [SLOT].
    'sending this to every auditor I know with a clickup link' →
    'sending this to every [SLOT] I know with a [BRAND] link'"""
    words = text.lower().split()
    skeleton = []
    skip_next = False

    for i, word in enumerate(words):
        if skip_next:
            skip_next = False
            continue

        # Replace brand mentions with [BRAND]
        if word in ("clickup", "click", "notion", "asana", "slack"):
            skeleton.append("[BRAND]")
            continue

        # Replace proper-noun-like words (only works for specific patterns)
        # Replace words that look like professions/roles/people
        if i > 0 and words[i-1] in ("every", "my", "all"):
            skeleton.append("[SLOT]")
            continue

        # Replace time references
        if re.match(r'\d+[ap]m', word) or word in ("1am", "2am", "3am", "4am", "6pm"):
            skeleton.append("[TIME]")
            continue

        skeleton.append(word)

    return " ".join(skeleton)


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
