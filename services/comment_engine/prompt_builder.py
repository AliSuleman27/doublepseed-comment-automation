"""Prompt Builder — Deterministic, no LLM.
Constructs system + user prompts from brand config and post data.
Enhanced with: anti-patterns, golden comments, relevance tagging,
brand casing control, structural diversity rules."""

import json
import random

MAX_SLIDES = 5       # Max slides to include per post (skip the rest)
MAX_SLIDE_CHARS = 120  # Truncate individual slide text if too long


def build_system_prompt(brand_config: dict, template_config: dict,
                        overrides: dict | None = None) -> str:
    """Build the system prompt (sent once per batch).

    Contains: role, brand, persona, golden examples, anti-patterns,
    rules, archetype defs, structural constraints.

    Args:
        brand_config: Brand section of config
        template_config: Template section of config
        overrides: Optional UI overrides (temperature, brand_casing, etc.)
    """
    overrides = overrides or {}

    brand_name = brand_config.get("name", "the brand")
    theme = template_config.get("theme_story", "")
    persona = template_config.get("commenting_persona", "")
    golden = template_config.get("golden_comments", [])
    anti_examples = template_config.get("anti_examples", [])
    rules = template_config.get("comment_rules", {})
    archetypes = template_config.get("archetype_guidance", {})
    banned_patterns = template_config.get("banned_patterns", [])

    default_min, default_max = rules.get("word_count_range", [6, 18])
    word_min = overrides.get("word_count_min", default_min)
    word_max = overrides.get("word_count_max", default_max)
    allowed_slang = ", ".join(rules.get("allowed_slang", []))
    slang_freq = overrides.get("slang_frequency", rules.get("slang_frequency", "natural"))

    # Brand casing preference
    preferred_casing = overrides.get("brand_casing") or brand_config.get("preferred_casing", {})
    if preferred_casing:
        casing_lines = []
        for form, weight in preferred_casing.items():
            pct = int(weight * 100)
            casing_lines.append(f'  - "{form}" (~{pct}% of the time)')
        casing_section = "BRAND NAME CASING — use these forms:\n" + "\n".join(casing_lines)
        casing_section += f'\n  NEVER use "{brand_name}" with camelCase (e.g. "ClickUp") unless it appears in the casing list above.'
    else:
        casing_section = f"Brand name: {brand_name}"

    # Archetype definitions (descriptions only, no literal patterns)
    arch_defs = ""
    for arch_type, guidance in archetypes.items():
        desc = guidance.get("description", "")
        style = guidance.get("style_notes", "")
        arch_defs += f"\n  {arch_type}:\n    What: {desc}\n    Style: {style}\n"

    # Golden comments section
    if golden:
        golden_str = "\n".join(f'  - "{c}"' for c in golden)
        golden_section = f"""GOLDEN COMMENTS — these are REAL approved comments written by a human reviewer.
Match this quality, voice, and naturalness. Study the rhythm, word choice, and structure:
{golden_str}"""
    else:
        golden_section = ""

    # Anti-examples section
    if anti_examples:
        anti_str = "\n".join(f'  - BAD: "{a["text"]}"\n    WHY: {a["reason"]}' for a in anti_examples)
        anti_section = f"""ANTI-PATTERNS — NEVER produce comments like these:
{anti_str}"""
    else:
        anti_section = ""

    # Banned patterns
    if banned_patterns:
        banned_str = ", ".join(f'"{p}"' for p in banned_patterns)
        banned_section = f"BANNED PHRASES — never use any of these in a comment: {banned_str}"
    else:
        banned_section = ""

    # Slang guidance
    slang_guide = {
        "none": "Do NOT use any slang.",
        "light": f"Use slang sparingly — at most 1 slang term per 3 comments. Allowed: {allowed_slang}",
        "natural": f"Use slang where it fits naturally — like a real person texting. Allowed: {allowed_slang}",
        "heavy": f"Use slang freely and often. Allowed: {allowed_slang}",
    }.get(slang_freq, f"Use slang naturally. Allowed: {allowed_slang}")

    return f"""You are a TikTok comment writer for {brand_name}.

{casing_section}

THEME STORY:
{theme}

COMMENTING PERSONA:
{persona}

{golden_section}

{anti_section}

{banned_section}

STRUCTURAL RULES — these are HARD requirements:
1. Word count: {word_min}-{word_max} words per comment. Short is better than long.
2. ONE thought per comment. Never two sentences. Never a period in the middle of a comment.
3. Never use compound sentences with "and" joining two independent clauses (e.g. "X gave me Y and Z noticed" is BAD).
4. Follow the assigned archetype as INSPIRATION, not as a template. Never copy any pattern word-for-word.
5. No emojis, no hashtags, no trailing periods.
6. {slang_guide}
7. No ad language: never say "check out", "link in bio", "game changer", "must have", "sign up", "highly recommend"
8. When brand instruction says "Mention brand" — include the brand name naturally woven in. When it says "No brand" — leave it out entirely.
9. EVERY comment in the batch must have a COMPLETELY DIFFERENT sentence structure — different opener, different rhythm, different length. If one comment starts with "downloading...", NO other comment in the batch can start similarly.
10. Output ONLY a JSON array. No markdown, no explanation, no code fences.

RELEVANCE RULE:
Some posts are tagged "SPECIFIC" — reference a detail from that post's content (a role, tip, or theme).
Some posts are tagged "VIBE" — write a general reaction that fits the post's topic area without quoting specific content. Vibe comments are looser, more generic, like reacting to the general energy.

ARCHETYPE DEFINITIONS:
{arch_defs}

OUTPUT FORMAT:
Return a JSON array exactly like this:
[{{"post_index": 1, "comment": "your comment here"}}, {{"post_index": 2, "comment": "..."}}]
"""


def build_user_prompt(posts_batch: list[dict], assignments: dict,
                      relevance_tags: dict | None = None,
                      brand_name: str = "ClickUp") -> str:
    """Build the user prompt for a batch of posts.

    Args:
        posts_batch: List of post dicts with content
        assignments: Dict mapping post_id -> {"archetype": ..., "brand_mention": bool}
        relevance_tags: Dict mapping post_id -> "specific" or "vibe"
        brand_name: Brand name (will use preferred casing from config)

    Returns:
        User prompt string with all posts and their archetype assignments.
    """
    relevance_tags = relevance_tags or {}

    lines = []
    for i, post in enumerate(posts_batch, 1):
        assignment = assignments.get(post["id"], {})
        archetype = assignment.get("archetype", "personal_testimony")
        brand_mention = assignment.get("brand_mention", True)
        relevance = relevance_tags.get(post["id"], "specific").upper()

        # Serialize post content (with slide cap)
        content = _serialize_post_content(post)
        brand_instruction = f"Mention brand" if brand_mention else f"Do NOT mention brand"

        lines.append(f"""POST {i}:
- Account: @{post.get('account_username', 'unknown')}
POST CONTENT:
{content}
- Assigned archetype: {archetype}
- Brand instruction: {brand_instruction}
- Relevance: {relevance}
""")

    lines.append(f"Return a JSON array with {len(posts_batch)} objects. One comment per post.")
    return "\n".join(lines)


def _serialize_post_content(post: dict) -> str:
    """Serialize post content into readable text for the LLM.
    Caps at MAX_SLIDES slides and truncates long slide text."""
    parts = []

    # Hook (first slide text)
    hook = post.get("hook", "")
    if hook:
        parts.append(f"- Hook: {_trim(hook)}")

    # Slide texts — cap at MAX_SLIDES
    slides = post.get("slide_texts", [])
    total_slides = len(slides)
    capped = slides[:MAX_SLIDES]

    if capped:
        for j, text in enumerate(capped, 1):
            # Skip if it's the same as hook
            if j == 1 and hook and text.strip() == hook.strip():
                continue
            parts.append(f"- Slide {j}: {_trim(text)}")

        if total_slides > MAX_SLIDES:
            parts.append(f"- ({total_slides - MAX_SLIDES} more slides omitted)")

    # Caption — truncate if long
    caption = post.get("caption", "") or post.get("title", "")
    if caption:
        parts.append(f"- Caption: {_trim(caption, 150)}")

    if not parts:
        parts.append("- [No content extracted]")

    return "\n".join(parts)


def _trim(text: str, max_len: int = MAX_SLIDE_CHARS) -> str:
    """Truncate text to max_len chars."""
    text = text.strip()
    if len(text) <= max_len:
        return text
    return text[:max_len].rstrip() + "..."
