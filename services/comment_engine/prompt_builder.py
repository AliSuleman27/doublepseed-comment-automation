"""Prompt Builder — Deterministic, no LLM.
Constructs system + user prompts from brand config and post data.
Caps slide text to avoid overwhelming token limits."""

import json

MAX_SLIDES = 5       # Max slides to include per post (skip the rest)
MAX_SLIDE_CHARS = 120  # Truncate individual slide text if too long


def build_system_prompt(brand_config: dict, template_config: dict) -> str:
    """Build the system prompt (sent once per batch).

    Contains: role, brand, theme story, persona, examples, rules, archetype defs.
    """
    brand_name = brand_config.get("name", "the brand")
    theme = template_config.get("theme_story", "")
    persona = template_config.get("commenting_persona", "")
    examples = template_config.get("example_comments", [])
    rules = template_config.get("comment_rules", {})
    archetypes = template_config.get("archetype_guidance", {})

    word_min, word_max = rules.get("word_count_range", [8, 18])
    allowed_slang = ", ".join(rules.get("allowed_slang", []))

    # Build archetype definitions section
    arch_defs = ""
    for arch_type, guidance in archetypes.items():
        patterns = guidance.get("patterns", [])
        pattern_str = "\n".join(f'    - "{p}"' for p in patterns)
        arch_defs += f"""
  {arch_type}:
    {guidance.get('description', '')}
    Patterns:
{pattern_str}
    Notes: {guidance.get('notes', '')}
"""

    # Build example comments section
    examples_str = "\n".join(f'  - "{e}"' for e in examples)

    return f"""You are a TikTok comment writer for {brand_name}.

BRAND: {brand_name}

THEME STORY:
{theme}

COMMENTING PERSONA:
{persona}

EXAMPLE COMMENTS (match this quality and voice):
{examples_str}

RULES — follow ALL of these strictly:
1. Word count: {word_min}-{word_max} words per comment
2. Each comment MUST reference something SPECIFIC from that post's content (a slide detail, the role, a tip)
3. Follow the assigned archetype exactly — this is an instruction, not a suggestion
4. No emojis, no hashtags, no trailing periods
5. Lowercase casual tone — like texting a friend. Fragments ok. Run-on style ok.
6. Allowed slang: {allowed_slang}
7. No ad language: never say "check out", "link in bio", "game changer", "must have", "sign up", "highly recommend"
8. When brand instruction says "Mention {brand_name}" — include the brand name naturally. When it says "Do NOT mention" — leave the brand out entirely.
9. Each comment in the batch MUST be completely different in structure — vary sentence openings, lengths, and patterns
10. Output ONLY a JSON array. No markdown, no explanation, no code fences.

ARCHETYPE DEFINITIONS:
{arch_defs}

OUTPUT FORMAT:
Return a JSON array exactly like this:
[{{"post_index": 1, "comment": "your comment here"}}, {{"post_index": 2, "comment": "..."}}]
"""


def build_user_prompt(posts_batch: list[dict], assignments: dict) -> str:
    """Build the user prompt for a batch of posts.

    Args:
        posts_batch: List of post dicts with content
        assignments: Dict mapping post_id -> {"archetype": ..., "brand_mention": bool}

    Returns:
        User prompt string with all posts and their archetype assignments.
    """
    brand_name = "ClickUp"  # Will be overridden from config at call site

    lines = []
    for i, post in enumerate(posts_batch, 1):
        assignment = assignments.get(post["id"], {})
        archetype = assignment.get("archetype", "personal_testimony")
        brand_mention = assignment.get("brand_mention", True)

        # Serialize post content (with slide cap)
        content = _serialize_post_content(post)
        brand_instruction = f"Mention {brand_name}" if brand_mention else f"Do NOT mention {brand_name}"

        lines.append(f"""POST {i}:
- Account: @{post.get('account_username', 'unknown')}
POST CONTENT:
{content}
- Assigned archetype: {archetype}
- Brand instruction: {brand_instruction}
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
