"""Fallback Template Bank — Deterministic, no LLM.
Pre-written parameterized comments for when LLM fails or validation rejects.
Enhanced: Prioritizes golden comments (real human-approved) over generic templates."""

import random


# Generic fallback templates (parameterized) — used only when golden_comments exhausted
GENERIC_TEMPLATES = [
    "I used to {pain} and {brand} ended that",
    "{brand} giving {role}s their time back is the content I needed today",
    "downloading {brand} rn because of this",
    "I need {brand} just to stop doing {pain} the hard way",
    "not me screenshotting this to send to my {person} with a {brand} link",
    "why did nobody tell me about {brand} sooner I needed this years ago",
    "{brand} for {topic} is actually genius",
    "me realizing {brand} could fix my entire overwhelmed era",
    "honestly {brand} would save me so much time with {topic}",
    "{brand} understanding how real people actually work is wild",
    "ok but {brand} being free while doing all this is actually crazy",
]

# Feigned ignorance templates (no brand mention)
FEIGNED_TEMPLATES = [
    "can someone drop the app name bc I need this",
    "what app is that at the end",
    "anyone know what app she is talking about",
    "wait what is that app I need it yesterday",
    "can someone please tell me what app that is",
    "ok but what is that app she mentioned in the slides",
]

# Common slot fill values for generic fallbacks
GENERIC_PAINS = [
    "track everything in my head",
    "juggle a million things manually",
    "plan everything on sticky notes",
    "forget half my to-dos by noon",
    "wing every single day",
]

GENERIC_PERSONS = ["coworker", "manager", "team", "friend", "partner"]
GENERIC_TOPICS = ["project management", "task tracking", "staying organized", "work-life balance"]

# Track which golden comments have been used in this run to avoid repeats
_used_golden = set()


def reset_golden_tracking():
    """Reset the used golden comments tracker (call at pipeline start)."""
    global _used_golden
    _used_golden = set()


def get_fallback_comment(post: dict, archetype: str, brand_mention: bool,
                         brand_name: str, golden_comments: list | None = None) -> str:
    """Generate a fallback comment — prefers golden comments over generic templates.

    Args:
        post: Post dict with content
        archetype: The assigned archetype
        brand_mention: Whether to mention the brand
        brand_name: The brand name string
        golden_comments: List of human-approved golden comments from config

    Returns:
        A fallback comment string.
    """
    if archetype == "feigned_ignorance" or not brand_mention:
        return random.choice(FEIGNED_TEMPLATES)

    # Try golden comments first (unused ones)
    if golden_comments:
        available = [c for c in golden_comments if c not in _used_golden]
        if available:
            chosen = random.choice(available)
            _used_golden.add(chosen)
            return chosen

    # Fall back to parameterized templates
    template = random.choice(GENERIC_TEMPLATES)

    role = _extract_role(post)
    pain = _extract_pain(post)
    topic = _extract_topic(post)
    person = random.choice(GENERIC_PERSONS)

    filled = template.format(
        brand=brand_name.lower(),  # Use lowercase brand in fallbacks for naturalness
        role=role,
        pain=pain,
        topic=topic,
        person=person,
    )

    return filled


def _extract_role(post: dict) -> str:
    """Try to extract a role/profession from post content."""
    slides = post.get("slide_texts", [])
    if slides and len(slides) >= 2:
        text = slides[1] if len(slides) > 1 else slides[0]
        if len(text.split()) <= 6:
            return text.lower().strip()

    hook = post.get("hook", "")
    if hook:
        lower = hook.lower()
        for prefix in ["as a ", "as an ", "being a ", "working as a "]:
            if prefix in lower:
                after = lower.split(prefix, 1)[1]
                words = after.split()[:3]
                return " ".join(words)

    return random.choice(["professional", "working person", "busy person"])


def _extract_pain(post: dict) -> str:
    """Try to extract a pain point from post content."""
    slides = post.get("slide_texts", [])
    if slides:
        for text in slides[1:4]:
            if len(text.split()) >= 4:
                words = text.lower().split()[:8]
                return " ".join(words)

    return random.choice(GENERIC_PAINS)


def _extract_topic(post: dict) -> str:
    """Try to extract a topic from the post."""
    caption = post.get("caption", "") or post.get("title", "")
    if caption:
        words = caption.split()[:4]
        return " ".join(words).lower().rstrip(".,!?")

    return random.choice(GENERIC_TOPICS)
