"""Archetype Selector — Deterministic, no LLM.
Assigns a comment archetype to each post using weighted random selection
with max-2-per-batch diversity enforcement."""

import random


def assign_archetypes(posts: list[dict], config: dict, batch_size: int = 8) -> list[dict]:
    """Assign archetypes to posts in batches.

    Args:
        posts: List of post dicts (must have 'id')
        config: Template config with 'archetype_weights' and 'brand_mention_strategy'
        batch_size: Posts per batch (5-8)

    Returns:
        List of dicts: [{"post_id": ..., "archetype": ..., "brand_mention": bool}, ...]
    """
    weights = config.get("archetype_weights", {})
    strategy = config.get("brand_mention_strategy", "always")
    assignments = []

    for b in range(0, len(posts), batch_size):
        batch = posts[b:b + batch_size]
        type_counts = {}

        for post in batch:
            arch = _pick_archetype(weights, type_counts)
            type_counts[arch] = type_counts.get(arch, 0) + 1
            brand_mention = _should_mention_brand(arch, strategy)

            assignments.append({
                "post_id": post["id"],
                "archetype": arch,
                "brand_mention": brand_mention,
            })

    return assignments


def _pick_archetype(weights: dict, type_counts: dict) -> str:
    """Weighted random pick with max-2-per-batch constraint."""
    available = {}
    total = 0.0

    for arch_type, weight in weights.items():
        if weight <= 0:
            continue
        if type_counts.get(arch_type, 0) >= 2:
            continue
        available[arch_type] = weight
        total += weight

    if total == 0:
        # All at max — pick from any with positive weight
        candidates = [t for t, w in weights.items() if w > 0]
        return random.choice(candidates) if candidates else "personal_testimony"

    rand = random.random() * total
    for arch_type, weight in available.items():
        rand -= weight
        if rand <= 0:
            return arch_type

    return list(available.keys())[0]


def _should_mention_brand(archetype: str, strategy: str) -> bool:
    """Determine if comment should mention the brand name."""
    if archetype == "feigned_ignorance":
        return False

    if strategy == "always":
        return True
    elif strategy == "mystery":
        return random.random() < 0.2
    elif strategy.startswith("mixed_"):
        pct = int(strategy.split("_")[1]) / 100
        return random.random() < pct
    else:
        return random.random() < 0.8
