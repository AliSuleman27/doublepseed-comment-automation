"""LangGraph Pipeline — Orchestrates the full commenting workflow.

Enhanced Graph:
  assign_archetypes → tag_relevance → build_prompts → call_llm
  → structural_validate → apply_fallbacks → global_dedup → done

Supports:
  - Full pipeline (all batches at once)
  - Per-batch processing (for streaming to frontend)
  - UI overrides (archetype weights, relevance ratio, temperature, brand casing)
  - Structural validation (mid-period, compound 'and', opener diversity)
  - Global cross-batch dedup
  - Debug logging (when DEBUG=True)
"""

import os
import json
import time
import traceback
from typing import TypedDict, Any
from langgraph.graph import StateGraph, END

from .archetype import assign_archetypes, assign_relevance_tags
from .prompt_builder import build_system_prompt, build_user_prompt
from .llm_wrappers import call_llm, parse_llm_response
from .validator import (
    validate_comment, dedup_batch, structural_dedup_batch,
    check_opener_diversity,
)
from .fallback import get_fallback_comment, reset_golden_tracking

DEBUG = os.environ.get("CE_DEBUG", "true").lower() in ("1", "true", "yes")


def _log(msg: str):
    """Print debug log if DEBUG is enabled."""
    if DEBUG:
        print(f"[CE] {msg}")


class CommentEngineState(TypedDict, total=False):
    """State passed between LangGraph nodes."""
    # Inputs
    posts: list[dict]
    brand_config: dict
    template_config: dict
    model: str
    batch_size: int
    overrides: dict            # UI overrides

    # Intermediate
    assignments: list[dict]       # archetype assignments
    assignment_map: dict          # post_id -> assignment
    relevance_tags: dict          # post_id -> "specific" | "vibe"
    batches: list[list[dict]]     # posts grouped into batches
    system_prompt: str
    raw_responses: list[str]      # raw LLM responses per batch
    parsed_comments: list[dict]   # parsed comments with post_id

    # Output
    results: list[dict]           # final validated comments
    summary: dict                 # stats about the run
    errors: list[str]             # any errors encountered


# ─── Node 1: Assign Archetypes ─────────────────
def node_assign_archetypes(state: CommentEngineState) -> dict:
    """Deterministic archetype assignment with UI weight overrides."""
    posts = state["posts"]
    config = state["template_config"]
    batch_size = state.get("batch_size", 8)
    overrides = state.get("overrides", {})

    weight_overrides = overrides.get("archetype_weights")

    _log(f"── ARCHETYPE ASSIGNMENT ──")
    _log(f"  Posts: {len(posts)}, Batch size: {batch_size}")
    if weight_overrides:
        _log(f"  Weight overrides: {weight_overrides}")

    assignments = assign_archetypes(posts, config, batch_size, weight_overrides)
    assignment_map = {a["post_id"]: a for a in assignments}

    # Group posts into batches
    batches = []
    for i in range(0, len(posts), batch_size):
        batches.append(posts[i:i + batch_size])

    _log(f"  Batches: {len(batches)}")
    for a in assignments:
        _log(f"  Post {a['post_id'][:8]}... → {a['archetype']} (brand_mention={a['brand_mention']})")

    return {
        "assignments": assignments,
        "assignment_map": assignment_map,
        "batches": batches,
    }


# ─── Node 2: Tag Relevance ────────────────────
def node_tag_relevance(state: CommentEngineState) -> dict:
    """Tag each post as 'specific' or 'vibe' based on relevance_ratio."""
    posts = state["posts"]
    config = state["template_config"]
    overrides = state.get("overrides", {})

    relevance_ratio = overrides.get("relevance_ratio",
                                    config.get("relevance_ratio", 0.5))

    _log(f"── RELEVANCE TAGGING ── (ratio={relevance_ratio})")

    tags = assign_relevance_tags(posts, relevance_ratio)

    for pid, tag in tags.items():
        _log(f"  Post {pid[:8]}... → {tag}")

    return {"relevance_tags": tags}


# ─── Node 3: Build Prompts ─────────────────────
def node_build_prompts(state: CommentEngineState) -> dict:
    """Build system prompt (once) with enhanced rules."""
    brand_config = state["brand_config"]
    template_config = state["template_config"]
    overrides = state.get("overrides", {})

    system_prompt = build_system_prompt(brand_config, template_config, overrides)

    _log(f"── SYSTEM PROMPT ──")
    _log(f"  Length: {len(system_prompt)} chars, ~{len(system_prompt.split())} words")
    if DEBUG:
        for line in system_prompt.split('\n'):
            print(f"  | {line}")

    return {"system_prompt": system_prompt}


# ─── Node 4: Call LLM ──────────────────────────
def node_call_llm(state: CommentEngineState) -> dict:
    """Call LLM for each batch with enhanced user prompts."""
    model = state.get("model", "claude-haiku")
    system_prompt = state["system_prompt"]
    batches = state["batches"]
    assignment_map = state["assignment_map"]
    relevance_tags = state.get("relevance_tags", {})
    brand_config = state["brand_config"]
    overrides = state.get("overrides", {})

    temperature = overrides.get("temperature", 0.9)

    raw_responses = []
    parsed_comments = []
    errors = list(state.get("errors", []))

    for batch_idx, batch in enumerate(batches):
        _log(f"── LLM BATCH {batch_idx + 1}/{len(batches)} ── ({len(batch)} posts, model={model}, temp={temperature})")

        user_prompt = build_user_prompt(
            batch, assignment_map, relevance_tags,
            brand_name=brand_config.get("name", "the brand"),
        )

        if DEBUG:
            _log(f"  USER PROMPT ({len(user_prompt)} chars):")
            for line in user_prompt.split('\n'):
                print(f"  > {line}")

        try:
            t0 = time.time()
            raw = call_llm(model, system_prompt, user_prompt, temperature=temperature)
            elapsed = time.time() - t0
            raw_responses.append(raw)

            _log(f"  LLM RESPONSE ({elapsed:.1f}s, {len(raw)} chars):")
            if DEBUG:
                for line in raw.split('\n'):
                    print(f"  < {line}")

            comments = parse_llm_response(raw)
            _log(f"  Parsed {len(comments)} comments from response")

            seen_ids = set()
            for c in comments:
                idx = c.get("post_index", 0) - 1
                if 0 <= idx < len(batch):
                    post = batch[idx]
                    seen_ids.add(post["id"])
                    parsed_comments.append({
                        "post_id": post["id"],
                        "text": c.get("comment", ""),
                        "source": "llm",
                        "batch_index": batch_idx,
                    })

            for post in batch:
                if post["id"] not in seen_ids:
                    _log(f"  Post {post['id'][:8]}... missing from LLM → will fallback")
                    parsed_comments.append({
                        "post_id": post["id"],
                        "text": "",
                        "source": "llm_failed",
                        "batch_index": batch_idx,
                    })

        except Exception as e:
            tb = traceback.format_exc()
            errors.append(f"Batch {batch_idx + 1} LLM error: {str(e)}")
            _log(f"  ERROR: {e}")
            if DEBUG:
                print(tb)

            for post in batch:
                parsed_comments.append({
                    "post_id": post["id"],
                    "text": "",
                    "source": "llm_failed",
                    "batch_index": batch_idx,
                })

    return {
        "raw_responses": raw_responses,
        "parsed_comments": parsed_comments,
        "errors": errors,
    }


# ─── Node 5: Structural Validate ─────────────────
def node_structural_validate(state: CommentEngineState) -> dict:
    """Validate all comments: mechanical rules + structural quality + dedup."""
    parsed = state["parsed_comments"]
    template_config = state["template_config"]
    brand_config = state["brand_config"]
    assignment_map = state["assignment_map"]
    overrides = state.get("overrides", {})

    _log(f"── STRUCTURAL VALIDATION ── ({len(parsed)} comments)")

    validated = []
    for c in parsed:
        assignment = assignment_map.get(c["post_id"], {})
        brand_mention = assignment.get("brand_mention", True)

        if c["source"] == "llm_failed" or not c["text"]:
            validated.append({
                **c,
                "valid": False,
                "hard_fail": True,
                "checks": [{"label": "LLM failed", "status": "fail"}],
            })
            _log(f"  Post {c['post_id'][:8]}... → SKIP (LLM failed)")
            continue

        result = validate_comment(
            c["text"], template_config, brand_config, brand_mention
        )

        checks_str = ", ".join(f"{ch['label']}:{ch['status']}" for ch in result["checks"])
        _log(f"  Post {c['post_id'][:8]}... → valid={result['valid']} [{checks_str}]")
        _log(f"    Text: \"{result['text']}\"")

        validated.append({
            **c,
            "text": result["text"],
            "valid": result["valid"],
            "hard_fail": result["hard_fail"],
            "checks": result["checks"],
        })

    # ─── Batch-level dedup (Jaccard bigram) ──────────
    _log(f"── DEDUP (Jaccard + Structural + Opener) ──")
    batch_groups = {}
    for c in validated:
        bi = c.get("batch_index", 0)
        batch_groups.setdefault(bi, []).append(c)

    for bi, batch_comments in batch_groups.items():
        valid_in_batch = [c for c in batch_comments if c["valid"]]
        if len(valid_in_batch) > 1:
            # Jaccard bigram dedup
            deduped = dedup_batch(valid_in_batch)
            for c in deduped:
                if c.get("is_duplicate"):
                    c["valid"] = False
                    c["checks"].append({
                        "label": f"Duplicate (sim={c.get('dup_similarity', '?')})",
                        "status": "fail",
                    })
                    _log(f"  Post {c['post_id'][:8]}... → JACCARD DUPLICATE")

            # Structural skeleton dedup
            still_valid = [c for c in batch_comments if c["valid"]]
            if len(still_valid) > 1:
                max_per_skel = overrides.get("max_per_structure", 1)
                structural_dedup_batch(still_valid, max_per_skel)
                for c in still_valid:
                    if c.get("is_structural_dup"):
                        c["valid"] = False
                        c["checks"].append({
                            "label": "Structural duplicate",
                            "status": "fail",
                        })
                        _log(f"  Post {c['post_id'][:8]}... → STRUCTURAL DUPLICATE")

            # Opener diversity check
            still_valid2 = [c for c in batch_comments if c["valid"]]
            if len(still_valid2) > 1:
                check_opener_diversity(still_valid2, max_same_opener=1)
                for c in still_valid2:
                    if c.get("duplicate_opener"):
                        c["valid"] = False
                        c["checks"].append({
                            "label": "Duplicate opener",
                            "status": "fail",
                        })
                        _log(f"  Post {c['post_id'][:8]}... → DUPLICATE OPENER")

    return {"parsed_comments": validated}


# ─── Node 6: Apply Fallbacks ───────────────────
def node_apply_fallbacks(state: CommentEngineState) -> dict:
    """Replace failed/duplicate comments with fallback templates."""
    comments = state["parsed_comments"]
    posts = state["posts"]
    assignment_map = state["assignment_map"]
    brand_name = state["brand_config"].get("name", "the brand")
    template_config = state["template_config"]
    brand_config = state["brand_config"]
    golden_comments = template_config.get("golden_comments", [])

    post_map = {p["id"]: p for p in posts}
    results = []

    llm_pass = 0
    fallback_used = 0
    flagged = 0
    structural_fails = 0

    _log(f"── FALLBACKS ──")

    # Reset golden tracking for this run
    reset_golden_tracking()

    for c in comments:
        post = post_map.get(c["post_id"], {})
        assignment = assignment_map.get(c["post_id"], {})
        archetype = assignment.get("archetype", "personal_testimony")
        brand_mention = assignment.get("brand_mention", True)

        if c["valid"]:
            status = "pass"
            if any(ch["status"] == "warn" for ch in c.get("checks", [])):
                status = "flagged"
                flagged += 1
            else:
                llm_pass += 1

            # Track structural fail reasons even for passed comments
            struct_checks = [ch for ch in c.get("checks", [])
                            if ch["label"] in ("No mid-sentence period", "No compound 'and'")]
            if any(ch["status"] == "fail" for ch in struct_checks):
                structural_fails += 1

            _log(f"  Post {c['post_id'][:8]}... → {status} (LLM)")

            results.append({
                "post_id": c["post_id"],
                "account_username": post.get("account_username", ""),
                "tiktok_url": post.get("tiktok_url", ""),
                "archetype": archetype,
                "brand_mention": brand_mention,
                "comment": c["text"],
                "word_count": len(c["text"].split()),
                "source": "llm",
                "status": status,
                "checks": c.get("checks", []),
            })
        else:
            # Use fallback — prefer golden comments
            fallback_text = get_fallback_comment(
                post, archetype, brand_mention, brand_name,
                golden_comments=golden_comments,
            )
            fb_result = validate_comment(
                fallback_text, template_config, brand_config, brand_mention
            )
            fallback_used += 1

            # Count why it failed
            fail_labels = [ch["label"] for ch in c.get("checks", []) if ch["status"] == "fail"]
            if any(l in ("No mid-sentence period", "No compound 'and'", "Structural duplicate",
                        "Duplicate opener", "No banned patterns") for l in fail_labels):
                structural_fails += 1

            _log(f"  Post {c['post_id'][:8]}... → FALLBACK: \"{fb_result['text']}\"")

            results.append({
                "post_id": c["post_id"],
                "account_username": post.get("account_username", ""),
                "tiktok_url": post.get("tiktok_url", ""),
                "archetype": archetype,
                "brand_mention": brand_mention,
                "comment": fb_result["text"],
                "word_count": len(fb_result["text"].split()),
                "source": "fallback",
                "status": "fallback",
                "checks": fb_result["checks"],
            })

    summary = {
        "total_posts": len(posts),
        "total_comments": len(results),
        "llm_pass": llm_pass,
        "flagged": flagged,
        "fallback_used": fallback_used,
        "structural_fails": structural_fails,
        "batches": len(state.get("batches", [])),
        "model": state.get("model", "unknown"),
        "errors": state.get("errors", []),
    }

    _log(f"── SUMMARY ── pass={llm_pass}, flagged={flagged}, fallback={fallback_used}, structural_fails={structural_fails}")

    return {"results": results, "summary": summary}


# ─── Build the Graph ────────────────────────────
def _build_graph() -> StateGraph:
    graph = StateGraph(CommentEngineState)

    graph.add_node("assign_archetypes", node_assign_archetypes)
    graph.add_node("tag_relevance", node_tag_relevance)
    graph.add_node("build_prompts", node_build_prompts)
    graph.add_node("call_llm", node_call_llm)
    graph.add_node("structural_validate", node_structural_validate)
    graph.add_node("apply_fallbacks", node_apply_fallbacks)

    graph.set_entry_point("assign_archetypes")
    graph.add_edge("assign_archetypes", "tag_relevance")
    graph.add_edge("tag_relevance", "build_prompts")
    graph.add_edge("build_prompts", "call_llm")
    graph.add_edge("call_llm", "structural_validate")
    graph.add_edge("structural_validate", "apply_fallbacks")
    graph.add_edge("apply_fallbacks", END)

    return graph.compile()


# Compiled graph (singleton)
_compiled_graph = None


def _get_graph():
    global _compiled_graph
    if _compiled_graph is None:
        _compiled_graph = _build_graph()
    return _compiled_graph


def run_pipeline(
    posts: list[dict],
    brand_config: dict,
    template_config: dict,
    model: str = "claude-haiku",
    batch_size: int = 8,
    overrides: dict | None = None,
) -> dict:
    """Run the full comment engine pipeline.

    Args:
        posts: List of post dicts (from the post viewer)
        brand_config: Brand section of the config JSON
        template_config: Template section of the config JSON
        model: "claude-haiku", "gpt-4o-mini", or "gemini-flash"
        batch_size: Posts per LLM batch (5-8)
        overrides: UI overrides dict

    Returns:
        {"results": [...], "summary": {...}, "assignments": [...]}
    """
    _log(f"═══ PIPELINE START ═══ posts={len(posts)}, model={model}, batch_size={batch_size}")

    graph = _get_graph()

    initial_state: CommentEngineState = {
        "posts": posts,
        "brand_config": brand_config,
        "template_config": template_config,
        "model": model,
        "batch_size": batch_size,
        "overrides": overrides or {},
        "errors": [],
    }

    final_state = graph.invoke(initial_state)

    _log(f"═══ PIPELINE DONE ═══")

    return {
        "results": final_state.get("results", []),
        "summary": final_state.get("summary", {}),
        "assignments": final_state.get("assignments", []),
    }


# ─── Per-Batch Processing (for streaming) ──────
def prepare_pipeline(posts, brand_config, template_config, model="claude-haiku",
                     batch_size=8, overrides=None):
    """Prepare the pipeline: assign archetypes, tag relevance, build system prompt.

    Returns dict with everything needed to call process_batch() for each batch.
    """
    overrides = overrides or {}
    _log(f"═══ PIPELINE PREPARE ═══ posts={len(posts)}, model={model}, batch_size={batch_size}")
    if overrides:
        _log(f"  Overrides: {json.dumps(overrides, default=str)}")

    weight_overrides = overrides.get("archetype_weights")
    assignments = assign_archetypes(posts, template_config, batch_size, weight_overrides)
    assignment_map = {a["post_id"]: a for a in assignments}

    for a in assignments:
        _log(f"  Archetype: {a['post_id'][:8]}... → {a['archetype']} (brand={a['brand_mention']})")

    # Relevance tagging
    relevance_ratio = overrides.get("relevance_ratio",
                                    template_config.get("relevance_ratio", 0.5))
    relevance_tags = assign_relevance_tags(posts, relevance_ratio)
    _log(f"  Relevance tags: {sum(1 for v in relevance_tags.values() if v == 'specific')} specific, "
         f"{sum(1 for v in relevance_tags.values() if v == 'vibe')} vibe (ratio={relevance_ratio})")

    system_prompt = build_system_prompt(brand_config, template_config, overrides)
    _log(f"  System prompt: {len(system_prompt)} chars")
    if DEBUG:
        for line in system_prompt.split('\n'):
            print(f"  | {line}")

    batches = []
    for i in range(0, len(posts), batch_size):
        batches.append(posts[i:i + batch_size])

    _log(f"  Batches: {len(batches)}")

    # Reset golden comment tracking
    reset_golden_tracking()

    return {
        "assignments": assignments,
        "assignment_map": assignment_map,
        "relevance_tags": relevance_tags,
        "batches": batches,
        "system_prompt": system_prompt,
        "brand_config": brand_config,
        "template_config": template_config,
        "model": model,
        "posts": posts,
        "overrides": overrides,
        # Track global comments for cross-batch dedup
        "_all_results": [],
    }


def process_batch(prep, batch_idx):
    """Process a single batch: LLM call → structural validate → fallback.

    Returns dict with batch results, or error info.
    """
    batch = prep["batches"][batch_idx]
    model = prep["model"]
    system_prompt = prep["system_prompt"]
    assignment_map = prep["assignment_map"]
    relevance_tags = prep.get("relevance_tags", {})
    brand_config = prep["brand_config"]
    template_config = prep["template_config"]
    brand_name = brand_config.get("name", "the brand")
    golden_comments = template_config.get("golden_comments", [])
    overrides = prep.get("overrides", {})

    temperature = overrides.get("temperature", 0.9)
    total_batches = len(prep["batches"])
    _log(f"── BATCH {batch_idx + 1}/{total_batches} ── ({len(batch)} posts, model={model}, temp={temperature})")

    # Build user prompt with relevance tags
    user_prompt = build_user_prompt(
        batch, assignment_map, relevance_tags,
        brand_name=brand_name,
    )

    if DEBUG:
        _log(f"  USER PROMPT ({len(user_prompt)} chars):")
        for line in user_prompt.split('\n'):
            print(f"  > {line}")

    parsed_comments = []
    error = None
    seen_post_ids = set()

    try:
        t0 = time.time()
        raw = call_llm(model, system_prompt, user_prompt, temperature=temperature)
        elapsed = time.time() - t0

        _log(f"  LLM RESPONSE ({elapsed:.1f}s, {len(raw)} chars):")
        if DEBUG:
            for line in raw.split('\n'):
                print(f"  < {line}")

        comments = parse_llm_response(raw)
        _log(f"  Parsed {len(comments)} comments")

        for c in comments:
            idx = c.get("post_index", 0) - 1
            if 0 <= idx < len(batch):
                post = batch[idx]
                seen_post_ids.add(post["id"])
                parsed_comments.append({
                    "post_id": post["id"],
                    "text": c.get("comment", ""),
                    "source": "llm",
                    "batch_index": batch_idx,
                })

        for post in batch:
            if post["id"] not in seen_post_ids:
                _log(f"  Post {post['id'][:8]}... missing from LLM response → will fallback")
                parsed_comments.append({
                    "post_id": post["id"],
                    "text": "",
                    "source": "llm_failed",
                    "batch_index": batch_idx,
                })

    except Exception as e:
        error = f"Batch {batch_idx + 1} LLM error: {str(e)}"
        _log(f"  ERROR: {e}")
        if DEBUG:
            print(traceback.format_exc())
        for post in batch:
            parsed_comments.append({
                "post_id": post["id"],
                "text": "",
                "source": "llm_failed",
                "batch_index": batch_idx,
            })

    # ─── Validate (mechanical + structural) ──────────
    _log(f"  VALIDATING {len(parsed_comments)} comments...")
    validated = []
    for c in parsed_comments:
        assignment = assignment_map.get(c["post_id"], {})
        brand_mention = assignment.get("brand_mention", True)

        if c["source"] == "llm_failed" or not c["text"]:
            validated.append({**c, "valid": False, "hard_fail": True,
                            "checks": [{"label": "LLM failed", "status": "fail"}]})
            _log(f"    {c['post_id'][:8]}... → SKIP (LLM failed)")
            continue

        result = validate_comment(c["text"], template_config, brand_config, brand_mention)
        checks_str = ", ".join(f"{ch['label']}:{ch['status']}" for ch in result["checks"])
        _log(f"    {c['post_id'][:8]}... → valid={result['valid']} [{checks_str}]")
        _log(f"      \"{result['text']}\"")

        validated.append({
            **c, "text": result["text"], "valid": result["valid"],
            "hard_fail": result["hard_fail"], "checks": result["checks"],
        })

    # ─── Batch dedup (Jaccard + Structural + Opener) ──────────
    valid_in_batch = [c for c in validated if c["valid"]]
    if len(valid_in_batch) > 1:
        deduped = dedup_batch(valid_in_batch)
        for c in deduped:
            if c.get("is_duplicate"):
                c["valid"] = False
                c["checks"].append({"label": f"Duplicate (sim={c.get('dup_similarity', '?')})", "status": "fail"})
                _log(f"    {c['post_id'][:8]}... → JACCARD DUPLICATE")

    still_valid = [c for c in validated if c["valid"]]
    if len(still_valid) > 1:
        max_per_skel = overrides.get("max_per_structure", 1)
        structural_dedup_batch(still_valid, max_per_skel)
        for c in still_valid:
            if c.get("is_structural_dup"):
                c["valid"] = False
                c["checks"].append({"label": "Structural duplicate", "status": "fail"})
                _log(f"    {c['post_id'][:8]}... → STRUCTURAL DUPLICATE")

    still_valid2 = [c for c in validated if c["valid"]]
    if len(still_valid2) > 1:
        check_opener_diversity(still_valid2, max_same_opener=1)
        for c in still_valid2:
            if c.get("duplicate_opener"):
                c["valid"] = False
                c["checks"].append({"label": "Duplicate opener", "status": "fail"})
                _log(f"    {c['post_id'][:8]}... → DUPLICATE OPENER")

    # ─── Global cross-batch dedup ──────────
    prev_results = prep.get("_all_results", [])
    if prev_results:
        still_valid3 = [c for c in validated if c["valid"]]
        prev_texts = [r["comment"] for r in prev_results]
        for c in still_valid3:
            for prev_text in prev_texts:
                from .validator import _get_bigrams, _jaccard
                sim = _jaccard(_get_bigrams(c["text"]), _get_bigrams(prev_text))
                if sim > 0.5:
                    c["valid"] = False
                    c["checks"].append({"label": f"Cross-batch dup (sim={round(sim, 2)})", "status": "fail"})
                    _log(f"    {c['post_id'][:8]}... → CROSS-BATCH DUPLICATE")
                    break

    # ─── Apply fallbacks ──────────
    post_map = {p["id"]: p for p in batch}
    results = []
    llm_pass = 0
    fallback_used = 0
    flagged = 0
    structural_fails = 0

    for c in validated:
        post = post_map.get(c["post_id"], {})
        assignment = assignment_map.get(c["post_id"], {})
        archetype = assignment.get("archetype", "personal_testimony")
        brand_mention = assignment.get("brand_mention", True)

        if c["valid"]:
            status = "pass"
            if any(ch["status"] == "warn" for ch in c.get("checks", [])):
                status = "flagged"
                flagged += 1
            else:
                llm_pass += 1

            results.append({
                "post_id": c["post_id"],
                "account_username": post.get("account_username", ""),
                "tiktok_url": post.get("tiktok_url", ""),
                "archetype": archetype,
                "brand_mention": brand_mention,
                "comment": c["text"],
                "word_count": len(c["text"].split()),
                "source": "llm",
                "status": status,
                "checks": c.get("checks", []),
            })
        else:
            # Count structural failure reasons
            fail_labels = [ch["label"] for ch in c.get("checks", []) if ch["status"] == "fail"]
            if any(l in ("No mid-sentence period", "No compound 'and'", "Structural duplicate",
                        "Duplicate opener", "No banned patterns") for l in fail_labels):
                structural_fails += 1

            fallback_text = get_fallback_comment(
                post, archetype, brand_mention, brand_name,
                golden_comments=golden_comments,
            )
            fb_result = validate_comment(fallback_text, template_config, brand_config, brand_mention)
            fallback_used += 1

            _log(f"    {c['post_id'][:8]}... → FALLBACK: \"{fb_result['text']}\"")

            results.append({
                "post_id": c["post_id"],
                "account_username": post.get("account_username", ""),
                "tiktok_url": post.get("tiktok_url", ""),
                "archetype": archetype,
                "brand_mention": brand_mention,
                "comment": fb_result["text"],
                "word_count": len(fb_result["text"].split()),
                "source": "fallback",
                "status": "fallback",
                "checks": fb_result["checks"],
            })

    # Track results globally for cross-batch dedup
    prep["_all_results"].extend(results)

    batch_summary = {
        "batch_index": batch_idx,
        "posts_in_batch": len(batch),
        "llm_pass": llm_pass,
        "flagged": flagged,
        "fallback_used": fallback_used,
        "structural_fails": structural_fails,
    }
    _log(f"  BATCH {batch_idx + 1} DONE: pass={llm_pass}, flagged={flagged}, fallback={fallback_used}, structural={structural_fails}")

    return {
        "results": results,
        "assignments": [assignment_map[p["id"]] for p in batch if p["id"] in assignment_map],
        "batch_summary": batch_summary,
        "error": error,
    }
