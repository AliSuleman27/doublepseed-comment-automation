"""LangGraph Pipeline — Orchestrates the full commenting workflow.

Graph:
  assign_archetypes → build_prompts → call_llm → validate → apply_fallbacks → done

Supports:
  - Full pipeline (all batches at once)
  - Per-batch processing (for streaming to frontend)
  - Debug logging (when DEBUG=True)
"""

import os
import json
import time
import traceback
from typing import TypedDict, Any
from langgraph.graph import StateGraph, END

from .archetype import assign_archetypes
from .prompt_builder import build_system_prompt, build_user_prompt
from .llm_wrappers import call_llm, parse_llm_response
from .validator import validate_comment, dedup_batch
from .fallback import get_fallback_comment

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

    # Intermediate
    assignments: list[dict]       # archetype assignments
    assignment_map: dict          # post_id -> assignment
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
    """Deterministic archetype assignment."""
    posts = state["posts"]
    config = state["template_config"]
    batch_size = state.get("batch_size", 8)

    _log(f"── ARCHETYPE ASSIGNMENT ──")
    _log(f"  Posts: {len(posts)}, Batch size: {batch_size}")

    assignments = assign_archetypes(posts, config, batch_size)
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


# ─── Node 2: Build Prompts ─────────────────────
def node_build_prompts(state: CommentEngineState) -> dict:
    """Build system prompt (once) and prepare for batched LLM calls."""
    brand_config = state["brand_config"]
    template_config = state["template_config"]

    system_prompt = build_system_prompt(brand_config, template_config)

    _log(f"── SYSTEM PROMPT ──")
    _log(f"  Length: {len(system_prompt)} chars, ~{len(system_prompt.split())} words")
    if DEBUG:
        for line in system_prompt.split('\n'):
            print(f"  | {line}")

    return {"system_prompt": system_prompt}


# ─── Node 3: Call LLM ──────────────────────────
def node_call_llm(state: CommentEngineState) -> dict:
    """Call LLM for each batch. This is the ONLY AI cost."""
    model = state.get("model", "claude-haiku")
    system_prompt = state["system_prompt"]
    batches = state["batches"]
    assignment_map = state["assignment_map"]
    brand_name = state["brand_config"].get("name", "the brand")

    raw_responses = []
    parsed_comments = []
    errors = list(state.get("errors", []))

    for batch_idx, batch in enumerate(batches):
        _log(f"── LLM BATCH {batch_idx + 1}/{len(batches)} ── ({len(batch)} posts, model={model})")

        # Inject brand name into prompt builder
        user_prompt = build_user_prompt(batch, assignment_map)
        # Replace generic brand reference with actual brand
        user_prompt = user_prompt.replace("Mention ClickUp", f"Mention {brand_name}")
        user_prompt = user_prompt.replace("Do NOT mention ClickUp", f"Do NOT mention {brand_name}")

        if DEBUG:
            _log(f"  USER PROMPT ({len(user_prompt)} chars):")
            for line in user_prompt.split('\n'):
                print(f"  > {line}")

        try:
            t0 = time.time()
            raw = call_llm(model, system_prompt, user_prompt, temperature=0.9)
            elapsed = time.time() - t0
            raw_responses.append(raw)

            _log(f"  LLM RESPONSE ({elapsed:.1f}s, {len(raw)} chars):")
            if DEBUG:
                for line in raw.split('\n'):
                    print(f"  < {line}")

            comments = parse_llm_response(raw)
            _log(f"  Parsed {len(comments)} comments from response")

            # Map back to post IDs
            seen_ids = set()
            for c in comments:
                idx = c.get("post_index", 0) - 1  # 1-indexed to 0-indexed
                if 0 <= idx < len(batch):
                    post = batch[idx]
                    seen_ids.add(post["id"])
                    parsed_comments.append({
                        "post_id": post["id"],
                        "text": c.get("comment", ""),
                        "source": "llm",
                        "batch_index": batch_idx,
                    })

            # Fill missing posts with fallback marker
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

            # Mark all posts in this batch as needing fallback
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


# ─── Node 4: Validate ──────────────────────────
def node_validate(state: CommentEngineState) -> dict:
    """Validate all comments and run batch dedup."""
    parsed = state["parsed_comments"]
    template_config = state["template_config"]
    brand_config = state["brand_config"]
    assignment_map = state["assignment_map"]
    batch_size = state.get("batch_size", 8)

    _log(f"── VALIDATION ── ({len(parsed)} comments)")

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

    # Batch dedup
    _log(f"── DEDUP ──")
    batch_groups = {}
    for c in validated:
        bi = c.get("batch_index", 0)
        batch_groups.setdefault(bi, []).append(c)

    for bi, batch_comments in batch_groups.items():
        valid_in_batch = [c for c in batch_comments if c["valid"]]
        if len(valid_in_batch) > 1:
            deduped = dedup_batch(valid_in_batch)
            for c in deduped:
                if c.get("is_duplicate"):
                    c["valid"] = False
                    c["checks"].append({
                        "label": f"Duplicate ({c.get('dup_similarity', '?')})",
                        "status": "fail",
                    })
                    _log(f"  Post {c['post_id'][:8]}... → DUPLICATE (sim={c.get('dup_similarity')})")

    return {"parsed_comments": validated}


# ─── Node 5: Apply Fallbacks ───────────────────
def node_apply_fallbacks(state: CommentEngineState) -> dict:
    """Replace failed/duplicate comments with fallback templates."""
    comments = state["parsed_comments"]
    posts = state["posts"]
    assignment_map = state["assignment_map"]
    brand_name = state["brand_config"].get("name", "the brand")
    template_config = state["template_config"]
    brand_config = state["brand_config"]

    post_map = {p["id"]: p for p in posts}
    results = []

    llm_pass = 0
    fallback_used = 0
    flagged = 0

    _log(f"── FALLBACKS ──")

    for c in comments:
        post = post_map.get(c["post_id"], {})
        assignment = assignment_map.get(c["post_id"], {})
        archetype = assignment.get("archetype", "personal_testimony")
        brand_mention = assignment.get("brand_mention", True)

        if c["valid"]:
            # LLM comment passed validation
            status = "pass"
            # Check for warnings
            if any(ch["status"] == "warn" for ch in c.get("checks", [])):
                status = "flagged"
                flagged += 1
            else:
                llm_pass += 1

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
            # Use fallback
            fallback_text = get_fallback_comment(
                post, archetype, brand_mention, brand_name
            )
            # Validate the fallback too
            fb_result = validate_comment(
                fallback_text, template_config, brand_config, brand_mention
            )
            fallback_used += 1

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
        "batches": len(state.get("batches", [])),
        "model": state.get("model", "unknown"),
        "errors": state.get("errors", []),
    }

    _log(f"── SUMMARY ── pass={llm_pass}, flagged={flagged}, fallback={fallback_used}, total={len(results)}")

    return {"results": results, "summary": summary}


# ─── Build the Graph ────────────────────────────
def _build_graph() -> StateGraph:
    graph = StateGraph(CommentEngineState)

    graph.add_node("assign_archetypes", node_assign_archetypes)
    graph.add_node("build_prompts", node_build_prompts)
    graph.add_node("call_llm", node_call_llm)
    graph.add_node("validate", node_validate)
    graph.add_node("apply_fallbacks", node_apply_fallbacks)

    graph.set_entry_point("assign_archetypes")
    graph.add_edge("assign_archetypes", "build_prompts")
    graph.add_edge("build_prompts", "call_llm")
    graph.add_edge("call_llm", "validate")
    graph.add_edge("validate", "apply_fallbacks")
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
) -> dict:
    """Run the full comment engine pipeline.

    Args:
        posts: List of post dicts (from the post viewer)
        brand_config: Brand section of the config JSON
        template_config: Template section of the config JSON
        model: "claude-haiku", "gpt-4o-mini", or "gemini-flash"
        batch_size: Posts per LLM batch (5-8)

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
def prepare_pipeline(posts, brand_config, template_config, model="claude-haiku", batch_size=8):
    """Prepare the pipeline: assign archetypes, build system prompt, create batches.

    Returns dict with everything needed to call process_batch() for each batch.
    """
    _log(f"═══ PIPELINE PREPARE ═══ posts={len(posts)}, model={model}, batch_size={batch_size}")

    assignments = assign_archetypes(posts, template_config, batch_size)
    assignment_map = {a["post_id"]: a for a in assignments}

    for a in assignments:
        _log(f"  Archetype: {a['post_id'][:8]}... → {a['archetype']} (brand={a['brand_mention']})")

    system_prompt = build_system_prompt(brand_config, template_config)
    _log(f"  System prompt: {len(system_prompt)} chars")
    if DEBUG:
        for line in system_prompt.split('\n'):
            print(f"  | {line}")

    batches = []
    for i in range(0, len(posts), batch_size):
        batches.append(posts[i:i + batch_size])

    _log(f"  Batches: {len(batches)}")

    return {
        "assignments": assignments,
        "assignment_map": assignment_map,
        "batches": batches,
        "system_prompt": system_prompt,
        "brand_config": brand_config,
        "template_config": template_config,
        "model": model,
        "posts": posts,
    }


def process_batch(prep, batch_idx):
    """Process a single batch: LLM call → validate → fallback.

    Returns dict with batch results, or error info.
    """
    batch = prep["batches"][batch_idx]
    model = prep["model"]
    system_prompt = prep["system_prompt"]
    assignment_map = prep["assignment_map"]
    brand_config = prep["brand_config"]
    template_config = prep["template_config"]
    brand_name = brand_config.get("name", "the brand")

    total_batches = len(prep["batches"])
    _log(f"── BATCH {batch_idx + 1}/{total_batches} ── ({len(batch)} posts, model={model})")

    # Build user prompt
    user_prompt = build_user_prompt(batch, assignment_map)
    user_prompt = user_prompt.replace("Mention ClickUp", f"Mention {brand_name}")
    user_prompt = user_prompt.replace("Do NOT mention ClickUp", f"Do NOT mention {brand_name}")

    if DEBUG:
        _log(f"  USER PROMPT ({len(user_prompt)} chars):")
        for line in user_prompt.split('\n'):
            print(f"  > {line}")

    parsed_comments = []
    error = None
    seen_post_ids = set()

    try:
        t0 = time.time()
        raw = call_llm(model, system_prompt, user_prompt, temperature=0.9)
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

        # Fill missing posts (LLM didn't return a comment for them)
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

    # Validate
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

    # Dedup within batch
    valid_in_batch = [c for c in validated if c["valid"]]
    if len(valid_in_batch) > 1:
        deduped = dedup_batch(valid_in_batch)
        for c in deduped:
            if c.get("is_duplicate"):
                c["valid"] = False
                c["checks"].append({"label": f"Duplicate ({c.get('dup_similarity', '?')})", "status": "fail"})
                _log(f"    {c['post_id'][:8]}... → DUPLICATE")

    # Apply fallbacks
    post_map = {p["id"]: p for p in batch}
    results = []
    llm_pass = 0
    fallback_used = 0
    flagged = 0

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
            fallback_text = get_fallback_comment(post, archetype, brand_mention, brand_name)
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

    batch_summary = {
        "batch_index": batch_idx,
        "posts_in_batch": len(batch),
        "llm_pass": llm_pass,
        "flagged": flagged,
        "fallback_used": fallback_used,
    }
    _log(f"  BATCH {batch_idx + 1} DONE: pass={llm_pass}, flagged={flagged}, fallback={fallback_used}")

    return {
        "results": results,
        "assignments": [assignment_map[p["id"]] for p in batch if p["id"] in assignment_map],
        "batch_summary": batch_summary,
        "error": error,
    }
