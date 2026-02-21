"""
DoubleSpeed Comment Engine — Flask App (Supabase-backed)

Flow:
  1. Pick product (client) → loads templates
  2. Pick template → fetches succeeded posts (last N hours)
  3. View posts: tiktok link, slide texts, caption, account
  4. Export as CSV
  5. Comment Engine: upload config → generate AI comments → validate → export
"""
import os, sys, csv, io, json, traceback
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response, stream_with_context
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.supabase_adapter import (
    get_products,
    get_templates_for_product, get_accounts_for_product,
    get_all_posts_for_product, test_connection,
)
from services.comment_engine import run_pipeline, prepare_pipeline, process_batch

app = Flask(__name__)

_session = {"product_id": None, "template_id": None, "posts": []}
_ce_config = {}  # Uploaded brand+template config
_ce_prep = {}    # Prepared pipeline state for batch streaming


def _resolve_template(templates: dict, template_slug: str) -> dict | None:
    """Find matching template config by slug (exact, fuzzy, or first fallback)."""
    tc = templates.get(template_slug)
    if tc:
        return tc
    for slug, tc in templates.items():
        if slug in template_slug or template_slug in slug:
            return tc
    if templates:
        return next(iter(templates.values()))
    return None


@app.route("/")
def index():
    products = get_products()
    return render_template("index.html", products=products)


@app.route("/api/test")
def api_test():
    return jsonify(test_connection())


@app.route("/api/products")
def api_products():
    try:
        return jsonify(get_products())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/products/<product_id>/templates")
def api_templates(product_id):
    try:
        return jsonify(get_templates_for_product(product_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/products/<product_id>/accounts")
def api_accounts(product_id):
    try:
        return jsonify(get_accounts_for_product(product_id))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/posts/fetch", methods=["POST"])
def api_fetch_posts():
    data = request.json
    product_id = data.get("product_id")
    template_id = data.get("template_id")
    start_date = data.get("start_date")
    end_date = data.get("end_date")
    statuses = data.get("statuses", ["succeeded"])

    if not product_id:
        return jsonify({"error": "product_id required"}), 400

    try:
        posts = get_all_posts_for_product(product_id, template_id, start_date, end_date, statuses=statuses)
    except Exception as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500

    _session["product_id"] = product_id
    _session["template_id"] = template_id
    _session["posts"] = posts

    results = []
    for p in posts:
        results.append({
            "id": p["id"],
            "account_username": p.get("username", ""),
            "tiktok_url": p.get("tiktok_url"),
            "tiktok_post_id": p.get("tiktok_post_id"),
            "caption": p.get("caption", ""),
            "title": p.get("title", ""),
            "hook": p.get("hook", ""),
            "slide_texts": p.get("slide_texts", []),
            "num_slides": p.get("num_slides"),
            "post_time": p.get("post_time"),
            "template_id": p.get("template_id"),
            "template_name": p.get("template_name", ""),
            "client_name": p.get("client_name", ""),
            "scene_data": p.get("scene_data"),
            "created_at": p.get("created_at", ""),
            "status": p.get("status"),
        })

    return jsonify({"posts": results, "count": len(results)})


@app.route("/api/posts/export", methods=["POST"])
def api_export():
    posts = _session.get("posts", [])
    if not posts:
        return jsonify({"error": "No posts to export. Fetch first."}), 400

    output = io.StringIO()
    fields = ["account_username", "tiktok_url", "caption", "slide_texts", "num_slides", "post_time"]
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    for p in posts:
        writer.writerow({
            "account_username": p.get("username", ""),
            "tiktok_url": p.get("tiktok_url", ""),
            "caption": p.get("caption", ""),
            "slide_texts": " | ".join(p.get("slide_texts", [])),
            "num_slides": p.get("num_slides", ""),
            "post_time": p.get("post_time", ""),
        })

    pid = _session.get("product_id", "export")[:8]
    filename = f"posts_{pid}_{datetime.now().strftime('%Y-%m-%d_%H%M')}.csv"
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename={filename}"})


# ═══════════════════════════════════════════════
# COMMENT ENGINE API
# ═══════════════════════════════════════════════

@app.route("/api/ce/config", methods=["POST"])
def api_ce_upload_config():
    """Upload brand+template config JSON."""
    try:
        if request.content_type and "multipart" in request.content_type:
            f = request.files.get("config")
            if not f:
                return jsonify({"error": "No file uploaded"}), 400
            raw = f.read().decode("utf-8")
            config = json.loads(raw)
        else:
            config = request.json

        if not config:
            return jsonify({"error": "Empty config"}), 400

        # Validate structure
        if "brand" not in config or "templates" not in config:
            return jsonify({"error": "Config must have 'brand' and 'templates' keys"}), 400

        _ce_config.clear()
        _ce_config.update(config)

        template_names = list(config["templates"].keys())
        return jsonify({
            "ok": True,
            "brand": config["brand"].get("name", "unknown"),
            "templates": template_names,
        })
    except json.JSONDecodeError as e:
        return jsonify({"error": f"Invalid JSON: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/ce/config", methods=["GET"])
def api_ce_get_config():
    """Get the currently loaded config."""
    if not _ce_config:
        return jsonify({"loaded": False})
    return jsonify({
        "loaded": True,
        "brand": _ce_config.get("brand", {}).get("name", "unknown"),
        "templates": list(_ce_config.get("templates", {}).keys()),
    })


@app.route("/api/ce/config/full", methods=["GET"])
def api_ce_config_full():
    """Get the full config for the viewer modal."""
    if not _ce_config:
        return jsonify({"loaded": False})
    return jsonify({"loaded": True, "config": _ce_config})


@app.route("/api/ce/config/detail", methods=["GET"])
def api_ce_config_detail():
    """Get full config detail so UI can show archetype weights, relevance ratio, etc."""
    if not _ce_config:
        return jsonify({"loaded": False})

    slug = request.args.get("template_slug", "")
    templates = _ce_config.get("templates", {})
    tc = _resolve_template(templates, slug) if slug else None
    if not tc and templates:
        tc = next(iter(templates.values()))

    brand = _ce_config.get("brand", {})

    return jsonify({
        "loaded": True,
        "brand_name": brand.get("name", "unknown"),
        "preferred_casing": brand.get("preferred_casing", {}),
        "archetype_weights": tc.get("archetype_weights", {}) if tc else {},
        "relevance_ratio": tc.get("relevance_ratio", 0.5) if tc else 0.5,
        "comment_rules": tc.get("comment_rules", {}) if tc else {},
        "golden_comments_count": len(tc.get("golden_comments", [])) if tc else 0,
        "anti_examples_count": len(tc.get("anti_examples", [])) if tc else 0,
    })


@app.route("/api/ce/generate", methods=["POST"])
def api_ce_generate():
    """Run the full comment generation pipeline.

    Body: {
        "posts": [...],
        "template_slug": "9-5",
        "model": "claude-haiku",
        "batch_size": 8,
        "overrides": { ... }     // optional UI overrides
    }
    """
    if not _ce_config:
        return jsonify({"error": "No config uploaded. Upload a brand config first."}), 400

    data = request.json
    posts = data.get("posts", [])
    template_slug = data.get("template_slug", "")
    model = data.get("model", "claude-haiku")
    batch_size = data.get("batch_size", 8)
    overrides = data.get("overrides", {})

    if not posts:
        return jsonify({"error": "No posts provided"}), 400

    brand_config = _ce_config.get("brand", {})
    templates = _ce_config.get("templates", {})

    template_config = _resolve_template(templates, template_slug)
    if not template_config:
        return jsonify({"error": f"No template config found for '{template_slug}'"}), 400

    try:
        result = run_pipeline(
            posts=posts,
            brand_config=brand_config,
            template_config=template_config,
            model=model,
            batch_size=batch_size,
            overrides=overrides,
        )
        return jsonify(result)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[CE] Pipeline error: {e}\n{tb}")
        return jsonify({"error": f"Pipeline error: {str(e)}"}), 500


@app.route("/api/ce/prepare", methods=["POST"])
def api_ce_prepare():
    """Prepare the pipeline: assign archetypes, tag relevance, build prompts.
    Returns batch count, assignments, and relevance tags.
    Accepts overrides from UI controls.
    """
    if not _ce_config:
        return jsonify({"error": "No config uploaded. Upload a brand config first."}), 400

    data = request.json
    posts = data.get("posts", [])
    template_slug = data.get("template_slug", "")
    model = data.get("model", "claude-haiku")
    batch_size = data.get("batch_size", 8)
    overrides = data.get("overrides", {})

    if not posts:
        return jsonify({"error": "No posts provided"}), 400

    brand_config = _ce_config.get("brand", {})
    templates = _ce_config.get("templates", {})

    template_config = _resolve_template(templates, template_slug)
    if not template_config:
        return jsonify({"error": f"No template config found for '{template_slug}'"}), 400

    try:
        prep = prepare_pipeline(
            posts=posts,
            brand_config=brand_config,
            template_config=template_config,
            model=model,
            batch_size=batch_size,
            overrides=overrides,
        )
        _ce_prep.clear()
        _ce_prep.update(prep)

        return jsonify({
            "ok": True,
            "total_batches": len(prep["batches"]),
            "total_posts": len(posts),
            "assignments": prep["assignments"],
            "relevance_tags": prep.get("relevance_tags", {}),
        })
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[CE] Prepare error: {e}\n{tb}")
        return jsonify({"error": f"Prepare error: {str(e)}"}), 500


@app.route("/api/ce/batch/<int:batch_idx>", methods=["POST"])
def api_ce_process_batch(batch_idx):
    """Process a single batch and return its results."""
    if not _ce_prep:
        return jsonify({"error": "Pipeline not prepared. Call /api/ce/prepare first."}), 400

    total_batches = len(_ce_prep.get("batches", []))
    if batch_idx < 0 or batch_idx >= total_batches:
        return jsonify({"error": f"Invalid batch index {batch_idx}. Total batches: {total_batches}"}), 400

    try:
        result = process_batch(_ce_prep, batch_idx)
        return jsonify(result)
    except Exception as e:
        tb = traceback.format_exc()
        print(f"[CE] Batch {batch_idx} error: {e}\n{tb}")
        return jsonify({"error": f"Batch error: {str(e)}"}), 500


if __name__ == "__main__":
    diag = test_connection()
    if diag["ok"]:
        print(f"\n  ✓ Supabase connected (role={diag['role']})\n")
    else:
        print(f"\n  ✗ Supabase issue: {diag['message']}\n")
    app.run(debug=True, host="0.0.0.0", port=5050)
