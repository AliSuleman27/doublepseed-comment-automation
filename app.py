"""
DoubleSpeed Comment Engine — Flask App (Supabase-backed)

Flow:
  1. Pick product (client) → loads templates
  2. Pick template → fetches succeeded posts (last N hours)
  3. View posts: tiktok link, slide texts, caption, account
  4. Export as CSV
"""
import os, sys, csv, io
from datetime import datetime
from flask import Flask, render_template, request, jsonify, Response
from dotenv import load_dotenv

load_dotenv()
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from services.supabase_adapter import (
    get_products,
    get_templates_for_product, get_accounts_for_product,
    get_all_posts_for_product, test_connection,
)

app = Flask(__name__)

_session = {"product_id": None, "template_id": None, "posts": []}


@app.route("/")
def index():
    products = get_products()
    return render_template("index.html", products=products)


@app.route("/api/test")
def api_test():
    """Diagnostic endpoint to test Supabase connection."""
    return jsonify(test_connection())


@app.route("/api/products")
def api_products():
    try:
        products = get_products()
        return jsonify(products)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/products/<product_id>/templates")
def api_templates(product_id):
    try:
        templates = get_templates_for_product(product_id)
        return jsonify(templates)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/products/<product_id>/accounts")
def api_accounts(product_id):
    try:
        accounts = get_accounts_for_product(product_id)
        return jsonify(accounts)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/posts/fetch", methods=["POST"])
def api_fetch_posts():
    data = request.json
    product_id = data.get("product_id")
    template_id = data.get("template_id")
    start_date = data.get("start_date")
    end_date = data.get("end_date")

    if not product_id:
        return jsonify({"error": "product_id required"}), 400

    try:
        posts = get_all_posts_for_product(product_id, template_id, start_date, end_date)
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

    return jsonify({
        "posts": results,
        "count": len(results),
    })


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


if __name__ == "__main__":
    # Run connection test at startup
    diag = test_connection()
    if diag["ok"]:
        print(f"\n  ✓ Supabase connected (role={diag['role']})\n")
    else:
        print(f"\n  ✗ Supabase issue: {diag['message']}\n")
    app.run(debug=True, host="0.0.0.0", port=5050)
