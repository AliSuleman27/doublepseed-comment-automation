"""
DoubleSpeed — Supabase Adapter
Queries the real DB: frontend_product, frontend_account, frontend_post
"""
import os, json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from supabase import create_client

_client = None

def get_sb():
    global _client
    if not _client:
        url = os.environ.get("SUPABASE_URL", "")
        # Prefer service_role key (bypasses RLS), fallback to anon key
        key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip() or os.environ.get("SUPABASE_KEY", "")
        if not url or not key:
            raise RuntimeError("SUPABASE_URL and SUPABASE_KEY (or SUPABASE_SERVICE_KEY) must be set in .env")
        _client = create_client(url, key)
    return _client


def test_connection():
    """Test the Supabase connection and return diagnostics."""
    try:
        sb = get_sb()
        key = os.environ.get("SUPABASE_SERVICE_KEY", "").strip() or os.environ.get("SUPABASE_KEY", "")

        import base64
        payload = key.split('.')[1]
        payload += '=' * (4 - len(payload) % 4)
        decoded = json.loads(base64.b64decode(payload))
        role = decoded.get("role", "unknown")

        res = sb.table("frontend_product").select("id").limit(1).execute()
        row_count = len(res.data) if res.data else 0

        return {
            "ok": row_count > 0,
            "role": role,
            "products_accessible": row_count > 0,
            "message": "Connected OK" if row_count > 0 else
                f"Connected but 0 rows returned (role={role}). "
                "If role=anon, you need the service_role key in .env (SUPABASE_SERVICE_KEY)."
        }
    except Exception as e:
        return {"ok": False, "role": "unknown", "products_accessible": False, "message": str(e)}


# ─── Products (Clients) ──────────────────────────
def get_products():
    """Get all managed products (clients like ClickUp, Benjamin, etc)."""
    sb = get_sb()
    res = sb.table("frontend_product").select("id, title, organization_id, is_managed").eq("is_managed", True).execute()
    if res.data:
        return res.data
    res = sb.table("frontend_product").select("id, title, organization_id, is_managed").execute()
    return res.data or []

def get_product_by_id(product_id):
    sb = get_sb()
    res = sb.table("frontend_product").select("*").eq("id", product_id).limit(1).execute()
    return res.data[0] if res.data else None


# ─── Templates ────────────────────────────────────
def get_templates_for_product(product_id):
    """Get distinct templates used by succeeded posts of this product.
    Derives template title from the hook (first slide text) of a sample post."""
    sb = get_sb()
    # Get template_id + scene_data from recent posts to derive titles
    res = sb.table("frontend_post").select(
        "template_id, scene_data, caption, title"
    ).eq("product_id", product_id
    ).eq("status", "succeeded"
    ).order("post_time", desc=True
    ).execute()

    if not res.data:
        return []

    # Group by template_id: count + grab first sample for title derivation
    tid_counts = Counter()
    tid_sample = {}
    for row in res.data:
        tid = row.get("template_id")
        if not tid:
            continue
        tid_counts[tid] += 1
        if tid not in tid_sample:
            tid_sample[tid] = row

    if not tid_counts:
        return []

    templates = []
    for tid, count in tid_counts.items():
        sample = tid_sample[tid]
        title = _derive_template_title(sample)
        templates.append({
            "id": tid,
            "title": title,
            "post_count": count,
        })

    return sorted(templates, key=lambda t: t.get("post_count", 0), reverse=True)


def _derive_template_title(post_sample):
    """Derive a human-readable template title from a sample post's hook text."""
    # Try to get the hook (first text block from first page of scene_data)
    sd = _parse_json(post_sample.get("scene_data"))
    if sd:
        hook = _extract_hook(sd)
        if hook:
            # Truncate to a readable label
            return _truncate(hook, 60)

    # Fallback: use caption
    caption = post_sample.get("caption") or post_sample.get("title") or ""
    if caption:
        return _truncate(caption, 60)

    return f"Template {post_sample.get('template_id', '?')[:8]}"


def _extract_hook(scene):
    """Extract the hook text — first text block from first page of scene_data."""
    pages = scene.get("pages", [])
    if not pages:
        return None
    # Respect page order if available
    page_order = scene.get("pageOrder", [])
    if page_order:
        page_map = {p["id"]: p for p in pages}
        first_page = page_map.get(page_order[0], pages[0])
    else:
        first_page = pages[0]

    for block in first_page.get("blocks", []):
        if block.get("type") == "text" and block.get("text"):
            return block["text"].strip()
    return None


def _truncate(text, maxlen=60):
    """Truncate text to maxlen chars with ellipsis."""
    text = text.replace("\n", " ").strip()
    if len(text) <= maxlen:
        return text
    return text[:maxlen-1].rstrip() + "…"


# ─── Accounts ─────────────────────────────────────
def get_accounts_for_product(product_id):
    """Get all tiktok accounts for this product."""
    sb = get_sb()
    res = sb.table("frontend_account").select(
        "id, username, status, tag, name, gender"
    ).eq("product_id", product_id).eq("account_type", "tiktok").execute()
    return res.data or []


# ─── Posts ─────────────────────────────────────────
def get_posts(product_id, template_id, hours_back=24):
    """Get succeeded posts for a product+template in the last N hours."""
    sb = get_sb()
    since = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()

    res = sb.table("frontend_post").select(
        "id, account_id, template_id, title, tiktok_post_id, caption, "
        "scene_data, template_data, status, post_time, num_slides, type, "
        "succeeded_at, product_id"
    ).eq("product_id", product_id
    ).eq("template_id", template_id
    ).eq("status", "succeeded"
    ).gte("post_time", since
    ).order("post_time", desc=True
    ).execute()

    posts = res.data or []
    return _enrich_posts(sb, posts)


def get_all_posts_for_product(product_id, template_id=None, hours_back=72):
    """Get all succeeded posts, optionally filtered by template.
    hours_back=0 means all time (no time filter)."""
    sb = get_sb()

    query = sb.table("frontend_post").select(
        "id, account_id, template_id, title, tiktok_post_id, caption, "
        "scene_data, template_data, status, post_time, num_slides, type, "
        "succeeded_at, product_id"
    ).eq("product_id", product_id
    ).eq("status", "succeeded"
    ).order("post_time", desc=True
    ).limit(200)

    if hours_back and hours_back > 0:
        since = (datetime.now(timezone.utc) - timedelta(hours=hours_back)).isoformat()
        query = query.gte("post_time", since)

    if template_id:
        query = query.eq("template_id", template_id)

    res = query.execute()
    posts = res.data or []
    return _enrich_posts(sb, posts)


def _enrich_posts(sb, posts):
    """Add username, tiktok_url, hook, and slide_texts to posts."""
    if not posts:
        return posts

    account_ids = list(set(p["account_id"] for p in posts if p.get("account_id")))
    acct_map = {}
    if account_ids:
        a_res = sb.table("frontend_account").select("id, username").in_("id", account_ids).execute()
        if a_res.data:
            for a in a_res.data:
                acct_map[a["id"]] = a["username"]

    for p in posts:
        p["username"] = acct_map.get(p["account_id"], "unknown")
        p["tiktok_url"] = (
            f"https://www.tiktok.com/@{p['username']}/photo/{p['tiktok_post_id']}"
            if p.get("tiktok_post_id") else None
        )
        p["slide_texts"] = extract_texts(p)
        p["hook"] = _get_post_hook(p)

    return posts


def _get_post_hook(post):
    """Extract the post hook — the first text from the first slide of scene_data."""
    sd = _parse_json(post.get("scene_data"))
    if sd:
        hook = _extract_hook(sd)
        if hook:
            return hook
    # Fallback to caption
    return post.get("caption") or post.get("title") or ""


# ─── Text Extraction from scene_data ──────────────
def extract_texts(post: dict) -> list[str]:
    """Extract all text blocks from scene_data (actual posted content).
    Falls back to template_data caption if scene_data has no text."""
    texts = []

    scene_raw = post.get("scene_data")
    if scene_raw:
        scene = _parse_json(scene_raw)
        if scene:
            texts = _extract_from_scene(scene)

    if not texts:
        td_raw = post.get("template_data")
        if td_raw:
            td = _parse_json(td_raw)
            if td and td.get("caption"):
                texts.append(td["caption"])

    if not texts:
        if post.get("caption"):
            texts.append(post["caption"])
        elif post.get("title"):
            texts.append(post["title"])

    return texts


def _extract_from_scene(scene: dict) -> list[str]:
    """Extract text from scene_data.pages[].blocks[] where type='text'.
    Respects pageOrder if available."""
    texts = []
    pages = scene.get("pages", [])
    page_order = scene.get("pageOrder", [])

    # Order pages correctly if pageOrder exists
    if page_order:
        page_map = {p["id"]: p for p in pages}
        ordered = [page_map[pid] for pid in page_order if pid in page_map]
        if ordered:
            pages = ordered

    for page in pages:
        for block in page.get("blocks", []):
            if block.get("type") == "text" and block.get("text"):
                texts.append(block["text"].strip())

    if not texts:
        slides = scene.get("slides", [])
        for slide in slides:
            for item in slide.get("items", []):
                if item.get("type") == "text" and item.get("text"):
                    texts.append(item["text"].strip())

    return texts


def _parse_json(raw):
    """Safely parse JSON string or return dict if already parsed."""
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return None
    return None
