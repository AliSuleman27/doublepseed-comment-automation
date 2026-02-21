"""
DoubleSpeed — Auth Adapter (Supabase Auth, separate instance)

Uses AUTH_SUPABASE_URL + AUTH_SUPABASE_KEY for authentication only.
The data Supabase (SUPABASE_URL + SUPABASE_SERVICE_KEY) is not touched.
"""
import os
from supabase import create_client

_auth_client = None


def get_auth_sb():
    """Lazy-init Auth Supabase client (separate from data Supabase)."""
    global _auth_client
    if not _auth_client:
        url = os.environ.get("AUTH_SUPABASE_URL", "").strip().strip('"')
        key = os.environ.get("AUTH_SUPABASE_KEY", "").strip().strip('"')
        if not url or not key:
            raise RuntimeError("AUTH_SUPABASE_URL and AUTH_SUPABASE_KEY must be set in .env")
        _auth_client = create_client(url, key)
    return _auth_client


def sign_in(email: str, password: str) -> dict:
    """Sign in with email+password. Returns {access_token, refresh_token, user}."""
    sb = get_auth_sb()
    response = sb.auth.sign_in_with_password({"email": email, "password": password})
    if not response.session:
        raise ValueError("Login failed: no session returned")
    session = response.session
    user = response.user or session.user
    return {
        "access_token": session.access_token,
        "refresh_token": session.refresh_token,
        "expires_at": session.expires_at,
        "user": {"id": str(user.id), "email": user.email},
    }


def sign_up(email: str, password: str) -> dict:
    """Create account with email+password."""
    sb = get_auth_sb()
    response = sb.auth.sign_up({"email": email, "password": password})
    if not response.session:
        user = response.user
        return {
            "access_token": None,
            "refresh_token": None,
            "user": {"id": str(user.id), "email": user.email} if user else None,
            "message": "Check email for confirmation link",
        }
    session = response.session
    user = response.user or session.user
    return {
        "access_token": session.access_token,
        "refresh_token": session.refresh_token,
        "expires_at": session.expires_at,
        "user": {"id": str(user.id), "email": user.email},
    }


def verify_token(access_token: str) -> dict | None:
    """Verify an access_token by calling Supabase auth.get_user(token).
    Returns user dict {id, email} or None if invalid."""
    sb = get_auth_sb()
    try:
        response = sb.auth.get_user(jwt=access_token)
        if response and response.user:
            return {"id": str(response.user.id), "email": response.user.email}
        return None
    except Exception:
        return None


def refresh_session(refresh_token: str) -> dict:
    """Exchange a refresh_token for a new access_token."""
    sb = get_auth_sb()
    response = sb.auth.refresh_session(refresh_token)
    if not response.session:
        raise ValueError("Refresh failed: no session returned")
    session = response.session
    return {
        "access_token": session.access_token,
        "refresh_token": session.refresh_token,
        "expires_at": session.expires_at,
    }


def sign_out(access_token: str) -> None:
    """Invalidate the session server-side (best-effort)."""
    sb = get_auth_sb()
    try:
        sb.auth.admin.sign_out(access_token, "global")
    except Exception:
        pass
