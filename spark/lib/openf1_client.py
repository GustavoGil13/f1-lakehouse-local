# spark/lib/openf1_client.py
from __future__ import annotations

import json
import time
from typing import Any, Dict, Optional, Tuple

import requests


class OpenF1ClientError(RuntimeError):
    pass


def _should_retry(status_code: int) -> bool:
    # Typical retryable responses
    return status_code in (429, 500, 502, 503, 504)


def fetch_json(
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    *,
    timeout_s: int = 30,
    max_retries: int = 5,
    backoff_base_s: float = 1.0,
    user_agent: str = "f1-lakehouse-local/bronze-ingest",
) -> Tuple[str, Dict[str, Any], int, Any]:
    """
    Fetch JSON from OpenF1.

    Returns: (url, params_used, http_status, parsed_json)
    """
    base = "https://api.openf1.org/v1"
    endpoint = endpoint.strip().lstrip("/")
    url = f"{base}/{endpoint}"

    params_used: Dict[str, Any] = dict(params or {})

    headers = {"User-Agent": user_agent}

    last_exc: Optional[Exception] = None

    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, params=params_used, timeout=timeout_s, headers=headers)

            if resp.status_code == 200:
                try:
                    return url, params_used, resp.status_code, resp.json()
                except json.JSONDecodeError as e:
                    raise OpenF1ClientError(f"Invalid JSON from {url}: {e}") from e

            # Non-200
            if _should_retry(resp.status_code) and attempt < max_retries:
                sleep_s = backoff_base_s * (2 ** attempt)
                # If API sends a Retry-After header, respect it if possible
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = max(sleep_s, float(ra))
                    except ValueError:
                        pass
                time.sleep(sleep_s)
                continue

            # Not retryable or retries exhausted
            body_preview = (resp.text or "")[:500]
            raise OpenF1ClientError(
                f"Request failed: {url} params={params_used} status={resp.status_code} body={body_preview}"
            )

        except (requests.Timeout, requests.ConnectionError) as e:
            last_exc = e
            if attempt < max_retries:
                time.sleep(backoff_base_s * (2 ** attempt))
                continue
            raise OpenF1ClientError(f"Network error after retries: {url} params={params_used} err={e}") from e

        except Exception as e:
            # Unknown error – no retry by default
            last_exc = e
            raise

    # Should never reach here
    raise OpenF1ClientError(f"Unexpected failure: {url} params={params_used} err={last_exc}")
