"""Trusted, fenced, durable issue publication; unknown POST is never repeated."""
import re
from .github import repository, request_hash, remote_request
from .schema import ResearchError


def _receipt(effect, remote):
    if "pull_request" in remote or type(remote.get("number")) is not int:
        raise ResearchError("remote marker matched a non-issue object")
    expected_url = f"https://github.com/{effect['repository']}/issues/{remote['number']}"
    if remote.get("html_url") != expected_url:
        raise ResearchError("remote issue repository/identity mismatch")
    if f"<!-- {effect['marker']} -->" not in (remote.get("body") or ""):
        raise ResearchError("remote effect marker absent")
    if request_hash(remote_request(remote)) != request_hash(effect["body"]):
        raise ResearchError("remote issue body differs from the exact prepared request")
    return str(remote["number"]), expected_url


def reconcile(store, client, effect_id):
    effect = store.outbox(effect_id)
    if not effect:
        raise ResearchError("unknown outbox effect")
    repository(effect["repository"])
    if client.repo != effect["repository"]:
        raise ResearchError("broker transport repository differs from durable intent")
    matches = [item for item in client.all_issues()
               if f"<!-- {effect['marker']} -->" in (item.get("body") or "")]
    if len(matches) > 1:
        raise ResearchError("duplicate remote markers require owner reconciliation")
    if not matches:
        # Search/list absence, including eventual visibility, does not authorize a retry.
        return {"status": "WAITING_ACCESS" if effect["status"] in {"UNKNOWN", "DISPATCHING"} else "NOT_FOUND",
                "effect_id": effect_id, "blind_retry_allowed": False}
    remote_id, url = _receipt(effect, matches[0])
    result = store.confirm_publication(effect_id, remote_id, url, effect["body_hash"])
    return {"status": "CONFIRMED", "effect_id": effect_id, "issue_url": result["remote_url"]}


def publish(store, client, ticket, effect_id):
    """Scope/body validated before preparation; current lease rechecked before POST."""
    effect = store.outbox(effect_id)
    if not effect:
        raise ResearchError("unknown outbox effect")
    repository(effect["repository"])
    if client.repo != effect["repository"]:
        raise ResearchError("broker transport repository mismatch")
    if effect["status"] == "CONFIRMED":
        return {"status": "CONFIRMED", "issue_url": effect["remote_url"], "effect_id": effect_id}
    reconciled = reconcile(store, client, effect_id)
    if reconciled["status"] != "NOT_FOUND":
        return reconciled
    # Do not dispatch stale worker output just because remote reconciliation is empty.
    effect = store.dispatch_publication(ticket, effect_id, timeout=client.timeout)
    try:
        remote = client.create_issue(effect["body"])
        remote_id, url = _receipt(effect, remote)
        confirmed = store.confirm_publication(effect_id, remote_id, url, effect["body_hash"])
        return {"status": "CONFIRMED", "issue_url": confirmed["remote_url"], "effect_id": effect_id}
    except Exception:
        # A 4xx, process failure and timeout all remain conservative until reconciled.
        store.publication_unknown(effect_id)
        return {"status": "UNKNOWN", "effect_id": effect_id, "blind_retry_allowed": False}
