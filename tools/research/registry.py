"""Public scope registry. Names never substitute for account identities."""
import argparse
from .schema import VERSION, address, canonical, digest, load_json, text, utc

DEFAULT_ACCOUNTS = {
    "mo-money": {"account_id": "0x32ed2e546b187ca15e2841edc82b22c713cf8ec3", "lane": "public-benchmark", "purpose": "external-benchmark"},
    "mm2-public": {"account_id": "0x04ad8cb08f04b58fbcea273603d6cd151ba8adb4", "lane": "public-benchmark", "purpose": "owner-account-reconciliation"},
}


def default_registry():
    return {"schema_version": VERSION, "accounts": [{"name": name, "chain_id": 137,
        "profile_url": "https://polymarket.com/profile/" + item["account_id"],
        "identity_verified_at": None, "identity_evidence_hash": None, "scope": "one-public-account",
        **item} for name, item in DEFAULT_ACCOUNTS.items()]}


def validate_registry(value):
    if not isinstance(value, dict) or value.get("schema_version") != VERSION:
        raise ValueError("unsupported registry schema")
    accounts = value.get("accounts")
    if not isinstance(accounts, list) or not 1 <= len(accounts) <= 16:
        raise ValueError("registry requires 1..16 public accounts")
    seen = set()
    for account in accounts:
        addr = address(account.get("account_id"))
        if addr in seen:
            raise ValueError("duplicate public account")
        seen.add(addr)
        text(account.get("name"), "name", 80)
        if account.get("chain_id") != 137 or account.get("scope") != "one-public-account":
            raise ValueError("unsupported chain/scope")
        if account.get("profile_url") != "https://polymarket.com/profile/" + addr:
            raise ValueError("profile identity mismatch")
        if account.get("identity_verified_at") is not None:
            utc(account["identity_verified_at"])
            evidence = account.get("identity_evidence_hash")
            if not isinstance(evidence, str) or len(evidence) != 64 or any(c not in "0123456789abcdef" for c in evidence):
                raise ValueError("identity verification requires evidence hash")
    canonical(value)
    return value


def load_registry(path=None):
    return validate_registry(load_json(path) if path else default_registry())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry")
    args = parser.parse_args()
    registry = load_registry(args.registry)
    print(canonical({"registry": registry, "registry_hash": digest(registry)}).decode())


if __name__ == "__main__":
    main()
