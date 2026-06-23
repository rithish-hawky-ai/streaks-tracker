def email_domain(email: str):
    if not email or "@" not in email:
        return None
    return email.rsplit("@", 1)[-1].strip().lower()


def _rule_matches(rule, email_lower: str) -> bool:
    op = (rule.get("operator") or "").strip().lower()
    val = (rule.get("value") or "").strip().lower()
    if not val:
        return False
    if op == "contains":      return val in email_lower
    if op == "not_contains":  return val not in email_lower
    if op == "starts_with":   return email_lower.startswith(val)
    if op == "ends_with":     return email_lower.endswith(val)
    if op == "equals":        return email_lower == val
    return False


def is_hawky_email(email: str, exclusion_domains, allow_list, deny_list,
                   substring_terms=(), rules=()) -> bool:
    """
    True when the email should be EXCLUDED (Hawky internal user).

    Resolution order (first match wins):
      1. allow_list (per-email)  → NEVER exclude (force include).
      2. deny_list  (per-email)  → EXCLUDE.
      3. Pattern rules in id order → kind determines verdict, stop on first match.
      4. Domain matches exclusion_domains (strict equality) → EXCLUDE.
      5. Email contains any term in substring_terms (legacy) → EXCLUDE.

    Per-email overrides win over rules so you can carve exceptions out of a
    broad rule (e.g. rule "contains hawky → deny" + allow `hawky.io@partner.com`).
    """
    if not email:
        return False
    e = email.strip().lower()
    if e in allow_list:
        return False
    if e in deny_list:
        return True
    for rule in (rules or ()):
        if _rule_matches(rule, e):
            return (rule.get("kind") or "").strip().lower() == "deny"
    if email_domain(e) in exclusion_domains:
        return True
    for term in substring_terms:
        t = (term or "").strip().lower()
        if t and t in e:
            return True
    return False
