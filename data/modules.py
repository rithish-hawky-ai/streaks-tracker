from urllib.parse import urlparse

URL_MODULE = {
    "analysis": "Creative Intel",
    "competitor": "Competitive Intel",
    "production": "Production+Playbooks",
    "playbook": "Production+Playbooks",
    "agents": "Agents",
}
COPILOT_MODULE = "Co-Pilot"
ALL_MODULES = [
    "Creative Intel",
    "Competitive Intel",
    "Production+Playbooks",
    "Co-Pilot",
    "Agents",
]

COPILOT_NAME_WHITELIST = {
    "Global Copilot Message Submitted",
}


def is_copilot_event(event_name: str) -> bool:
    if not event_name:
        return False
    if event_name in COPILOT_NAME_WHITELIST:
        return True
    return event_name.lower().startswith("copilot_")


def parse_url_module(current_url: str):
    """
    Returns (module, sub_page) from a $current_url.
    - module = first path segment after /app/, mapped via URL_MODULE.
    - sub_page = literal second segment after /app/<x>/ (preserved verbatim,
      including the word 'dashboard' when present).
    """
    if not current_url:
        return None, None
    try:
        path = urlparse(current_url).path or ""
    except Exception:
        return None, None
    parts = [p for p in path.split("/") if p]
    if len(parts) < 2 or parts[0].lower() != "app":
        return None, None
    first = parts[1].lower()
    module = URL_MODULE.get(first)
    sub = parts[2] if len(parts) >= 3 else None
    return module, sub


def attribute_event(event_name: str, current_url: str):
    """
    Returns (modules_credited: list[str], sub_page: str | None).

    Co-Pilot events DOUBLE-COUNT per resolved decision: they credit Co-Pilot AND
    the URL-derived module if one exists. URL-derived module without a Co-Pilot
    event signal just credits the URL module.
    """
    modules = []
    url_module, sub = parse_url_module(current_url)
    if url_module:
        modules.append(url_module)
    if is_copilot_event(event_name) and COPILOT_MODULE not in modules:
        modules.append(COPILOT_MODULE)
    return modules, sub
