from peopledatalabs import PDLPY


def build_pdl_client(api_key: str) -> PDLPY:
    if not api_key:
        raise ValueError("PDL_API_KEY is not set")
    return PDLPY(api_key=api_key)


def enrich_person(client: PDLPY, name: str | None, email: str | None) -> dict:
    params = {}

    if name and name.strip():
        params["name"] = name.strip()

    if email and email.strip():
        params["email"] = email.strip()

    if not params:
        return {
            "status": "skipped",
            "reason": "missing both name and email",
        }

    response = client.person.enrichment(**params)

    try:
        body = response.json()
    except Exception:
        body = {"raw_text": getattr(response, "text", "")}

    return {
        "status_code": getattr(response, "status_code", None),
        "ok": getattr(response, "ok", False),
        "body": body,
    }