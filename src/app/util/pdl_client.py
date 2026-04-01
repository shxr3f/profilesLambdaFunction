from peopledatalabs import PDLPY


def build_pdl_client(api_key: str) -> PDLPY:
    if not api_key:
        raise ValueError("PDL_API_KEY is not set")
    return PDLPY(api_key=api_key)


def enrich_person(client: PDLPY, first_name: str | None, last_name: str | None) -> dict:
    conditions = []

    if first_name and first_name.strip():
        conditions.append(f"first_name='{first_name.strip()}'")

    if last_name and last_name.strip():
        conditions.append(f"last_name='{last_name.strip()}'")

    sql_query = f"""
    SELECT * FROM person
    WHERE {" AND ".join(conditions)}
    """

    params = {
        "sql": sql_query,
        "size": 1,
        "pretty": True
    }

    response = client.person.search(**params)

    try:
        body = response.json()
    except Exception:
        body = {"raw_text": getattr(response, "text", "")}

    return {
        "status_code": getattr(response, "status_code", None),
        "ok": getattr(response, "ok", False),
        "body": body,
    }