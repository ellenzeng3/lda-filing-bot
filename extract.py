def get_uuid(filing):
    return filing.get("filing_uuid")

def get_filing_date(filing):
    return filing.get("dt_posted")

def get_filing_year(filing):
    return filing.get("filing_year")

def get_filing_period(filing):
    return filing.get("filing_period")

def get_registrant_name(filing):
    return filing.get("registrant", {}).get("name")

def get_client_name(filing):
    return filing.get("client", {}).get("name")

def get_lobbying_descriptions(filing):
    return ", ".join(
        [a.get("description") for a in filing.get("lobbying_activities", []) if a.get("description")]
    )

def get_income(filing):
    return filing.get("income")

def get_expenses(filing):
    return filing.get("expenses")

def get_filing_document_url(filing):
    return filing.get("filing_document_url")

def get_lobbyist_names(filing):
    return ", ".join(
        sorted({
            f"{l.get('lobbyist', {}).get('first_name').title()} {l.get('lobbyist', {}).get('last_name').title()}"
            for a in filing.get("lobbying_activities", [])
            for l in a.get("lobbyists", [])
            if l.get("lobbyist", {}).get("first_name") and l.get("lobbyist", {}).get("last_name")
        })) 