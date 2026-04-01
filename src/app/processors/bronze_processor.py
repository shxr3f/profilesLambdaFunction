import uuid
from datetime import datetime, timezone

from app.config import DATA_LAKE_BUCKET, BRONZE_PREFIX
from app.util.s3_io import read_json, write_csv_rows


def flatten_person(person: dict, partition_date: str) -> dict:
    return {
        "date": partition_date,
        "person_id": person.get("id"),
        "full_name": person.get("full_name"),
        "first_name": person.get("first_name"),
        "middle_initial": person.get("middle_initial"),
        "middle_name": person.get("middle_name"),
        "last_initial": person.get("last_initial"),
        "last_name": person.get("last_name"),
        "sex": person.get("sex"),
        "linkedin_url": person.get("linkedin_url"),
        "linkedin_username": person.get("linkedin_username"),
        "linkedin_id": person.get("linkedin_id"),
        "facebook_url": person.get("facebook_url"),
        "facebook_username": person.get("facebook_username"),
        "facebook_id": person.get("facebook_id"),
        "twitter_url": person.get("twitter_url"),
        "twitter_username": person.get("twitter_username"),
        "github_url": person.get("github_url"),
        "github_username": person.get("github_username"),
        "industry": person.get("industry"),
        "job_title": person.get("job_title"),
        "job_title_role": person.get("job_title_role"),
        "job_title_sub_role": person.get("job_title_sub_role"),
        "job_title_class": person.get("job_title_class"),
        "job_title_levels": person.get("job_title_levels", []),
        "job_company_id": person.get("job_company_id"),
        "job_company_name": person.get("job_company_name"),
        "job_company_website": person.get("job_company_website"),
        "job_company_size": person.get("job_company_size"),
        "job_company_founded": person.get("job_company_founded"),
        "job_company_industry": person.get("job_company_industry"),
        "job_company_industry_v2": person.get("job_company_industry_v2"),
        "job_company_linkedin_url": person.get("job_company_linkedin_url"),
        "job_company_linkedin_id": person.get("job_company_linkedin_id"),
        "job_company_facebook_url": person.get("job_company_facebook_url"),
        "job_company_twitter_url": person.get("job_company_twitter_url"),
        "job_company_location_name": person.get("job_company_location_name"),
        "job_company_location_locality": person.get("job_company_location_locality"),
        "job_company_location_metro": person.get("job_company_location_metro"),
        "job_company_location_region": person.get("job_company_location_region"),
        "job_company_location_geo": person.get("job_company_location_geo"),
        "job_company_location_street_address": person.get("job_company_location_street_address"),
        "job_company_location_address_line_2": person.get("job_company_location_address_line_2"),
        "job_company_location_postal_code": person.get("job_company_location_postal_code"),
        "job_company_location_country": person.get("job_company_location_country"),
        "job_company_location_continent": person.get("job_company_location_continent"),
        "job_last_changed": person.get("job_last_changed"),
        "job_last_verified": person.get("job_last_verified"),
        "job_start_date": person.get("job_start_date"),
        "location_country": person.get("location_country"),
        "location_continent": person.get("location_continent"),
        "location_address_line_2": person.get("location_address_line_2"),
        "location_last_updated": person.get("location_last_updated"),
        "interests": person.get("interests", []),
        "skills": person.get("skills", []),
        "countries": person.get("countries", []),
        "dataset_version": person.get("dataset_version"),
        "birth_date": person.get("birth_date"),
        "birth_year": person.get("birth_year"),
        "emails": person.get("emails"),
        "location_geo": person.get("location_geo"),
        "location_locality": person.get("location_locality"),
        "location_metro": person.get("location_metro"),
        "location_name": person.get("location_name"),
        "location_names": person.get("location_names"),
        "location_postal_code": person.get("location_postal_code"),
        "location_region": person.get("location_region"),
        "location_street_address": person.get("location_street_address"),
        "mobile_phone": person.get("mobile_phone"),
        "personal_emails": person.get("personal_emails"),
        "phone_numbers": person.get("phone_numbers"),
        "recommended_personal_email": person.get("recommended_personal_email"),
        "regions": person.get("regions"),
        "street_addresses": person.get("street_addresses"),
        "work_email": person.get("work_email"),
    }


def flatten_experience(person: dict, partition_date: str) -> list[dict]:
    rows = []
    person_id = person.get("id")

    for idx, exp in enumerate(person.get("experience", [])):
        company = exp.get("company", {}) or {}
        title = exp.get("title", {}) or {}

        rows.append({
            "date": partition_date,
            "person_id": person_id,
            "experience_index": idx,
            "company_name": company.get("name"),
            "company_size": company.get("size"),
            "company_id": company.get("id"),
            "company_founded": company.get("founded"),
            "company_industry": company.get("industry"),
            "company_industry_v2": company.get("industry_v2"),
            "company_location": company.get("location"),
            "company_linkedin_url": company.get("linkedin_url"),
            "company_linkedin_id": company.get("linkedin_id"),
            "company_facebook_url": company.get("facebook_url"),
            "company_twitter_url": company.get("twitter_url"),
            "company_website": company.get("website"),
            "location_names": exp.get("location_names", []),
            "end_date": exp.get("end_date"),
            "start_date": exp.get("start_date"),
            "title_name": title.get("name"),
            "title_class": title.get("class"),
            "title_role": title.get("role"),
            "title_sub_role": title.get("sub_role"),
            "title_levels": title.get("levels", []),
            "is_primary": exp.get("is_primary"),
        })

    return rows


def flatten_education(person: dict, partition_date: str) -> list[dict]:
    rows = []
    person_id = person.get("id")

    for idx, edu in enumerate(person.get("education", [])):
        school = edu.get("school", {}) or {}

        rows.append({
            "date": partition_date,
            "person_id": person_id,
            "education_index": idx,
            "school_name": school.get("name"),
            "school_type": school.get("type"),
            "school_id": school.get("id"),
            "school_location": school.get("location"),
            "school_linkedin_url": school.get("linkedin_url"),
            "school_facebook_url": school.get("facebook_url"),
            "school_twitter_url": school.get("twitter_url"),
            "school_linkedin_id": school.get("linkedin_id"),
            "school_website": school.get("website"),
            "school_domain": school.get("domain"),
            "degrees": edu.get("degrees", []),
            "start_date": edu.get("start_date"),
            "end_date": edu.get("end_date"),
            "majors": edu.get("majors", []),
            "minors": edu.get("minors", []),
            "gpa": edu.get("gpa"),
        })

    return rows


def flatten_profiles(person: dict, partition_date: str) -> list[dict]:
    rows = []
    person_id = person.get("id")

    for idx, profile in enumerate(person.get("profiles", [])):
        rows.append({
            "date": partition_date,
            "person_id": person_id,
            "profile_index": idx,
            "network": profile.get("network"),
            "profile_id": profile.get("id"),
            "url": profile.get("url"),
            "username": profile.get("username"),
        })

    return rows


def build_dataset_keys(date_partition: str, run_id: str) -> dict:
    base = BRONZE_PREFIX.rstrip("/")
    return {
        "people": f"{base}/people/date={date_partition}/{run_id}.csv",
        "experience": f"{base}/experience/date={date_partition}/{run_id}.csv",
        "education": f"{base}/education/date={date_partition}/{run_id}.csv",
        "profiles": f"{base}/profiles/date={date_partition}/{run_id}.csv",
    }


def process_raw_to_bronze(raw_bucket: str, raw_key: str) -> dict:
    raw_payload = read_json(raw_bucket, raw_key)

    date_partition = raw_payload.get("run_timestamp_utc", "")[:8]
    if date_partition:
        date_partition = (
            f"{date_partition[0:4]}-{date_partition[4:6]}-{date_partition[6:8]}"
        )
    else:
        date_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    run_id = uuid.uuid4().hex[:8]

    people_rows = []
    experience_rows = []
    education_rows = []
    profile_rows = []

    for person in raw_payload.get("results", []):
        people_rows.append(
            flatten_person(
                person=person,
                partition_date=date_partition,
            )
        )
        experience_rows.extend(flatten_experience(person, date_partition))
        education_rows.extend(flatten_education(person, date_partition))
        profile_rows.extend(flatten_profiles(person, date_partition))

    output_keys = build_dataset_keys(date_partition=date_partition, run_id=run_id)

    written = {}

    if people_rows:
        write_csv_rows(
            bucket=DATA_LAKE_BUCKET,
            key=output_keys["people"],
            rows=people_rows,
        )
        written["people_output_key"] = output_keys["people"]

    if experience_rows:
        write_csv_rows(
            bucket=DATA_LAKE_BUCKET,
            key=output_keys["experience"],
            rows=experience_rows,
        )
        written["experience_output_key"] = output_keys["experience"]

    if education_rows:
        write_csv_rows(
            bucket=DATA_LAKE_BUCKET,
            key=output_keys["education"],
            rows=education_rows,
        )
        written["education_output_key"] = output_keys["education"]

    if profile_rows:
        write_csv_rows(
            bucket=DATA_LAKE_BUCKET,
            key=output_keys["profiles"],
            rows=profile_rows,
        )
        written["profile_output_key"] = output_keys["profiles"]

    #print('Lenght',len(written))

    return {
        "run_id": run_id,
        "source_raw_bucket": raw_bucket,
        "source_raw_key": raw_key,
        "DATA_LAKE_BUCKET": DATA_LAKE_BUCKET,
        "date_partition": date_partition,
        "person_count": len(people_rows),
        "experience_count": len(experience_rows),
        "education_count": len(education_rows),
        "profile_count": len(profile_rows),
        **written,
    }