"""
ATS matching — 100% free.
Compares a job description against every PDF resume in RESUME_FOLDER.
Returns best match + full score matrix.
"""
import os
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import pdfplumber

TECH_SKILLS = {
    "python","sql","java","scala","r","bash",
    "spark","pyspark","kafka","airflow","dbt","luigi","prefect","dagster","nifi",
    "snowflake","bigquery","redshift","hive","presto","trino","databricks",
    "postgres","postgresql","mysql","mongodb","cassandra","redis","elasticsearch",
    "aws","azure","gcp","s3","lambda","glue","kinesis","emr","synapse","fabric",
    "docker","kubernetes","terraform","jenkins","git","github","gitlab","bitbucket",
    "tableau","powerbi","looker","grafana","qlik","metabase",
    "pandas","numpy","scikit-learn","pytorch","tensorflow","matplotlib","seaborn",
    "etl","elt","pipeline","datawarehouse","datalake","lakehouse","delta lake",
    "iceberg","parquet","avro","json","xml","csv","api","rest","graphql",
    "ci/cd","devops","agile","scrum","jira","confluence",
    "machine learning","ml","nlp","deep learning","statistics",
    "fhir","hl7","hipaa","gdpr","epic","cerner",
    "excel","vba","ssis","ssrs","ssas","informatica","talend","pentaho",
    "alteryx","fivetran","stitch","airbyte","dbt cloud",
}

SOFT_SKILLS = {
    "communication","collaboration","teamwork","leadership","analytical",
    "problem-solving","stakeholder","cross-functional","presentation",
}

ALL_KW = TECH_SKILLS | SOFT_SKILLS


def _clean(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s\+#\./]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _extract_kw(text: str) -> set:
    cleaned = _clean(text)
    return {kw for kw in ALL_KW if re.search(r"\b" + re.escape(kw) + r"\b", cleaned)}


def _read_pdf(path: str) -> str:
    pages = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            pages.append(page.extract_text() or "")
    return "\n".join(pages)


def _score(resume_text: str, job_text: str) -> dict:
    r_clean = _clean(resume_text)
    j_clean = _clean(job_text)

    # TF-IDF cosine similarity (40%)
    try:
        vec    = TfidfVectorizer(stop_words="english", ngram_range=(1, 2))
        mat    = vec.fit_transform([r_clean, j_clean])
        cosine = float(cosine_similarity(mat[0:1], mat[1:2])[0][0])
    except Exception:
        cosine = 0.0

    # Keyword overlap (50%)
    r_kw    = _extract_kw(resume_text)
    j_kw    = _extract_kw(job_text)
    matched = r_kw & j_kw
    missing = j_kw - r_kw
    kw_rate = len(matched) / len(j_kw) if j_kw else 0.0

    # Role relevance (10%)
    data_roles = {"data engineer","data analyst","analytics engineer",
                  "bi analyst","business intelligence","reporting analyst",
                  "systems analyst","clinical","population insights"}
    role_hit   = any(role in j_clean for role in data_roles)
    role_score = 1.0 if role_hit else 0.5

    ats = (cosine * 0.40 + kw_rate * 0.50 + role_score * 0.10) * 100
    return {
        "ats_score":          round(ats, 2),
        "cosine_similarity":  round(cosine * 100, 2),
        "keyword_match_rate": round(kw_rate * 100, 2),
        "matched_keywords":   sorted(matched),
        "missing_keywords":   sorted(missing),
        "total_job_keywords": len(j_kw),
    }


def match_all_resumes(job_description: str, resume_folder: str) -> dict:
    """
    Compare job description against every PDF in resume_folder.
    Returns:
        best   — {resume_name, ats_score, matched_keywords, missing_keywords, ...}
        all    — list of above for every resume
        passed — True if best.ats_score >= ATS_THRESHOLD
    """
    from config import ATS_THRESHOLD
    results = []
    for fname in os.listdir(resume_folder):
        if not fname.lower().endswith(".pdf"):
            continue
        path = os.path.join(resume_folder, fname)
        try:
            text   = _read_pdf(path)
            scores = _score(text, job_description)
            results.append({"resume_name": fname, **scores})
        except Exception as e:
            results.append({"resume_name": fname, "ats_score": 0.0,
                            "error": str(e), "matched_keywords": [],
                            "missing_keywords": [], "cosine_similarity": 0.0,
                            "keyword_match_rate": 0.0, "total_job_keywords": 0})

    if not results:
        return {"best": None, "all": [], "passed": False}

    best = max(results, key=lambda x: x["ats_score"])
    return {
        "best":   best,
        "all":    results,
        "passed": best["ats_score"] >= ATS_THRESHOLD,
    }
