"""
This module provides a series of RESTful APIs to provide data
gathered from the agcomdata.parquet file.

Author: Merak Winston Hall
Date: 2024-07-13
"""

import psycopg2
from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from starlette.responses import FileResponse
from datetime import datetime
import warnings

warnings.filterwarnings('ignore')

DB_HOST = "dpg-cva6ogbtq21c73bs2j8g-a.frankfurt-postgres.render.com"
DB_PORT = "5432"
DB_NAME = "agcom_dati_monitoraggio"
DB_USER = "agcom_dati_monitoraggio_user"
DB_PASSWORD = "o6uuicatOkvkAKZc41deozEBd56g8nrm"
TABLE_NAME = "records"

def query_postgresql(query, params=None):
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        with conn.cursor() as cur:
            cur.execute(query, params)
            data = cur.fetchall()
        conn.close()
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading from PostgreSQL: {e}")

# -------------------------------------------------------

def get_min_max_dates():
    query = f"""SELECT MIN(day), MAX(day) FROM {TABLE_NAME};"""
    result = query_postgresql(query)
    return result[0][0].strftime('%Y/%m/%d'), result[0][1].strftime('%Y/%m/%d')

start_date, end_date = get_min_max_dates()

kind = ["speech", "news", "both"]

politicians_list = [k[0] for k in query_postgresql(f"""SELECT DISTINCT fullname FROM {TABLE_NAME} WHERE name != 'Soggetto Collettivo';""")]
political_groups_list  = [k[0] for k in query_postgresql(f"""SELECT DISTINCT fullname FROM {TABLE_NAME} WHERE name = 'Soggetto Collettivo';""")]
channels = [k[0] for k in query_postgresql(f"""SELECT DISTINCT channel FROM {TABLE_NAME};""")]
programs = [k[0] for k in query_postgresql(f"""SELECT DISTINCT program FROM {TABLE_NAME};""")]
affiliations = [k[0] for k in query_postgresql(f"""SELECT DISTINCT affiliation FROM {TABLE_NAME};""")]
topics = [k[0] for k in query_postgresql(f"""SELECT DISTINCT topic FROM {TABLE_NAME};""")]

def validate_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y/%m/%d')
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY/MM/DD") from exc

# -------------------------------------------------------

description = """
## AGCOM - elementary data of the italian television monitoring

This API provides the possibility to query the elementary televised monitoring data provided by [AGCOM](https://www.agcom.it/) - Italian authority for guarantees in communications - of political interventions (data as news or as word)

The data can be found in XML format at [https://www.agcom.it/dati-elementari-di-monitoraggio-televisivo](https://www.agcom.it/dati-elementari-di-monitoraggio-televisivo)

Period:<br/>
from **%s** to **%s**


The license under which the data is released by AGCOM is CC-BY-SA-NC

![](https://www.agcom.it/documents/10179/4502194/Logo+Creative+common/2e1fe5a2-4324-4965-b8af-76403bb42b15?t=1618583317352)

""" % (start_date, end_date)

app = FastAPI(
    title="AGCOM - dati elementari di monitoraggio televisivo",
    version="0.0.1",
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_index():
    return FileResponse('templates/home.html')

@app.get("/index")
async def read_charts():
    """
    Serve the index.html file.
    """
    return FileResponse('templates/index.html')

@app.get("/faq")
async def read_faq():
    """
    Serve the pages-faq.html file.
    """
    return FileResponse('templates/pages-faq.html')

@app.get("/contact")
async def contact():
    """
    Serve the pages-contact.html file.
    """
    return FileResponse('templates/pages-contact.html')

# -------------------------------------------------------

@app.get("/v1/politicians")
async def get_politicians(
    start_date_: str = Query(default=start_date),
    end_date_: str = Query(default=end_date),
    kind_: str = Query(default="both", enum=kind),
    page: int = Query(default=1),
    page_size: int = Query(default=5000)
):
    """
    Return all politicians with pagination
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="Page and page size must be positive integers.")
    

    kind_filter = "" if kind_ == "both" else f"AND kind = '{kind_}'"

    query = f"""
        SELECT DISTINCT fullname
        FROM records
        WHERE name != 'Soggetto Collettivo' AND day >= %s AND day <= %s {kind_filter}
        GROUP BY fullname
        ORDER BY fullname
    """

    politicians_ = query_postgresql(query, (start_date_, end_date_))
    politicians_ = [p[0] for p in politicians_]

    start = (page - 1) * page_size
    end = start + page_size

    return {
        "total": len(politicians_),
        "politicians": politicians_[start:end],
        "page_size": page_size
    }


@app.get("/v1/political-groups")
async def get_political_groups(
    start_date_: str = Query(default=start_date, description="Start date"),
    end_date_: str = Query(default=end_date, description="End date"),
    kind_: str = Query(default="both", description="Type of data", enum=kind),
    page: int = Query(default=1, description="Page number"),
    page_size: int = Query(default=5000, description="Page size")
):
    """
    Return all political groups with pagination
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="Page and page size must be positive integers.")

    kind_filter = "" if kind_ == "both" else f"AND kind = '{kind_}'"

    query = f"""
        SELECT DISTINCT lastname
        FROM records
        WHERE name = 'Soggetto Collettivo' AND day >= %s AND day <= %s {kind_filter}
        GROUP BY lastname
        ORDER BY lastname
    """

    political_groups_ = query_postgresql(query, (start_date_, end_date_))
    political_groups_ = [pg[0] for pg in political_groups_]

    start = (page - 1) * page_size
    end = start + page_size

    return {
        "total": len(political_groups_),
        "political_groups": political_groups_[start:end],
        "page_size": page_size
    }


@app.get("/v1/channels")
async def get_channels(
    page: int = Query(default=1, description="Page number"),
    page_size: int = Query(default=5000, description="Page size"),
    program_: str = Query(default = "all", description="Program")
):
    """
    Return all channels
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="Page and page size must be positive integers.")
    
    filter = ""

    if program_ != "all":
        if program_ not in programs:
            raise HTTPException(status_code=400, detail="Invalid program")
        filter = f"""WHERE program = '{program_}'"""
        
    query = f"""
        SELECT DISTINCT channel
        FROM records
        {filter}
        GROUP BY channel
        ORDER BY channel
    """

    channels_ = query_postgresql(query)
    channels_ = [k[0] for k in channels_]

    start = (page - 1) * page_size
    end = start + page_size

    return { "channels": channels_[start:end], "page_size": page_size }


@app.get("/v1/programs")
async def get_programs(
    page: int = Query(default=1, description="Page number"),
    page_size: int = Query(default=5000, description="Page size"),
    channel_: str = Query(default = "all", description="Channel")
):
    """
    Return all programs
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="Page and page size must be positive integers.")
    
    filter = ""

    if channel_ != "all":
        if channel_ not in channels:
            raise HTTPException(status_code=400, detail="Invalid channel")
        filter = f"""WHERE channel = '{channel_}'"""

    query = f"""
        SELECT DISTINCT program
        FROM records
        {filter}
        GROUP BY program
        ORDER BY program
    """

    programs_ = query_postgresql(query)
    programs_ = [k[0] for k in programs_]

    start = (page - 1) * page_size
    end = start + page_size

    return { "programs": programs_[start:end], "page_size": page_size }


@app.get("/v1/affiliations")
async def get_affiliations(
    page: int = Query(default=1, description="Page number"),
    page_size: int = Query(default=5000, description="Page size"),
    politician_: str = Query(default = "all", description="Politician")
):
    """
    Return all affiliations
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="Page and page size must be positive integers.")
    
    filter = ""

    if politician_ != "all":
        if politician_ not in politicians_list:
            raise HTTPException(status_code=400, detail="Invalid politician")
        filter = f"""WHERE fullname = '{politician_}'"""

    query = f"""
        SELECT DISTINCT affiliation
        FROM records
        {filter}
        GROUP BY affiliation
        ORDER BY affiliation
    """

    affiliations_ = query_postgresql(query)
    affiliations_ = [k[0] for k in affiliations_]

    start = (page - 1) * page_size
    end = start + page_size

    return { "affiliations": affiliations_[start:end], "page_size": page_size }


@app.get("/v1/topics")
async def get_topics(
    page: int = Query(default=1, description="Page number"),
    page_size: int = Query(default=5000, description="Page size")
):
    """
    Return all topics
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="Page and page size must be positive integers.")

    query = f"""
        SELECT DISTINCT topic
        FROM records
        GROUP BY topic
        ORDER BY topic
    """

    topics_ = query_postgresql(query)
    topics_ = [k[0] for k in topics_]

    start = (page - 1) * page_size
    end = start + page_size

    return { "topics": topics_[start:end], "page_size": page_size }

# -------------------------------------------------------

@app.get("/v1/politician-topics/{name}")
async def get_politician_topics(
    name: str,
    start_date_: str = Query(default=start_date),
    end_date_: str = Query(default=end_date),
    kind_: str = Query(default="both", enum=kind),
    channel_: str = Query(default="all"),
    affiliation_: str = Query(default="all"),
    program_: str = Query(default="all"),
    topic_: str = Query(default="all"),
    page: int = Query(default=1),
    page_size: int = Query(default=100)
):
    """
    Return how much a politician talked about all the possible topics
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="Page and page size must be positive integers.")

    conditions = ["name != 'Soggetto Collettivo'", "fullname = %s"]
    params = [name]

    if channel_ != "all":
        if channel_ not in channels:
            raise HTTPException(status_code=400, detail="Invalid channel")
        conditions.append("channel = %s")
        params.append(channel_)
    if affiliation_ != "all":
        if affiliation_ not in affiliations:
            raise HTTPException(status_code=400, detail="Invalid affiliation")
        conditions.append("affiliation = %s")
        params.append(affiliation_)
    if program_ != "all":
        if program_ not in programs:
            raise HTTPException(status_code=400, detail="Invalid program")
        conditions.append("program = %s")
        params.append(program_)
    if topic_ != "all":
        if topic_ not in topics:
            raise HTTPException(status_code=400, detail="Invalid topic")
        conditions.append("topic = %s")
        params.append(topic_)
    if kind_ != "both":
        conditions.append("kind = %s")
        params.append(kind_)

    condition_str = " AND ".join(conditions)

    query = f"""
        SELECT topic, SUM(duration), COUNT(*)
        FROM records
        WHERE {condition_str} AND day >= %s AND day <= %s
        GROUP BY topic
        ORDER BY topic
    """
    params.extend([start_date_, end_date_])

    data = query_postgresql(query, params)
    explicit_data = []
    for d in data:
        temp_topic = d[0]
        temp_duration = d[1]
        temp_interventions = d[2]
        explicit_data.append({"topic": temp_topic, "minutes": temp_duration, "interventions": temp_interventions})

    paginated_data = explicit_data[(page - 1) * page_size: page * page_size]

    return {"politician": name, "topics": paginated_data, "page_size": page_size}


@app.get("/v1/political-group-topics/{name}")
async def get_political_group_topics(
    name: str,
    start_date_: str = Query(default=start_date),
    end_date_: str = Query(default=end_date),
    kind_: str = Query(default="both", enum=kind),
    channel_: str = Query(default="all"),
    program_: str = Query(default="all"),
    topic_: str = Query(default="all"),
    page: int = Query(default=1),
    page_size: int = Query(default=100)
):
    """
    Return how much a political group talked about all the possible topics 
    """
    if page < 1 or page_size < 1:
        raise HTTPException(status_code=400, detail="Page and page size must be positive integers.")

    conditions = ["name = 'Soggetto Collettivo'", "lastname = %s"]
    params = [name]

    if channel_ != "all":
        if channel_ not in channels:
            raise HTTPException(status_code=400, detail="Invalid channel")
        conditions.append("channel = %s")
        params.append(channel_)
    if program_ != "all":
        if program_ not in programs:
            raise HTTPException(status_code=400, detail="Invalid program")
        conditions.append("program = %s")
        params.append(program_)
    if topic_ != "all":
        if topic_ not in topics:
            raise HTTPException(status_code=400, detail="Invalid topic")
        conditions.append("topic = %s")
        params.append(topic_)
    if kind_ != "both":
        conditions.append("kind = %s")
        params.append(kind_)

    condition_str = " AND ".join(conditions)

    query = f"""
        SELECT topic, SUM(duration), COUNT(*)
        FROM records
        WHERE {condition_str} AND day >= %s AND day <= %s
        GROUP BY topic
        ORDER BY topic
    """
    params.extend([start_date_, end_date_])

    data = query_postgresql(query, params)
    explicit_data = []
    for d in data:
        temp_topic = d[0]
        temp_duration = d[1]
        temp_interventions = d[2]
        explicit_data.append({"topic": temp_topic, "minutes": temp_duration, "interventions": temp_interventions})

    paginated_data = explicit_data[(page - 1) * page_size: page * page_size]

    return {"political group": name, "topics": paginated_data, "page_size": page_size}