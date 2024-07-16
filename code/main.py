"""
This module provides a series of RESTful APIs to provide data
gathered from the agcomdata.parquet file.

Author: Merak Winston Hall
Date: 2024-07-13
"""

import os
import warnings
from datetime import datetime
import datetime as dt
from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from starlette.responses import FileResponse
import polars as pl
warnings.filterwarnings('ignore')


data = pl.read_parquet(".." + os.sep + "docs"  + os.sep +  "data" + os.sep + "agcomdata.parquet")
data = data.with_columns((pl.col("name") + " " + pl.col("lastname")).alias("fullname"))

data = data.with_columns(
    pl.col('kind').map_dict({'Parola': 'speech', 'Notizia': 'news'}).alias('kind')
)

start_date = data.select('day').min().to_series()[0].strftime('%Y/%m/%d')
end_date = data.select('day').max().to_series()[0].strftime('%Y/%m/%d')

kind = ["speech", "news", "both"]

politicians = data.filter(pl.col("name") != "political movement")
politicians_list = data.select('fullname').unique().to_series().to_list()
political_groups = data.filter(pl.col("name") == "Soggetto collettivo")
political_groups_list = data.select('lastname').unique().to_series().to_list()

channels = data.select('channel').unique().to_series().to_list()
programs = data.select('program').unique().to_series().to_list()
affiliations = data.select('affiliation').unique().to_series().to_list()
topics = data.select('topic').unique().to_series().to_list()

# -------------------------------------------------------

def validate_date(date_str):
    """
    Function to validate dates (from string to datetime)
    """
    try:
        return datetime.strptime(date_str, '%Y/%m/%d')
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY/MM/DD") from exc


def filter_data(data_, start_date_, end_date_, kind_):
    """
    Function to filter data by selected date interval and kind
    """
    start_date_ = validate_date(start_date_)
    end_date_ = validate_date(end_date_)

    if start_date_ > end_date_:
        raise HTTPException(status_code=400, detail="Start date must be before end date")

    filtered_data = data_.filter(pl.col("day") >= start_date_).filter(pl.col("day") <= end_date_)


    if kind_ != "both":
        filtered_data = filtered_data.filter(pl.col("kind") == kind_)

    return filtered_data

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
    description=description,
    version="0.0.1",
    contact={
        "name": "Merak",
        "email": "merakwinston.hall@gmail.com",
    },
    license_info={
        "name": "data under cc-by-nc-sa",
        "url": "https://creativecommons.org/licenses/by-nc-sa/4.0/"
    },
)

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_index():
    """
    Serve the index.html file.
    """
    return FileResponse('templates/index.html')

# -------------------------------------------------------

@app.get("/v1/politicians")
async def get_politicians(
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return all politicians
    """
    politicians_ = filter_data(politicians, start_date_, end_date_, kind_)
    politicians_ = politicians_.select('fullname').unique().to_series().to_list()

    return { "politicians": politicians_ }


@app.get("/v1/political-groups")
async def get_political_groups(
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return all political groups
    """

    political_groups_ = filter_data(political_groups, start_date_, end_date_, kind_)
    political_groups_ = political_groups_.select('lastname').unique().to_series().to_list()

    return { "political_groups": political_groups_ }


@app.get("/v1/channels")
async def get_channels():
    """
    Return all channels
    """

    channels_ = data.select('channel').unique().to_series().to_list()

    return { "channels": channels_ }


@app.get("/v1/programs")
async def get_programs():
    """
    Return all programs
    """
    programs_ = data.select('program').unique().to_series().to_list()

    return { "programs": programs_ }


@app.get("/v1/affiliations")
async def get_affiliations():
    """
    Return all affiliations
    """

    affiliations_ = data.select('affiliation').unique().to_series().to_list()

    return { "affiliations": affiliations_ }


@app.get("/v1/topics")
async def get_topics():
    """
    Return all topics
    """

    topics_ = data.select('topic').unique().to_series().to_list()

    return { "topics": topics_ }

# -------------------------------------------------------

@app.get("/v1/politician-topics/{name}")
async def get_politician_topics(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how much a politician talked about all the possible topics
    """

    politician_topics = politicians.filter(pl.col('fullname') == name)

    politician_topics = filter_data(politician_topics, start_date_, end_date_, kind_)
    final_list = []

    for t in topics:
        temp = politician_topics.filter(pl.col('topic') == t)
        total = temp.select('duration').sum().to_series().to_list()
        interventions = temp.shape[0]
        final_list.append({"topic": t,
                           "minutes" : total[0],
                           "interventions" : interventions })

    return { "politician": name, "topics": final_list }


@app.get("/v1/political-group-topics/{name}")
async def get_political_group_topics(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how much a political group talked about all the possible topics 
    """

    polgroup_topics = political_groups.filter(pl.col('lastname') == name)
    polgroup_topics = filter_data(polgroup_topics, start_date_, end_date_, kind_)
    final_list = []

    for t in topics:
        temp = polgroup_topics.filter(pl.col('topic') == t)
        total = temp.select('duration').sum().to_series().to_list()
        interventions = temp.shape[0]
        final_list.append({"topic": t,
                           "minutes" : total[0],
                           "interventions" : interventions })

    return { "political group": name, "topics": final_list }

# -------------------------------------------------------

@app.get("/v1/politician-channels/{name}")
async def get_politician_channels(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how much a politician talked in a specific channel
    """

    politician_channels = politicians.filter(pl.col('fullname') == name)
    politician_channels = filter_data(politician_channels, start_date_, end_date_, kind_)
    final_list = []

    for c in channels:
        temp = politician_channels.filter(pl.col('channel') == c)
        total = temp.select('duration').sum().to_series().to_list()
        interventions = temp.shape[0]
        final_list.append({"channel": c,
                           "minutes" : total[0],
                           "interventions" : interventions })

    return { "politician": name, "channels": final_list }


@app.get("/v1/political-group-channels/{name}")
async def get_political_group_channels(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how much a political group talked in a specific channel
    """
    polgroup_channels = political_groups.filter(pl.col('lastname') == name)
    polgroup_channels = filter_data(polgroup_channels, start_date_, end_date_, kind_)

    final_list = []

    for c in channels:
        temp = polgroup_channels.filter(pl.col('channel') == c)
        total = temp.select('duration').sum().to_series().to_list()
        interventions = temp.shape[0]
        final_list.append({"channel": c,
                           "minutes" : total[0],
                           "interventions" : interventions })

    return { "political group": name, "channels": final_list }

# -------------------------------------------------------

@app.get("/v1/politicians-affiliation/{name}")
async def get_politicians_affiliation(
    name: str,
):
    """
    Return how many politicians participate (have participated) in an affiliation
    """
    politicians_affiliation = politicians.filter(pl.col('affiliation') == name)
    final_list = politicians_affiliation.select('fullname').unique().to_series().to_list()

    return { "affiliation": name, "politicians": final_list }


@app.get("/v1/affiliations-politician/{name}")
async def get_affiliations_politician(
    name: str,
):
    """
    Return how many affiliations a politician have participated in
    """

    affiliations_politician = politicians.filter(pl.col('fullname') == name)
    final_list = affiliations_politician.select('affiliation').unique().to_series().to_list()

    return { "politician": name, "affiliations": final_list}

# -------------------------------------------------------

@app.get("/v1/interventions-political-group/{name}")
async def get_interventions_political_group(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how much a political group has intervened in tv 
    (counting also politicians single intervents)
    """

    interventions_polgroup = data.filter(pl.col('affiliation') == name)
    interventions_polgroup = filter_data(interventions_polgroup, start_date_, end_date_, kind_)
    interventions = interventions_polgroup.shape[0]
    minutes = interventions_polgroup.select('duration').sum().to_series().to_list()

    return { "political group": name, "interventions": interventions, "minutes": minutes[0] }

# -------------------------------------------------------

@app.get("/v1/dates")
async def get_dates():
    """
    Return the first and last date of the dataset
    """
    start_date_ = data.select('day').min().to_series()[0].strftime('%Y-%m-%d')
    end_date_ = data.select('day').max().to_series()[0].strftime('%Y-%m-%d')

    return { "first_date": start_date_, "end_date": end_date_}

# -------------------------------------------------------

@app.get("/v1/interventions-politician-per-year/{name}")
async def get_interventions_politician_per_year(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how much a politician has intervened in tv per year
    """

    interventions_politician = data.filter(pl.col('fullname') == name)
    interventions_politician = filter_data(interventions_politician, start_date_, end_date_, kind_)

    first_year = int(start_date_.split('/')[0])
    last_year = int(end_date_.split('/')[0])

    years = []
    interventions = []
    minutes = []

    while first_year != (last_year + 1):
        fy = datetime.strptime(str(first_year), '%Y')
        ly = datetime.strptime(str(first_year + 1), '%Y')
        a = interventions_politician.filter(pl.col('day') >= fy)
        a = a.filter(pl.col('day') <= ly)
        years.append(str(first_year))
        interventions.append(a.shape[0])
        minutes.append(a.select('duration').sum().to_series().to_list()[0])
        first_year += 1

    return { "politician": name, "years": years,
            "interventions": interventions, "minutes": minutes }


@app.get("/v1/interventions-political-group-per-year/{name}")
async def get_interventions_political_group_per_year(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how much a political group has intervened in tv per year
    """

    interventions_polgroup = data.filter(pl.col('lastname') == name)
    interventions_polgroup = filter_data(interventions_polgroup, start_date_, end_date_, kind_)

    first_year = int(start_date_.split('/')[0])
    last_year = int(end_date_.split('/')[0])

    years = []
    interventions = []
    minutes = []

    while first_year != (last_year + 1):
        fy = datetime.strptime(str(first_year), '%Y')
        ly = datetime.strptime(str(first_year + 1), '%Y')
        a = interventions_polgroup.filter(pl.col('day') >= fy)
        a = a.filter(pl.col('day') <= ly)
        years.append(first_year)
        interventions.append(a.shape[0])
        minutes.append(a.select('duration').sum().to_series().to_list()[0])
        first_year += 1

    return { "political group": name, "years": years,
            "interventions": interventions, "minutes": minutes }

# -------------------------------------------------------

@app.get("/v1/interventions-politician-per-day/{name}")
async def get_interventions_politician_per_day(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how much a politician has intervened in tv per day
    """

    interventions_politician = data.filter(pl.col('fullname') == name)
    interventions_politician = filter_data(interventions_politician, start_date_, end_date_, kind_)

    begin_date = dt.date(int(start_date_.split('/')[0]),
                         int(start_date_.split('/')[1]),
                         int(start_date_.split('/')[2]))
    b = begin_date
    final_date = dt.date(int(end_date_.split('/')[0]),
                         int(end_date_.split('/')[1]),
                         int(end_date_.split('/')[2]))
    day = dt.timedelta(1)
    year = begin_date.year
    interventions = []
    i = []
    max_value = 0

    while begin_date != final_date:
        if begin_date.year != year:
            interventions.append(i)
            year = year + 1
            for j in i:
                if max_value < j[1]:
                    max_value = j[1]
            i = []
        else:
            d = datetime.strptime(str(begin_date), "%Y-%m-%d")
            temp = interventions_politician.filter(pl.col("day") == d)
            i.append([begin_date, temp.shape[0]])
            begin_date = begin_date + day
    interventions.append(i)

    return { "politician": name,"interventions": interventions, "max_value": max_value,
            "begin year": b.year, "final year": final_date.year}


@app.get("/v1/interventions-political-group-per-day/{name}")
async def get_interventions_political_group_per_day(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how much a political group has intervened in tv per day
    """

    interventions_polgroup = data.filter(pl.col('lastname') == name)
    interventions_polgroup = filter_data(interventions_polgroup, start_date_, end_date_, kind_)

    begin_date = dt.date(int(start_date_.split('/')[0]),
                         int(start_date_.split('/')[1]),
                         int(start_date_.split('/')[2]))
    b = begin_date
    final_date = dt.date(int(end_date_.split('/')[0]),
                         int(end_date_.split('/')[1]),
                         int(end_date_.split('/')[2]))
    day = dt.timedelta(1)
    year = begin_date.year
    interventions = []
    i = []
    max_value = 0

    while begin_date != final_date:
        if begin_date.year != year:
            interventions.append(i)
            year = year + 1
            for j in i:
                if max_value < j[1]:
                    max_value = j[1]
            i = []
        else:
            d = datetime.strptime(str(begin_date), "%Y-%m-%d")
            temp = interventions_polgroup.filter(pl.col("day") == d)
            i.append([begin_date, temp.shape[0]])
            begin_date = begin_date + day
    interventions.append(i)

    return { "political group": name, "interventions": interventions, "max_value": max_value,
            "begin year": b.year, "final year": final_date.year}

# -------------------------------------------------------

@app.get("/v1/politician-channels-programs/{name}")
async def get_politician_channels_programs(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return every channel a politician talked in, specifying for each 
    channel all the programs he participated in and for how long (minutes)
    """

    politician_channels = politicians.filter(pl.col('fullname') == name)
    politician_channels = filter_data(politician_channels, start_date_, end_date_, kind_)
    channels_ = politician_channels.select('channel').unique().to_series().to_list()
    final_list = []

    for c in channels_:
        single_channel = politician_channels.filter(pl.col('channel') == c)
        programs_channel = single_channel.select('program').unique().to_series().to_list()
        temp = []
        for p in programs_channel:
            single_program = single_channel.filter(pl.col('program') == p)
            p1 = single_program.select('program').unique().to_series().to_list()
            total = single_program.select('duration').sum().to_series().to_list()
            temp.append({'program': p1[0], 'minutes': total[0]})
        final_list.append({'channel': single_channel.select('channel').unique()
                           .to_series().to_list()[0], 'programs': temp})

    return { "politician": name, "channels": final_list }


@app.get("/v1/political-group-channels-programs/{name}")
async def get_political_group_channels_programs(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return every channel a political group talked in, specifying for each 
    channel all the programs he participated in and for how long (minutes)
    """

    polgroup_channels = political_groups.filter(pl.col('lastname') == name)
    polgroup_channels = filter_data(polgroup_channels, start_date_, end_date_, kind_)
    channels_ = polgroup_channels.select('channel').unique().to_series().to_list()
    final_list = []

    for c in channels_:
        single_channel = polgroup_channels.filter(pl.col('channel') == c)
        programs_channel = single_channel.select('program').unique().to_series().to_list()
        temp = []
        for p in programs_channel:
            single_program = single_channel.filter(pl.col('program') == p)
            p1 = single_program.select('program').unique().to_series().to_list()
            total = single_program.select('duration').sum().to_series().to_list()
            temp.append({'program': p1[0], 'minutes': total[0]})
        final_list.append({'channel': single_channel.select('channel').unique()
                           .to_series().to_list()[0], 'programs': temp})

    return { "political group": name, "channels": final_list }

# -------------------------------------------------------

@app.get("/v1/channel-programs-politician/{name}/{channel}")
async def get_channel_programs_politician(
    name: str,
    channel: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind),
    affiliation_: str = Query(default = "all", description="Affiliation"),
    program_: str = Query(default = "all", description="Program"),
    topic_: str = Query(default = "all", description="Topic")
):
    """
    Return every program of a channel a politician talked in, 
    specifying for each program all the topics and for how long (minutes)
    """
    filtered_data = politicians

    if channel not in channels:
        raise HTTPException(status_code=400, detail="Channel not found")
    filtered_data = filtered_data.filter(pl.col('channel') == channel)

    if name not in politicians_list:
        raise HTTPException(status_code=400, detail="Politician not found")
    filtered_data = filtered_data.filter(pl.col('fullname') == name)

    if affiliation_ != "all":
        if affiliation_ not in affiliations:
            raise HTTPException(status_code=400, detail="Invalid program")
        filtered_data = filtered_data.filter(pl.col('affiliation') == affiliation_)

    if program_ != "all":
        if program_ not in programs:
            raise HTTPException(status_code=400, detail="Invalid program")
        filtered_data = filtered_data.filter(pl.col('program') == program_)

    if topic_ != "all":
        if topic_ not in topics:
            raise HTTPException(status_code=400, detail="Invalid topic")
        filtered_data = filtered_data.filter(pl.col('topic') == topic_)
    
    politician_channels = filter_data(filtered_data, start_date_, end_date_, kind_)
    programs_ = politician_channels.select('program').unique().to_series().to_list()
    final_list = []

    for p in programs_:
        single_program = politician_channels.filter(pl.col('program') == p)
        topics_programs = single_program.select('topic').unique().to_series().to_list()
        temp = []
        for t in topics_programs:
            single_topic = single_program.filter(pl.col('topic') == t)
            t1 = single_topic.select('topic').unique().to_series().to_list()
            total = single_topic.select('duration').sum().to_series().to_list()
            temp.append({'topic': t1[0], 'minutes': total[0]})
        final_list.append({'program': single_program.select('program').unique()
                           .to_series().to_list()[0], 'topics': temp})

    return { "politician": name, "channel": channel, "programs": final_list }


@app.get("/v1/channel-programs-political-group/{name}/{channel}")
async def get_channel_programs_political_group(
    name: str,
    channel: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind),
    program_: str = Query(default = "all", description="Program"),
    topic_: str = Query(default = "all", description="Topic")
):
    """
    Return every program of a channel a political group talked in, 
    specifying for each program all the topics and for how long (minutes)
    """

    filtered_data = political_groups

    if channel not in channels:
        raise HTTPException(status_code=400, detail="Channel not found")
    filtered_data = filtered_data.filter(pl.col('channel') == channel)

    if name not in political_groups_list:
        raise HTTPException(status_code=400, detail="Political Group not found")
    filtered_data = filtered_data.filter(pl.col('lastname') == name)

    if program_ != "all":
        if program_ not in programs:
            raise HTTPException(status_code=400, detail="Invalid program")
        filtered_data = filtered_data.filter(pl.col('program') == program_)

    if topic_ != "all":
        if topic_ not in topics:
            raise HTTPException(status_code=400, detail="Invalid topic")
        filtered_data = filtered_data.filter(pl.col('topic') == topic_)

    polgroup_channels = filter_data(filtered_data, start_date_, end_date_, kind_)
    programs_ = polgroup_channels.select('program').unique().to_series().to_list()
    final_list = []

    for p in programs_:
        single_program = polgroup_channels.filter(pl.col('program') == p)
        topics_programs = single_program.select('topic').unique().to_series().to_list()
        temp = []
        for t in topics_programs:
            single_topic = single_program.filter(pl.col('topic') == t)
            t1 = single_topic.select('topic').unique().to_series().to_list()
            total = single_topic.select('duration').sum().to_series().to_list()
            temp.append({'topic': t1[0], 'minutes': total[0]})
        final_list.append({'program': single_program.select('program').unique()
                           .to_series().to_list()[0], 'topics': temp})

    return { "political group": name, "channel": channel, "programs": final_list }

# -------------------------------------------------------

@app.get("/v1/minutes-channel-per-politician/{channel}/{name}")
async def get_minutes_channel_per_politician(
    channel: str,
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind),
):
    """
    Return all the programs in a channel, and how many minutes a politician has intervened
    """

    filtered_data = data

    if channel not in channels:
        raise HTTPException(status_code=400, detail="Channel not found")
    all_programs_channel = filtered_data.filter(pl.col('channel') == channel)

    if name not in politicians_list:
        raise HTTPException(status_code=400, detail="Politician not found")

    all_programs_channel = filter_data(all_programs_channel, start_date_, end_date_, kind_)
    programs_channel = all_programs_channel.select('program').unique().to_series().to_list()
    programs_channel.sort()

    first_year = int(start_date_.split('/')[0])
    last_year = int(end_date_.split('/')[0])

    final_programs = []

    for prog in programs_channel:
        temp_prgrm = {}
        temp_prgrm["program"] = prog
        temp = all_programs_channel.filter(pl.col('program') == prog)
        temp = temp.filter(pl.col('fullname') == name)
        datas = {}
        m_y = first_year
        x_y = last_year
        while m_y != (x_y + 1):
            fy = datetime.strptime(str(m_y), '%Y')
            ly = datetime.strptime(str(m_y + 1), '%Y')
            a = temp.filter(pl.col('day') >= fy)
            a = a.filter(pl.col('day') <= ly)
            datas[m_y] = a.select('duration').sum().to_series().to_list()[0]
            m_y += 1
        temp_prgrm["data"] = datas
        final_programs.append(temp_prgrm)

    return { "politician": name, "programs": final_programs }


@app.get("/v1/minutes-channel-per-political-group/{channel}/{name}")
async def get_minutes_channel_per_political_group(
    channel: str,
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind),
    topic_: str = Query(default = "all", description="Topic")
):
    """
    Return all the programs in a channel, and how many minutes a political group has intervened
    """

    filtered_data = data

    if channel not in channels:
        raise HTTPException(status_code=400, detail="Channel not found")
    all_programs_channel = filtered_data.filter(pl.col('channel') == channel)

    if name not in political_groups_list:
        raise HTTPException(status_code=400, detail="Political group not found")

    all_programs_channel = filter_data(all_programs_channel, start_date_, end_date_, kind_)
    programs_channel = all_programs_channel.select('program').unique().to_series().to_list()
    programs_channel.sort()

    first_year = int(start_date_.split('/')[0])
    last_year = int(end_date_.split('/')[0])

    final_programs = []

    for prog in programs_channel:
        temp_prgrm = {}
        temp_prgrm["program"] = prog
        temp = all_programs_channel.filter(pl.col('program') == prog)
        temp = temp.filter(pl.col('lastname') == name)
        datas = {}
        m_y = first_year
        x_y = last_year
        while m_y != (x_y + 1):
            fy = datetime.strptime(str(m_y), '%Y')
            ly = datetime.strptime(str(m_y + 1), '%Y')
            a = temp.filter(pl.col('day') >= fy)
            a = a.filter(pl.col('day') <= ly)
            datas[m_y] = a.select('duration').sum().to_series().to_list()[0]
            m_y += 1
        temp_prgrm["data"] = datas
        final_programs.append(temp_prgrm)

    return { "political group": name, "programs": final_programs }
