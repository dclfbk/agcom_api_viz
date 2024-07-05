from fastapi import FastAPI, Query
from fastapi.staticfiles import StaticFiles
from starlette.responses import FileResponse
import polars as pl
import os
import warnings
from datetime import datetime
import datetime as dt
import calendar
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
political_groups = data.filter(pl.col("name") == "political movement")

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
        raise ValueError("Incorrect date format, should be DD-MM-YYYY") from exc


def filter_data(data_, start_date_, end_date_, kind_):
    """
    Function to filter data by selected date interval and kind
    """
    start_date_ = validate_date(start_date_)
    end_date_ = validate_date(end_date_)

    filtered_data = data_.filter(pl.col("day") >= start_date_).filter(pl.col("day") <= end_date_)


    if kind_ != "both":
        filtered_data = filtered_data.filter(pl.col("kind") == kind_)

    return filtered_data

# -------------------------------------------------------

app = FastAPI()

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


@app.get("/v1/political_groups")
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

@app.get("/v1/politicianTopics/{name}")
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


@app.get("/v1/polGroupTopics/{name}")
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

@app.get("/v1/politicianChannels/{name}")
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


@app.get("/v1/polGroupChannels/{name}")
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

@app.get("/v1/politiciansAffiliation/{name}")
async def get_politicians_affiliation(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how many politicians participate (have participated) in an affiliation
    """
    politicians_affiliation = politicians.filter(pl.col('affiliation') == name)
    politicians_affiliation = filter_data(politicians_affiliation, start_date_, end_date_, kind_)
    final_list = politicians_affiliation.select('fullname').unique().to_series().to_list()

    return { "affiliation": name, "politicians": final_list }


@app.get("/v1/affiliationsPolitician/{name}")
async def get_affiliations_politician(
    name: str,
    start_date_: str = Query(default = start_date, description="Start date"),
    end_date_: str = Query(default = end_date, description="End date"),
    kind_: str = Query(default = "both" , description="Type of data", enum = kind)
):
    """
    Return how many affiliations a politician have participated in
    """

    affiliations_politician = politicians.filter(pl.col('fullname') == name)
    affiliations_politician = filter_data(affiliations_politician, start_date_, end_date_, kind_)
    final_list = affiliations_politician.select('affiliation').unique().to_series().to_list()

    return { "politician": name, "affiliations": final_list}

# -------------------------------------------------------

@app.get("/v1/interventionsPoliticalGroup/{name}")
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

@app.get("/v1/interventionsPoliticianPerYear/{name}")
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


@app.get("/v1/interventionsPoliticalGroupPerYear/{name}")
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

@app.get("/v1/interventionsPoliticianPerDay/{name}")
async def get_interventions_politician_per_day(
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


@app.get("/v1/interventionsPoliticalGroupPerDay/{name}")
async def get_interventions_political_group_per_day(
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
