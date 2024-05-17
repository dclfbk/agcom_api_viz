from fastapi import FastAPI, Query
from starlette.responses import FileResponse
import pandas as pd
import os
import warnings
from datetime import datetime
import calendar
warnings.filterwarnings('ignore')

# potential APIs:
#     - /polGroupTopics --> how much a political group talked about all the possible topics
#     - /politicianChannels --> how much a politician talked in a specific channel
#     - /polGroupChannels --> how much a political group talked in a specific channel
#     - /politiciansInAffiliation --> how many politicians participate (have participated) in every political group
#     - /interventionsPoliticalGroup --> how much a political group has intervened in tv
#     - /politicianPoliticalGroups --> how many political groups a politician have participated in


data = pd.read_parquet(".." + os.sep + "docs"  + os.sep +  "data" + os.sep + "agcomdata.parquet")

data["fullname"] = data['name'] + " " + data['lastname']
data['day'] = pd.to_datetime(data['day'])

data['kind'] = data['kind'].replace({'Parola': 'speech', 'Notizia': 'news'})

start_date = data.day.min().strftime('%d/%m/%Y')
end_date = data.day.max().strftime('%d/%m/%Y')

kind = ["speech", "news", "both"]

politicians = data[data.name != "political movement"]
political_groups = data[data.name == "political movement"]
topics = data.topic.unique().tolist()


def validate_date(date_str):
    """
    Function to validate dates (from string to datetime)
    """
    try:
        return datetime.strptime(date_str, '%d/%m/%Y')
    except ValueError as exc:
        raise ValueError("Incorrect date format, should be DD-MM-YYYY") from exc


def filter_data(data_, start_date_, end_date_, kind_):
    """
    Function to filter data by selected date interval and kind
    """
    start_date_ = validate_date(start_date_)
    end_date_ = validate_date(end_date_)

    filtered_data = data_[
        (data_.day >= start_date_) &
        (data_.day <= end_date_)]

    if kind_ != "both":
        filtered_data = filtered_data[filtered_data.kind == kind_]

    return filtered_data



app = FastAPI()

@app.get("/")
async def read_index():
    """
    Serve the index.html file.
    """
    return FileResponse('index.html')


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
    politicians_ = politicians_.fullname.unique().tolist()

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
    political_groups_ = political_groups_.lastname.unique().tolist()

    return { "political_groups": political_groups_ }


@app.get("/v1/channels")
async def get_channels():
    """
    Return all channels
    """

    channels = data.channel.unique().tolist()

    return { "channels": channels }


@app.get("/v1/programs")
async def get_programs():
    """
    Return all programs
    """
    programs = data.program.unique().tolist()

    return { "programs": programs }


@app.get("/v1/affiliations")
async def get_affiliations():
    """
    Return all affiliations
    """

    affiliations = data.affiliation.unique().tolist()

    return { "affiliations": affiliations }


@app.get("/v1/topics")
async def get_topics():
    """
    Return all topics
    """

    topics = data.topic.unique().tolist()

    return { "topics": topics }


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
    politician_topics = filter_data(politicians, start_date_, end_date_, kind_)

    politician_topics = politician_topics[(politician_topics.name + politician_topics.lastname) == name]
    topic_list = []

    for t in topics:
        temp = politician_topics[politician_topics.topic == t]
        total = temp["duration"].sum()
        interventions = temp.shape[0]
        topic_list.append({"topic": t, "minutes" : total, "interventions" : interventions})

    return {"topic": topic_list}