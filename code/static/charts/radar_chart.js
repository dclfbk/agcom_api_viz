async function radarChart() {
  var rC = document.getElementById("radarChart");
  document.getElementById("barChart2").style.display = "none";
  document.getElementById("barChart3").style.display = "none";
  document.getElementById("stackedBarChart").style.display = "none";
  document.getElementById("calendarChart").style.display = "none";
  document.getElementById("lineChart").style.display = "none";
  document.getElementById("lineChart2").style.display = "none";
  document.getElementById("radarChart2").style.display = "none";
  document.getElementById("radarChart3").style.display = "none";
  document.getElementById("barPieChart").style.display = "none";
  document.getElementById("tableDiv").style.display = "none";
  document.getElementById("loadingScreen").style.display = "none";
  rC.style.display = "block";
  if (radarChartInstance !== null) {
    radarChartInstance.dispose();
  }
  document.querySelector(".card-title").innerHTML =
    "Charts <span>/Radar Chart</span>";
  if (
    $("#select_pol").val()[0] == undefined ||
    $("#select_pol").val()[0] == ""
  ) {
    document.querySelector(".card-title").innerHTML =
      "Charts <span>/Radar Chart <br><br> You need to select at least a politician/political group to use this chart</span>";
    return 0;
  }
  radarChartInstance = echarts.init(rC);
  radarChartInstance.showLoading();
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (p.checked == true) {
    var t = "/v1/politician-topics/";
  } else if (pg.checked == true) {
    var t = "/v1/political-group-topics/";
  } else {
    return 0;
  }
  var politicians = [];
  var values = [];
  var url_c = "";
  var url_p = "";
  var url_t = "";
  var url_a = "";
  if (
    $("#select_channels").val()[0] != undefined &&
    $("#select_channels").val()[0] != ""
  ) {
    url_c += `&channel_=${$("#select_channels").val()[0]}`;
  }
  if (
    $("#select_programs").val()[0] != undefined &&
    $("#select_programs").val()[0] != ""
  ) {
    url_p += `&program_=${$("#select_programs").val()[0]}`;
  }
  if (
    $("#select_topics").val()[0] != undefined &&
    $("#select_topics").val()[0] != ""
  ) {
    url_t += `&topic_=${$("#select_topics").val()[0]}`;
  }
  if (
    $("#select_affiliations").val()[0] != undefined &&
    $("#select_affiliations").val()[0] != ""
  ) {
    url_a += `&affiliation_=${$("#select_affiliations").val()[0]}`;
  }
  const selectedValues = $("#select_pol").val();
  for (const value of selectedValues) {
    var i = 1;
    const baseUrl =
      t +
      value +
      "?start_date_=" +
      start_date.value.replace(/-/g, "%2F") +
      "&end_date_=" +
      end_date.value.replace(/-/g, "%2F") +
      "&kind_=" +
      cb +
      url_a +
      url_c +
      url_p +
      url_t;
    var temp_values = [];
    while (true) {
      var url = `${baseUrl}&page=${i}`;
      i++;
      const data = await fetchData(url);
      if (data.topics.length == 0) {
        break;
      } else {
        temp_values.push(data["topics"][0]);
      }
    }
    values.push(temp_values);
    politicians.push(value);
  }
  var minutes = [];
  var topics = [];
  values.forEach(function (value) {
    var temp = [];
    value.forEach(function (v) {
      temp.push(v["minutes"]);
    });
    minutes.push(temp);
  });
  max_min = [];
  values[0].forEach(function (t) {
    topics.push(t["topic"]);
    max_min.push(0);
  });
  values.forEach(function (data) {
    data.forEach(function (x, index) {
      if (max_min[index] < x["minutes"]) {
        max_min[index] = x["minutes"] + x["minutes"] / 5;
      } else {
      }
    });
  });
  var indicator = [];
  var data = [];
  topics.forEach(function (f, index) {
    indicator.push({ name: f, max: max_min[index] });
  });
  politicians.forEach(function (p, index) {
    data.push({ name: p, value: minutes[index] });
  });
  option = {
    title: {
      subtext:
        "show how many interventions a politician made about all topics and compare up to 4 politician/political groups",
      left: "center",
    },
    legend: {
      top: 50,
      data: politicians,
      left: "left",
      orient: "vertical",
      formatter: function (name) {
        if (name.length > 35) {
          var index = 35;
          while (name.charAt(index) !== " " && index > 0) {
            index--;
          }
          if (index === 0) {
            index = 35;
          }
          return name.substr(0, index) + "\n" + name.substr(index);
        } else {
          return name;
        }
      },
    },
    tooltip: {
      trigger: "item",
    },
    radar: {
      indicator: indicator,
      center: ["50%", "55%"],
    },
    series: [
      {
        name: "Minutes politicians",
        type: "radar",
        data: data,
      },
    ],
  };
  radarChartInstance.setOption(option);
  radarChartInstance.hideLoading();
}
