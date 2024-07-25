async function barChart2() {
  if (barChart2Instance !== null) {
    barChart2Instance.dispose();
  }
  $("#select_pol").select2({
    maximumSelectionLength: 4,
    placeholder: "Cerca politico/partito",
  });
  document.querySelector(".card-title").innerHTML =
    "Charts <span>/Bar Chart</span>";
  if (
    $("#select_pol").val()[0] == undefined ||
    $("#select_pol").val()[0] == ""
  ) {
    document.querySelector(".card-title").innerHTML =
      "Charts <span>/Bar Chart 2<br><br> You need to select at least a politician/political group to use this chart</span>";
    return 0;
  }
  document.getElementById("barChart3").style.display = "none";
  document.getElementById("stackedBarChart").style.display = "none";
  document.getElementById("calendarChart").style.display = "none";
  document.getElementById("lineChart").style.display = "none";
  document.getElementById("lineChart2").style.display = "none";
  document.getElementById("radarChart").style.display = "none";
  document.getElementById("radarChart2").style.display = "none";
  document.getElementById("radarChart3").style.display = "none";
  document.getElementById("barPieChart").style.display = "none";
  document.getElementById("tableDiv").style.display = "none";
  document.getElementById("loadingScreen").style.display = "none";
  var bC = document.getElementById("barChart2");
  bC.style.display = "block";
  barChart2Instance = echarts.init(bC);
  barChart2Instance.showLoading();
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (p.checked == true) {
    var t = "/v1/politician-topics/";
  } else if (pg.checked == true) {
    var t = "/v1/political-group-topics/";
  } else {
    return 0;
  }
  var final_values = [];
  var politicians = [];
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
    var values = [];
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

    while (true) {
      var url = `${baseUrl}&page=${i}`;
      i++;
      const data = await fetchData(url);
      if (data.topics.length == 0) {
        break;
      } else {
        values.push(data["topics"][0]);
      }
    }
    politicians.push(value);
    final_values.push(values);
  }
  console.log(final_values, politicians);
  var topics = [];
  final_values.forEach((v) => {
    var t = [];
    v.forEach((x) => {
      t.push(x["topic"]);
    });
    topics = t;
  });
  var series = [];
  final_values.forEach((v, index) => {
    var m = [];
    v.forEach((x) => {
      m.push(x["minutes"]);
    });
    series.push({ name: politicians[index], type: "bar", data: m });
  });
  var option = {
    tooltip: {
      trigger: "axis",
      axisPointer: {
        type: "shadow",
      },
      formatter: function (params) {
        var result = "";
        result += "<b>" + params[0].name + "</b>";
        params.forEach((item, index) => {
          result += '<div style="display: flex; align-items: center;">';
          result +=
            '<span style="display:inline-block;margin-right:5px;border-radius:50%;width:10px;height:10px;background-color:' +
            item.color +
            ';"></span>';
          result +=
            '<b style="padding-right: 10px;">' +
            params[index].seriesName +
            ":</b>";
          result += calcTime(params[index].data);
          result += "</div>";
        });
        return result;
      },
    },
    legend: {},
    grid: {
      left: "3%",
      right: "4%",
      bottom: "3%",
      containLabel: true,
    },
    yAxis: {
      type: "value",
      boundaryGap: [0, 0.01],
    },
    xAxis: {
      type: "category",
      data: topics,
      show: false,
    },
    series: series,
  };
  barChart2Instance.setOption(option);
  barChart2Instance.hideLoading();
  var removedElements = [];
  var removedTopics = [];
  var removedIndex = [];
  var counter = 0;

  barChart2Instance.on("click", function (p) {
    if (p.componentType == "series") {
      counter++;
      var t;
      var tempList = [];
      series.forEach((v) => {
        if (v["data"].indexOf(p.data) != -1) {
          t = v["data"].indexOf(p.data);
        }
      });
      series.forEach((v, index) => {
        tempList.push(v["data"][t]);
        v["data"].splice(t, 1);
        series[index] = v;
      });
      removedIndex.push(t);
      removedTopics.push(topics[t]);
      topics.splice(t, 1);
      removedElements.push(tempList);
      var option2 = {
        tooltip: {
          trigger: "axis",
          axisPointer: {
            type: "shadow",
          },
        },
        legend: {},
        grid: {
          left: "3%",
          right: "4%",
          bottom: "3%",
          containLabel: true,
        },
        yAxis: {
          type: "value",
          boundaryGap: [0, 0.01],
        },
        xAxis: {
          type: "category",
          data: topics,
          show: false,
        },
        series: series,
        graphic: [
          {
            type: "text",
            left: 50,
            top: 20,
            style: {
              text: "Back",
              fontSize: 18,
            },
            onclick: function () {
              if (counter > 0) {
                var rt = removedTopics.splice(removedTopics.length - 1, 1);
                var re = removedElements.splice(removedElements.length - 1, 1);
                var ri = removedIndex.splice(removedIndex.length - 1, 1);
                topics.splice(ri[0], 0, rt[0]);
                series.forEach((ser, index) => {
                  ser["data"].splice(ri[0], 0, re[0][index]);
                });
                option.series = series;
                barChart2Instance.setOption(option);
                counter--;
              }
            },
          },
        ],
      };
      barChart2Instance.setOption(option2);
    }
  });
}
