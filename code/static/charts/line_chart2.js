async function lineChart2() {
  if (lineChart2Instance !== null) {
    lineChart2Instance.dispose();
  }
  $("#select_pol").select2({
    maximumSelectionLength: 4,
  });
  document.querySelector(".card-title").innerHTML =
    "Charts <span>/Line Chart 2</span>";
  if (
    $("#select_pol").val()[0] == undefined ||
    $("#select_pol").val()[0] == ""
  ) {
    document.querySelector(".card-title").innerHTML =
      "Charts <span>/Line Chart 2<br><br> You need to select at least a politician/political group to use this chart</span>";
    return 0;
  }
  document.getElementById("barChart2").style.display = "none";
  document.getElementById("barChart3").style.display = "none";
  document.getElementById("stackedBarChart").style.display = "none";
  document.getElementById("calendarChart").style.display = "none";
  document.getElementById("lineChart").style.display = "none";
  document.getElementById("radarChart").style.display = "none";
  document.getElementById("radarChart2").style.display = "none";
  document.getElementById("radarChart3").style.display = "none";
  document.getElementById("barPieChart").style.display = "none";
  document.getElementById("tableDiv").style.display = "none";
  document.getElementById("loadingScreen").style.display = "none";
  var lC = document.getElementById("lineChart2");
  lC.style.display = "block";
  lineChart2Instance = echarts.init(lC);
  lineChart2Instance.showLoading();
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (p.checked == true) {
    var t = "/v1/interventions-politician-per-day/";
  } else if (pg.checked == true) {
    var t = "/v1/interventions-political-group-per-day/";
  } else {
    return 0;
  }
  var series = [];
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
    var values = [];
    var i = 1;
    while (true) {
      var url = `${baseUrl}&page=${i}`;
      i++;
      const data = await fetchData(url);
      if (data.interventions.length == 0) {
        break;
      } else {
        values.push(data["interventions"][0]);
      }
    }
    series.push(values);
    politicians.push(value);
  }
  series.forEach((pol, index) => {
    var f = [];
    pol.forEach((year) => {
      f = f.concat(year);
    });
    series[index] = f;
  });
  option = {
    xAxis: {
      type: "category",
    },
    yAxis: {
      type: "value",
    },
    title: {
      text: "Intervention per year",
      subtext: "Here we can analyze the interventions each day",
      left: "center",
    },
    legend: {
      data: politicians,
      left: "right",
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
    series: [
      {
        name: politicians[0],
        data: series[0],
        type: "line",
      },
      {
        name: politicians[1],
        data: series[1],
        type: "line",
      },
      {
        name: politicians[2],
        data: series[2],
        type: "line",
      },
      {
        name: politicians[3],
        data: series[3],
        type: "line",
      },
    ],
    dataZoom: [
      {
        id: "dataZoomX",
        type: "slider",
        xAxisIndex: [0],
        filterMode: "filter",
      },
    ],
  };
  lineChart2Instance.setOption(option);
  lineChart2Instance.hideLoading();
}
