async function lineChart() {
  if ($("#select_pol").val().length == 0) {
    return 0;
  }
  document.getElementById("barChart").style.display = "none";
  document.getElementById("barChart2").style.display = "none";
  document.getElementById("barChart3").style.display = "none";
  document.getElementById("stackedBarChart").style.display = "none";
  document.getElementById("lineChart2").style.display = "none";
  document.getElementById("calendarChart").style.display = "none";
  document.getElementById("radarChart").style.display = "none";
  document.getElementById("barPieChart").style.display = "none";
  document.getElementById("tableDiv").style.display = "none";
  var lC = document.getElementById("lineChart");
  lC.style.display = "block";
  var lineChart = echarts.init(lC);
  lineChart.showLoading();
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (p.checked == true) {
    var t = "/v1/interventions-politician-per-year/";
  } else if (pg.checked == true) {
    var t = "/v1/interventions-political-group-per-year/";
  } else {
    return 0;
  }
  var series = [];
  var years;
  var politicians = [];
  const selectedValues = $("#select_pol").val();
  for (const value of selectedValues) {
    const url =
      t +
      value +
      "?start_date_=" +
      start_date.value.replace(/-/g, "%2F") +
      "&end_date_=" +
      end_date.value.replace(/-/g, "%2F") +
      "&kind_=" +
      cb;
    const data = await fetchData(url);
    years = data["years"];
    var interventions = data["interventions"];
    series.push(interventions);
    if (p.checked == true) {
      politicians.push(data["politician"]);
    } else if (pg.checked == true) {
      politicians.push(data["political group"]);
    }
  }
  option = {
    xAxis: {
      type: "category",
      data: years,
    },
    yAxis: {
      type: "value",
    },
    title: {
      text: "Intervention per year",
      subtext: "Here we can analyze the interventions throughout the years",
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
  };
  lineChart.setOption(option);
  lineChart.hideLoading();
}
