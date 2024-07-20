async function calendarChart() {
  if (calendarChartInstance !== null) {
    calendarChartInstance.dispose();
  }
  if ($("#select_pol").val().length == 0) {
    return 0;
  }
  document.getElementById("barChart").style.display = "none";
  document.getElementById("barChart2").style.display = "none";
  document.getElementById("barChart3").style.display = "none";
  document.getElementById("stackedBarChart").style.display = "none";
  document.getElementById("lineChart").style.display = "none";
  document.getElementById("lineChart2").style.display = "none";
  document.getElementById("radarChart").style.display = "none";
  document.getElementById("barPieChart").style.display = "none";
  document.getElementById("tableDiv").style.display = "none";
  var cC = document.getElementById("calendarChart");
  cC.style.display = "block";
  calendarChartInstance = echarts.init(cC);
  calendarChartInstance.showLoading();
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (p.checked == true) {
    var t = "/v1/interventions-politician-per-day/";
  } else if (pg.checked == true) {
    var t = "/v1/interventions-political-group-per-day/";
  } else {
    return 0;
  }
  const url =
    t +
    $("#select_pol").val()[0] +
    "?start_date_=" +
    start_date.value.replace(/-/g, "%2F") +
    "&end_date_=" +
    end_date.value.replace(/-/g, "%2F") +
    "&kind_=" +
    cb;
  fetchData(url).then((result) => {
    var interv = result["interventions"];
    var max_value = result["max_value"];
    var begin_year = result["begin year"];
    var final_year = result["final year"];
    var calendar = [];
    var series = [];
    interv.forEach(function (x, index) {
      x.forEach(function (y, index2) {
        interv[index][index2][0] = +echarts.time.parse(
          interv[index][index2][0]
        );
      });
    });
    for (var i = begin_year; i <= final_year; i++) {
      calendar.push({
        top: 100 + (i - begin_year) * 200,
        range: i.toString(),
        cellSize: ["auto", 20],
      });
    }
    for (var i = begin_year; i <= final_year; i++) {
      series.push({
        type: "heatmap",
        coordinateSystem: "calendar",
        calendarIndex: i - begin_year,
        data: interv[i - begin_year],
      });
    }
    option = {
      tooltip: {
        position: "top",
        formatter: function (params) {
          const date = echarts.time.format(
            params.value[0],
            "{dd}-{MM}-{yyyy}",
            false
          );
          var result = "";
          result += '<div style="display: flex; align-items: center;">';
          result +=
            '<span style="display:inline-block;margin-right:5px;border-radius:50%;width:10px;height:10px;background-color:' +
            params.color +
            ';"></span>';
          result += `<b style="padding-right: 10px;">${date}: </b>${params.value[1]} interventions`;
          result += "</div>";
          return result;
        },
      },
      visualMap: {
        min: 0,
        max: max_value,
        calculable: true,
        orient: "horizontal",
        left: "center",
        top: "top",
      },
      calendar: calendar,
      series: series,
    };
    calendarChartInstance.setOption(option);
    calendarChartInstance.hideLoading();
  });
}
