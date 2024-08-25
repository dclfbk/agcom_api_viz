async function radarChart2() {
  var rC = document.getElementById("radarChart2");
  rC.style.display = "block";
  document.getElementById("barChart2").style.display = "none";
  document.getElementById("barChart3").style.display = "none";
  document.getElementById("stackedBarChart").style.display = "none";
  document.getElementById("calendarChart").style.display = "none";
  document.getElementById("lineChart").style.display = "none";
  document.getElementById("lineChart2").style.display = "none";
  document.getElementById("radarChart").style.display = "none";
  document.getElementById("radarChart3").style.display = "none";
  document.getElementById("barPieChart").style.display = "none";
  document.getElementById("tableDiv").style.display = "none";
  document.getElementById("loadingScreen").style.display = "none";
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (radarChart2Instance !== null) {
    radarChart2Instance.dispose();
  }
  document.querySelector(".card-title").innerHTML =
    "Charts <span>/Radar Chart 2</span>";
  if (
    $("#select_channels").val()[0] == undefined ||
    $("#select_channels").val()[0] == "" ||
    (p.checked == false && pg.checked == false)
  ) {
    document.querySelector(".card-title").innerHTML =
      "Charts <span>/Radar Chart 2<br><br> You need to select at least a channel and choose between politicians and political groups to use this chart</span>";
    return 0;
  }
  radarChart2Instance = echarts.init(rC);
  radarChart2Instance.showLoading();
  if (p.checked == true) {
    var t = "/v1/channel-politicians/";
  } else if (pg.checked == true) {
    var t = "/v1/channel-political-groups/";
  } else {
    return 0;
  }
  var url_p = "";
  var url_t = "";
  var url_a = "";
  if (
    $("#select_programs").val()[0] != undefined &&
    $("#select_programs").val()[0] != ""
  ) {
    url_p += `&program_=${encodeURIComponent($("#select_programs").val()[0])}`;
  }
  if (
    $("#select_topics").val()[0] != undefined &&
    $("#select_topics").val()[0] != ""
  ) {
    url_t += `&topic_=${encodeURIComponent($("#select_topics").val()[0])}`;
  }
  if (
    $("#select_affiliations").val()[0] != undefined &&
    $("#select_affiliations").val()[0] != ""
  ) {
    url_a += `&affiliation_=${encodeURIComponent(
      $("#select_affiliations").val()[0]
    )}`;
  }
  const selected_channel = $("#select_channels").val();
  const selected_pol = $("#select_pol").val();
  var names = [];
  var values = [];
  if (selected_pol != undefined && selected_pol != "") {
    for (const value of selected_pol) {
      const url =
        t +
        encodeURIComponent(selected_channel) +
        "?start_date_=" +
        start_date.value.replace(/-/g, "%2F") +
        "&end_date_=" +
        end_date.value.replace(/-/g, "%2F") +
        "&kind_=" +
        cb +
        url_a +
        "&name_=" +
        value +
        url_p +
        url_t;
      const data = await fetchData(url);
      if (data.pol[0] != undefined) {
        names.push(data.pol[0].name);
        values.push(data.pol[0].minutes);
      } else {
        names.push(value);
        values.push(0);
      }
    }
  } else {
    const url =
      t +
      encodeURIComponent(selected_channel) +
      "?start_date_=" +
      start_date.value.replace(/-/g, "%2F") +
      "&end_date_=" +
      end_date.value.replace(/-/g, "%2F") +
      "&kind_=" +
      cb +
      url_a +
      url_p +
      url_t;
    const data = await fetchData(url);
    data.pol.forEach((p) => {
      names.push(p.name);
      values.push(p.minutes);
    });
  }

  if (
    values.every(function (element) {
      return element === 0;
    }) == true
  ) {
    document.querySelector(".card-title").innerHTML =
      "Charts <span>/Radar Chart</span> <br><br> NO DATA FOUND";
    radarChart2Instance.hideLoading();
    return 0;
  }

  const maxMinutes = Math.max(...values);

  var indicators = names.map(function (name) {
    return { name: name, max: maxMinutes };
  });
  option = {
    title: {
      text: `${selected_channel}`,
      subtext:
        "compare the 10 politicians/political groups that intervened the most in a channel, \n if you want you can select the politicians/ political groups to compare!",
      left: "center",
    },
    tooltip: {
      trigger: "item",
      formatter: function (params) {
        var result = "";
        result += '<div style="display: flex; align-items: center;">';
        result +=
          '<span style="display:inline-block;margin-right:5px;border-radius:50%;width:10px;height:10px;background-color:' +
          params.color +
          ';"></span>';
        result +=
          '<b style="padding-right: 10px;">' + params.name + "</b></div>";
        indicators.forEach((vv, index) => {
          result += '<div style="display: flex; align-items: center;">';
          result += '<b style="padding-right: 10px;">' + vv.name + ":</b>";
          result += calcTime(params.value[index]);
          result += "</div>";
        });
        return result;
      },
    },
    radar: {
      indicator: indicators,
    },
    series: [
      {
        name: "Minutes politicians",
        type: "radar",
        data: [
          {
            value: values,
            name: "Minutes:",
          },
        ],
      },
    ],
  };
  radarChart2Instance.setOption(option);
  radarChart2Instance.hideLoading();
}
