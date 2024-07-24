async function radarChart3() {
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (
    $("#select_programs").val().length == 0 ||
    (p.checked == false && pg.checked == false)
  ) {
    return 0;
  }
  if (radarChart3Instance !== null) {
    radarChart3Instance.dispose();
  }
  document.getElementById("barChart2").style.display = "none";
  document.getElementById("barChart3").style.display = "none";
  document.getElementById("stackedBarChart").style.display = "none";
  document.getElementById("calendarChart").style.display = "none";
  document.getElementById("lineChart").style.display = "none";
  document.getElementById("lineChart2").style.display = "none";
  document.getElementById("radarChart").style.display = "none";
  document.getElementById("radarChart2").style.display = "none";
  document.getElementById("barPieChart").style.display = "none";
  document.getElementById("tableDiv").style.display = "none";
  document.getElementById("loadingScreen").style.display = "none";

  var rC = document.getElementById("radarChart3");
  rC.style.display = "block";
  radarChart3Instance = echarts.init(rC);
  radarChart3Instance.showLoading();
  if (p.checked == true) {
    var t = "/v1/program-politicians/";
  } else if (pg.checked == true) {
    var t = "/v1/program-political-groups/";
  } else {
    return 0;
  }
  var url_c = "";
  var url_t = "";
  var url_a = "";
  if (
    $("#select_channels").val()[0] != undefined &&
    $("#select_channels").val()[0] != ""
  ) {
    url_c += `&channel_=${$("#select_channels").val()[0]}`;
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
  const selected_program = $("#select_programs").val();
  const selected_pol = $("#select_pol").val();
  var names = [];
  var values = [];
  if (selected_pol != undefined && selected_pol != "") {
    for (const value of selected_pol) {
      const url =
        t +
        selected_program +
        "?start_date_=" +
        start_date.value.replace(/-/g, "%2F") +
        "&end_date_=" +
        end_date.value.replace(/-/g, "%2F") +
        "&kind_=" +
        cb +
        url_a +
        "&name_=" +
        value +
        url_c +
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
    const baseUrl =
      t +
      selected_channel +
      "?start_date_=" +
      start_date.value.replace(/-/g, "%2F") +
      "&end_date_=" +
      end_date.value.replace(/-/g, "%2F") +
      "&kind_=" +
      cb +
      url_a +
      url_p +
      url_t;
    var i = 1;
    while (true) {
      var url = `${baseUrl}&page=${i}`;
      i++;
      const data = await fetchData(url);
      if (data.pol.length == 0) {
        break;
      } else {
        names.push(data["pol"][0].name);
        values.push(data["pol"][0].minutes);
      }
    }
  }

  const maxMinutes = Math.max(...values);

  var indicators = names.map(function (name) {
    return { name: name, max: maxMinutes };
  });
  option = {
    title: {
      text: `${selected_program}`,
      subtext:
        "Here we can analyze how much time a channel dedicated to politicians",
      left: "center",
    },
    tooltip: {
      trigger: "item",
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
            name: "Allocated Budget",
          },
        ],
      },
    ],
  };
  radarChart3Instance.setOption(option);
  radarChart3Instance.hideLoading();
}
