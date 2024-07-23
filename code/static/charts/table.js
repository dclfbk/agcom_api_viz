async function table(tab, still_running) {
  document.getElementById("tableDiv").style.display = "none";
  document.getElementById("loadingScreen").style.display = "block";

  tab = $("#table").DataTable();
  tab.clear();
  if (
    $("#select_pol").val()[0] == undefined ||
    $("#select_pol").val()[0] == ""
  ) {
    still_running = false;
    document.getElementById("loadingScreen").style.display = "none";
    return 0;
  }
  document.getElementById("barChart2").style.display = "none";
  document.getElementById("barChart3").style.display = "none";
  document.getElementById("stackedBarChart").style.display = "none";
  document.getElementById("calendarChart").style.display = "none";
  document.getElementById("lineChart").style.display = "none";
  document.getElementById("lineChart2").style.display = "none";
  document.getElementById("radarChart").style.display = "none";
  document.getElementById("radarChart2").style.display = "none";
  document.getElementById("radarChart3").style.display = "none";
  document.getElementById("barPieChart").style.display = "none";
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (p.checked == true) {
    var t = "/v1/channels-programs-topics-politician/";
  } else if (pg.checked == true) {
    var t = "/v1/channels-programs-topics-political-group/";
  } else {
    document.getElementById("loadingScreen").style.display = "none"; // Nascondi la schermata di caricamento
    return 0;
  }
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
  var url =
    t +
    $("#select_pol").val()[0] +
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
  var i = 1;
  while (true) {
    var pageUrl = `${url}&page=${i}`;
    i++;
    var j = 1;
    var data = 0;
    while (true) {
      var programPageUrl = `${pageUrl}&program_page=${j}`;
      j++;
      data = await fetchData(programPageUrl);
      if (data.channels[0].programs.length === 0) {
        break;
      }
      data.channels[0].programs[0].topics.forEach((tp) => {
        if (still_running) {
          var newData = [
            data.channels[0].channel,
            data.channels[0].programs[0].program,
            tp.topic,
            calcTime(tp.minutes),
          ];
          tab.row.add(newData).draw(false);
        }
      });
    }
    if (i > data.total_pages) {
      break;
    }
  }

  if (document.getElementById("loadingScreen").style.display != "none") {
    document.getElementById("loadingScreen").style.display = "none";
    document.getElementById("tableDiv").style.display = "block";
  }
}
