async function table(tab, still_running) {
  tab = $("#table").DataTable();
  tab.clear();
  if ($("#select_pol").val().length == 0) {
    still_running = false;
    return 0;
  }
  document.getElementById("tableDiv").style.display = "block";
  document.getElementById("barChart").style.display = "none";
  document.getElementById("barChart2").style.display = "none";
  document.getElementById("barChart3").style.display = "none";
  document.getElementById("stackedBarChart").style.display = "none";
  document.getElementById("calendarChart").style.display = "none";
  document.getElementById("lineChart").style.display = "none";
  document.getElementById("lineChart2").style.display = "none";
  document.getElementById("radarChart").style.display = "none";
  document.getElementById("barPieChart").style.display = "none";
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (p.checked == true) {
    var t = "/v1/channels-programs-topics-politician/";
  } else if (pg.checked == true) {
    var t = "/v1/channels-programs-topics-political-group/";
  } else {
    return 0;
  }
  var url =
    t +
    $("#select_pol").val()[0] +
    "?start_date_=" +
    start_date.value.replace(/-/g, "%2F") +
    "&end_date_=" +
    end_date.value.replace(/-/g, "%2F") +
    "&kind_=" +
    cb;
  var i = 1;
  while (true) {
    url += `&page=${i}`;
    i++;
    var j = 1;
    var data = 0;
    while (true) {
      url += `&program_page=${j}`;
      j++;
      data = await fetchData(url);
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
}
