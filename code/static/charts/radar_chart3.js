async function radarChart3() {
  controller.abort();
  while(functionIsRunning){
    await new Promise((resolve) => setTimeout(resolve, 50));
    if (!controller.signal.aborted){
      return;
    }
  }
  functionIsRunning = true;
  controller = new AbortController();
  if(select_pol_length != 10){
    select_pol_length = 10;
    selectPolLength10();
  }

  var rC = document.getElementById("radarChart3");
  rC.style.display = "block";
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
  const p = document.getElementById("politician");
  const pg = document.getElementById("political_group");
  if (radarChart3Instance !== null) {
    radarChart3Instance.dispose();
  }
  document.querySelector(".card-title").innerHTML =
    "<u>Analisi Programma</u><span>&nbsp&nbsp&nbsp/seleziona fino a 10 politici-partiti </span><br><br>Maggiori esponenti nel programma <br><br><span>Questo grafico visualizza i dieci esponenti o gruppi politici che hanno avuto maggiore visibilità in un determinato programma televisivo, mostrando il tempo totale degli interventi e delle notizie trasmesse. <br>Ogni asse del grafico rappresenta un politico o partito, con il punto sull’asse che indica il valore corrispondente: quanto più il punto è vicino al centro del poligono, tanto più basso è il tempo degli interventi. <br>Questo grafico consente di analizzare il tempo dedicato ai politici o partiti sul programma, permettendo di valutare l'equità e la neutralità nella copertura mediatica. <br><br>È anche possibile selezionare fino a dieci politici o partiti specifici per confrontare la loro partecipazione nel programma. </span>";
  if (
    $("#select_programs").val()[0] == undefined ||
    $("#select_programs").val()[0] == "" ||
    (p.checked == false && pg.checked == false)
  ) {
    document.querySelector(".card-title").innerHTML =
      "<u>Analisi Programma</u><span>&nbsp&nbsp&nbsp/seleziona fino a 10 politici-partiti </span><br><br>Maggiori esponenti nel programma <br><br><span>É necessario selezionare almeno un programma e scegliere tra Politici e Partiti per utilizzare questo grafico. </span>";
    functionIsRunning = false;
    return 0;
  }
  radarChart3Instance = echarts.init(rC);
  radarChart3Instance.showLoading();
  if (p.checked == true) {
    var t = "/v1/program-politicians/";
  } else if (pg.checked == true) {
    var t = "/v1/program-political-groups/";
  } else {
    functionIsRunning = false;
    return 0;
  }
  var url_c = "";
  var url_t = "";
  var url_a = "";
  if (
    $("#select_channels").val()[0] != undefined &&
    $("#select_channels").val()[0] != ""
  ) {
    url_c += `&channel_=${encodeURIComponent($("#select_channels").val()[0])}`;
  }
  var topic_index = 0;
  while ($("#select_topics").val()[topic_index] != undefined){
    if (
      $("#select_topics").val()[topic_index] != undefined &&
      $("#select_topics").val()[topic_index] != ""
    ) {
      url_t += `&topics_list=${encodeURIComponent($("#select_topics").val()[topic_index])}`;
    }
    topic_index++;
  }
  if (
    $("#select_affiliations").val()[0] != undefined &&
    $("#select_affiliations").val()[0] != ""
  ) {
    url_a += `&affiliation_=${encodeURIComponent(
      $("#select_affiliations").val()[0]
    )}`;
  }
  const selected_program = $("#select_programs").val();
  const selected_pol = $("#select_pol").val();
  var names = [];
  var values = [];
  if (selected_pol != undefined && selected_pol != "") {
    for (const value of selected_pol) {
      if (controller.signal.aborted) {
        functionIsRunning = false;
        return;
      }
      const url =
        t +
        encodeURIComponent(selected_program) +
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
      if (!data || data.pol.length == 0) {
        continue;
      }
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
      encodeURIComponent(selected_program) +
      "?start_date_=" +
      start_date.value.replace(/-/g, "%2F") +
      "&end_date_=" +
      end_date.value.replace(/-/g, "%2F") +
      "&kind_=" +
      cb +
      url_a +
      url_c +
      url_t;
    const data = await fetchData(url);
    if(!data || data.pol.length == 0){
    } else {
      data.pol.forEach((p) => {
        names.push(p.name);
        values.push(p.minutes);
      });
    }
  }

  if (
    values.every(function (element) {
      return element === 0;
    }) == true
  ) {
    document.querySelector(".card-title").innerHTML =
      "Charts <span>/Radar Chart</span> <br><br> NO DATA FOUND";
    radarChart3Instance.hideLoading();
    functionIsRunning = false;
    return 0;
  }

  const maxMinutes = Math.max(...values);

  var indicators = names.map(function (name) {
    return { name: name, max: maxMinutes };
  });
  option = {
    title: {
      text: `${selected_program}`,
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
            name: "Minutes: ",
          },
        ],
      },
    ],
  };
  radarChart3Instance.setOption(option);
  radarChart3Instance.hideLoading();
  functionIsRunning = false;
}
