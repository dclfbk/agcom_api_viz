//update charts
async function updateCharts() {
  if (document.getElementById("barChart2").style.display == "block") {
    barChart2();
  } else if (document.getElementById("barChart3").style.display == "block") {
    barChart3();
  } else if (
    document.getElementById("calendarChart").style.display == "block"
  ) {
    calendarChart();
  } else if (document.getElementById("lineChart").style.display == "block") {
    lineChart();
  } else if (document.getElementById("lineChart2").style.display == "block") {
    lineChart2();
  } else if (document.getElementById("radarChart").style.display == "block") {
    radarChart();
  } else if (document.getElementById("radarChart2").style.display == "block") {
    radarChart2();
  } else if (document.getElementById("radarChart3").style.display == "block") {
    radarChart3();
  } else if (document.getElementById("barPieChart").style.display == "block") {
    barPieChart();
  } else if (
    document.getElementById("tableDiv").style.display == "block" ||
    document.getElementById("loadingScreen").style.display == "block"
  ) {
    table();
  } else {
    console.log("hello");
  }
}
