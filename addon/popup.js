
function show_results(id1, id2) {
  chrome.tabs.query({'active': true, 'lastFocusedWindow': true}, function (tabs) {
    var url_string = tabs[0].url;

    var date_picker = document.getElementById('date-picker');

    var url = new URL(url_string);
    if (url.hostname == "www.airbnb.com") {
      // Get the room id
      if(url.pathname.startsWith("/rooms/")) {
        var path_str = url.pathname;
        var listing_id = path_str.substring(path_str.lastIndexOf("/") + 1);
        var listing_date = date_picker.value;
        fetch(`http://18.221.216.108/check-listing?id=${listing_id}&date=${listing_date}`).then(r => r.text()).then(result => {
          //display results
          if (result)
          {
            var res = result.split(",");
            if (res[0] == '999')
            {
              document.getElementById(id1).innerHTML = "Booking closed for selected date.";
              document.getElementById(id2).innerHTML = 'Average monthly lead time is ' + res[1] + ' days.';
            }
            else
            {
              document.getElementById(id1).innerHTML = 'Lead time for selected date is ' + res[0] + ' days.';
              document.getElementById(id2).innerHTML = 'Average monthly lead time is ' + res[1] + ' days.';
            }
          }
          else
          {
            document.getElementById(id1).innerHTML = "No historical data is available for this listing.";
          }
          });
      }
    }
  });
}


document.addEventListener('DOMContentLoaded', function () {
  var button = document.getElementById('checkPage');
  button.addEventListener('click', function()
    {show_results('lead-time', 'month-avg');}
  );
});
