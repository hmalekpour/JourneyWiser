
function myFunction() {
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
        fetch(`http://18.188.79.176/check-listing?id=${listing_id}&date=${listing_date}`).then(r => r.text()).then(result => {
          // Result now contains the response text, do what you want...
          alert(result)
        })
      }
    }
  });
} 

document.addEventListener('DOMContentLoaded', function () {
  var button = document.getElementById('checkPage');
  button.addEventListener('click', myFunction);
});