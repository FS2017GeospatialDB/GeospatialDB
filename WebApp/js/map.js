function initMap() {
	/* Fix This! */
	var center = {lat: 39.7488835, lng: -105.2167468};
	var zoom = 15;

	map = new google.maps.Map(document.getElementById('map'), {
		zoom: zoom,
		center: center
	});
}

var markers = [];
var activeInfoWindow = null;

function submitQuery() {
	var bounds = map.getBounds();
	var ne = bounds.getNorthEast();
	var sw = bounds.getSouthWest();
	var east = ne.lng();
	var west = sw.lng();
	var north = ne.lat();
	var south = sw.lat();

	var transport = new Thrift.TXHRTransport("http://localhost:8000/service");
	var protocol = new Thrift.TJSONProtocol(transport);
	var client = new GeolocationServiceClient(protocol);
	var result = client.getFeatures(west, east, south, north);

	clearMap();
	for (var i = 0; i < result.length; i++) {
		// Parse Feature GeoJSON
		var json = JSON.parse(result[i].json)
		var latlng = {
			lat: json.geometry.coordinates[1], 
			lng: json.geometry.coordinates[0]
		}
		
		// Generate Marker
		var marker = new google.maps.Marker({
			position: latlng,
			map: map,
			title: json.id
		});
		markers.push(marker);

		// Generate Info Window
		var infowindow = new google.maps.InfoWindow({
			content: '<b>Id:</b> ' + json.id + '<br><b>Time:</b> ' + result[i].time
				+ '<br><br><b>JSON:</b> ' + JSON.stringify(json)
		});

		// Add Click Listener
		google.maps.event.addListener(marker, 'click', (function(marker, infoWindow) {
			return function() {

				// Mutual Exclusion
				if (activeInfoWindow !== null)
					activeInfoWindow.close();
				activeInfoWindow = infoWindow;

				infoWindow.open(map, marker);
			};
		})(marker, infowindow));
	}
}

function clearMap() {
	for (var i = 0; i < markers.length; i++)
		markers[i].setMap(null);
	markers = [];
}
