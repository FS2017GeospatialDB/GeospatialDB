var map = (function() {

	// Private Variables
	var infoWindow = null;
	var map = null;

	function initMap() {
		// Create the Map
		infoWindow = new google.maps.InfoWindow({})
		map = new google.maps.Map(document.getElementById('map'), {
			zoom: 15,
			center: {lat: 39.7488835, lng: -105.2167468}	/* Fix This! */
		});

		// Listen for Feature Clicks
		map.data.addListener('click', function(event) {
			infoWindow.setPosition(event.latLng);
			infoWindow.setContent('<b>Id:</b> ' + event.feature.getId());
			infoWindow.open(map);
		});

		// Style the Map
		map.data.setStyle({
			icon: 'img/point_icon.png',
			fillColor: 'green'
		});
	}

	function submitQuery() {
		var bounds = map.getBounds();
		var east = bounds.getNorthEast().lng();
		var west = bounds.getSouthWest().lng();
		var north = bounds.getNorthEast().lat();
		var south = bounds.getSouthWest().lat();

		var transport = new Thrift.TXHRTransport("http://localhost:8000/service");
		var protocol = new Thrift.TJSONProtocol(transport);
		var client = new GeolocationServiceClient(protocol);
		var result = client.getFeatures(west, east, south, north);

		// Clear the Map
		map.data.forEach(function(feature) {
			map.data.remove(feature);
		});

		// Add new GeoJSON's to Map
		for (var i = 0; i < result.length; i++) {
			map.data.addGeoJson(JSON.parse(result[i].json));
		}
	}

	// Module Exports
	return {
		initMap: initMap,
		submitQuery: submitQuery
	};
})();