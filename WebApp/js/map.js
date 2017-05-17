function initMap() {
	/* Fix This! */
	var center = {lat: 40.0, lng: -105.0};
	var zoom = 10;

	map = new google.maps.Map(document.getElementById('map'), {
		zoom: zoom,
		center: center
	});
}

var markers = [];

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

	for (var i = 0; i < markers.length; i++) {
		markers[i].setMap(null);
	}
	markers = [];
//	console.log(result);
	for (var i = 0; i < result.length; i++) {
		var latlng = {lat: parseFloat(result[i].longitude), 
			lng: parseFloat(result[i].latitude)};
		var marker = new google.maps.Marker({
			position: latlng,
			map: map,
			title: result[i].id.toString()
		});
		markers.push(marker);
	}

}
