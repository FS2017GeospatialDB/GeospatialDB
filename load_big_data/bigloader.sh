#!/bin/bash
file=${1}
for lat in $(seq 37.5 0.5 41)
do
	for lon in $(seq -109 0.5 -102)
	do
		l=$(echo ${lon}-0.5 | bc)
		b=$(echo ${lat}-0.5 | bc)
		echo "Top: ${lat}"
		echo "Bot: ${b}"
		echo "Left: ${l}"
		echo "Right: ${lon}"

		~/Downloads/osmosis/bin/osmosis --read-pbf file=${file} --bounding-box top=${lat} left=${l} bottom=${b} right=${lon} --write-xml file=temp.osm
		
		echo Converting to GeoJSON...
		node --max_old_space_size=8192 /usr/local/lib/node_modules/osmtogeojson/osmtogeojson temp.osm > temp.json
		echo Done
		cd loader
		python main.py ../temp.json
		cd ..
	done
done
