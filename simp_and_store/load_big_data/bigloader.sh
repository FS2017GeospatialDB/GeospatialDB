#!/bin/bash
file=${1}
for lat in {38..42..2}
do
	for lon in {-108..-102..2}
	do
		l=$(echo ${lon}-2.0 | bc)
		b=$(echo ${lat}-2.0 | bc)
		echo "Top: ${lat}"
		echo "Bot: ${b}"
		echo "Left: ${l}"
		echo "Right: ${lon}"

		bzcat ${file} | ~/Downloads/osmosis/bin/osmosis --read-xml enableDateParsing=no file=- --bounding-box top=${lat} left=${l} bottom=${b} right=${lon} --write-xml file=- | cat > temp.osm
		
		echo Converting to GeoJSON...
		node --max_old_space_size=8192 /usr/local/lib/node_modules/osmtogeojson/osmtogeojson temp.osm > temp.json
		echo Done
		cd loader
		python main.py ../temp.json
		cd ..
	done
done
