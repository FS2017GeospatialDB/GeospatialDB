#!/bin/bash

read -p "Do you wish to apply the schema (THIS WILL ERASE THE DB)? [Y/n] " yn
case $yn in
    [Yy]* ) echo 'Applying schema'; cqlsh -e "SOURCE 'schema.cql'";;
    [Nn]* ) ;;
    * ) echo "Please answer yes or no.";;
esac

# TODO: now only load fixed name json, change it to user provided json
echo 'Loading to master...'
python load_to_master.py map.json

