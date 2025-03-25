#!/bin/bash

curl -L -o ./tpch.zip https://github.com/BemiHQ/BemiDB/releases/download/v0.1.0/TPC-H_generated_data_s0.1.zip
unzip ./tpch.zip -d ./benchmark/data
rm ./tpch.zip

cd ./benchmark/data

mv ./TPC-H_generated_data/* ./
rm -rf ./TPC-H_generated_data
rm -rf ./__MACOSX

psql postgres://127.0.0.1:5432/tpch -f ./create-tables.ddl

for i in `ls *.tbl`; do
  table=${i/.tbl/}
  echo "Loading $table..."
  sed 's/|$//' $i > /tmp/$i
  psql postgres://127.0.0.1:5432/tpch -q -c "TRUNCATE $table"
  psql postgres://127.0.0.1:5432/tpch -c "\\copy $table FROM '/tmp/$i' CSV DELIMITER '|'"
done
