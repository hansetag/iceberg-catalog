#!/bin/sh
ROOT_DIR=/app

# Replace env vars in JavaScript files
echo "Replacing env constants in JS"
for file in $ROOT_DIR/assets/index.*.js   
do
  echo "Processing $file ...";

  sed -i 's|{BASE_URL:"/",MODE:"production",DEV:!1,PROD:!0}.VITE_APP_ICEBERG_CATALOG_URL|'\"${VITE_APP_ICEBERG_CATALOG_URL}\"'|g' $file 

done

echo "Starting Nginx"
nginx -g 'daemon off;'
