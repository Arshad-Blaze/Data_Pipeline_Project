docker run --name de-postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=orders_db \
  -e TZ=Asia/Kolkata \
  -p 8080:5432 \
  -d postgres:18
