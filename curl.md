# cek leader
curl -sL "http://localhost:8001/info"
curl -sL "http://localhost:8002/info"
curl -sL "http://localhost:8003/info"

# request ke follower/leader
curl -i -X POST "http://localhost:8001/lock/acquire" \
     -H "X-Role: admin" \
     -H "Content-Type: application/json" \
     -d '{"resource_id": "file_1", "client_id": "user_A", "type": "exclusive"}'

# enqueue
curl -X POST "http://localhost:8004/queue/enqueue" \
     -H "X-Role: producer" \
     -H "Content-Type: application/json" \
     -d '{"topic": "sensor_A", "message": "TEMP_25"}'

# dequeue
curl -s "http://localhost:8006/queue/dequeue/sensor_A" -H "X-Role: consumer" | python -m json.tool

# ack
curl -X POST "http://localhost:8006/queue/ack" \
     -H "X-Role: consumer" \
     -H "Content-Type: application/json" \
     -d '{"ack_id": "[isi pakai ack di dequeue]"}'

# put
curl -X POST "http://localhost:8007/cache/config_01" \
     -H "X-Role: admin" -H "Content-Type: application/json" \
     -d '{"value": "VERSION_1.0"}'

# cek state
curl -s "http://localhost:8007/cache/state/config_01" | python -m json.tool

# get dari node lain
curl -s "http://localhost:8008/cache/config_01" -H "X-Role: reader" | python -m json.tool

# update data
curl -X POST "http://localhost:8008/cache/config_01" \
     -H "X-Role: admin" -H "Content-Type: application/json" \
     -d '{"value": "VERSION_2.0"}'

# audit log
docker exec lock-1 cat data/audit.log | tail -n 5

# locust
locust -f benchmarks/load_test_scenarios.py