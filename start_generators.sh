#!/bin/bash
# Start 3 TRACE feed generators on ports 5555, 5556, 5557
# Used inside the Docker container
# --host 0.0.0.0 allows connections from other containers

python3 /app/fake_trace_generator.py --tcp --host 0.0.0.0 --port 5555 &
python3 /app/fake_trace_generator.py --tcp --host 0.0.0.0 --port 5556 &
python3 /app/fake_trace_generator.py --tcp --host 0.0.0.0 --port 5557 &

echo "Feed generators started on ports 5555, 5556, 5557"
echo "Waiting for connections..."

# Wait for all background processes
wait
