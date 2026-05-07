#!/bin/bash

# Configuration
export PATH=$PATH:/usr/local/bin:/opt/homebrew/bin
WORKSPACE="/Users/nithin/Desktop/cross-border-payments"

echo "🚀 Starting PayFlow Services..."

# 0. Cleanup existing services
echo "🧹 Cleaning up old processes..."
pkill -f "node index.js" > /dev/null 2>&1
pkill -f "mongod --port 2701" > /dev/null 2>&1
sleep 1

# 1. Start MongoDB Replica Nodes (27017, 27018, 27019)
echo "📦 Starting MongoDB replica nodes..."
mongod --port 27017 --dbpath "$WORKSPACE/db/node1" --replSet rs0 > "$WORKSPACE/db/node1/mongo.log" 2>&1 &
mongod --port 27018 --dbpath "$WORKSPACE/db/node2" --replSet rs0 > "$WORKSPACE/db/node2/mongo.log" 2>&1 &
mongod --port 27019 --dbpath "$WORKSPACE/db/node3" --replSet rs0 > "$WORKSPACE/db/node3/mongo.log" 2>&1 &

# 2. Wait for DB to be ready/elect primary
echo "⏳ Waiting for MongoDB to elect a primary (15s)..."
sleep 15

# 3. Start Microservices
echo "⚙️ Starting Microservices..."
cd "$WORKSPACE/currency-service" && node index.js > currency.log 2>&1 &
cd "$WORKSPACE/transaction-service" && node index.js > transaction.log 2>&1 &
cd "$WORKSPACE/worker-service" && node index.js > worker.log 2>&1 &

echo "✅ All services started in the background."
echo "🔗 Frontend: file://$WORKSPACE/frontend/index.html"
echo "----------------------------------------------------"
echo "Check .log files in each service directory for details."
