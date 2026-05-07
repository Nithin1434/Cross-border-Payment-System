#!/bin/bash

# =================================================================
# 🚀 PayFlow AWS Deployment Script (Ubuntu)
# =================================================================

echo "🌐 Updating system and installing Docker..."
sudo apt-get update
sudo apt-get install -y docker.io docker-compose

sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -a -G docker $USER

echo "✅ Docker environment is ready!"
echo "----------------------------------------------------"
echo "👉 NEXT STEPS:"
echo "1. Log out and log back in (to apply docker group changes)."
echo "2. Run: sudo docker-compose up --build -d"
echo "3. Access your app at: http://<EC2-PUBLIC-IP>"
echo "----------------------------------------------------"
