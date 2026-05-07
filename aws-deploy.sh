#!/bin/bash

# =================================================================
# 🚀 PayFlow AWS Deployment Script
# This script automates Docker installation and platform startup.
# =================================================================

echo "🌐 Updating system and installing dependencies..."
sudo yum update -y
sudo amazon-linux-extras install docker -y
sudo service docker start
sudo usermod -a -G docker ec2-user

# Install Docker Compose
echo "📦 Installing Docker Compose..."
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

echo "✅ Docker environment is ready!"
echo "----------------------------------------------------"
echo "👉 NEXT STEPS:"
echo "1. Log out and log back in (to apply docker group changes)."
echo "2. Run: docker-compose up --build -d"
echo "3. Access your app at: http://<EC2-PUBLIC-IP>"
echo "----------------------------------------------------"
