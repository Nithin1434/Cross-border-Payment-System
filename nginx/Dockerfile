FROM nginx:alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf

# We will copy the frontend files into the container via docker-compose volume or COPY
# For a production image, COPY is better:
# COPY ../frontend /usr/share/nginx/html
