# Use the official Nginx image
FROM nginx:latest

# Remove default Nginx config
RUN rm /etc/nginx/conf.d/default.conf

# Copy your custom Nginx configuration
COPY nginx.conf /etc/nginx/conf.d/default.conf

# Copy your static website files
COPY . /usr/share/nginx/html 

# Expose port 80 (default Nginx port)
EXPOSE 80

# Default command to run Nginx (inherited from base image)
CMD ["nginx", "-g", "daemon off;"]