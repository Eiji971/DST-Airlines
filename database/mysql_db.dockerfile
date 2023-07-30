# Use the official MySQL image from Docker Hub
FROM mysql:latest

# Expose the default MySQL port
EXPOSE 3306

# Start the MySQL server
CMD ["mysqld"]