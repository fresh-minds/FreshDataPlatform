FROM node:18-alpine

WORKDIR /app

COPY package.json .
RUN npm install

COPY . .

# Expose port 3000
EXPOSE 3000

# Run in dev mode host=0.0.0.0 to allow external access
CMD ["npm", "run", "dev", "--", "--host"]
