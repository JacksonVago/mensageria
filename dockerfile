# Use a Node.js base image
FROM node:alpine AS builder
#FROM node:20-alpine
#FROM node:22.19.0-slim

# Set the working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json (or yarn.lock) to the working directory
COPY package*.json ./

#Show erros
RUN set -eux;

# Install dependencies
RUN apk add --no-cache openssl
RUN npm install

COPY prisma ./prisma/
# Copy your Prisma schema

RUN npx prisma generate
# Generate Prisma Client

# Copy the rest of your application code
COPY . .

RUN apk add dumb-init

# Build the NestJS application
RUN npm run build

# Expose the port your NestJS application listens on
EXPOSE 99

# Command to run the application
CMD ["dumb-init","node", "dist/src/main.js"]