FROM node:24-alpine AS build
WORKDIR /build
COPY services/search-ui/package*.json ./
RUN npm ci --ignore-scripts
COPY services/search-ui/ ./
RUN npm run build
FROM nginx:1.28-alpine
COPY services/search-ui/nginx.conf /etc/nginx/nginx.conf
COPY --from=build /build/dist /usr/share/nginx/html
EXPOSE 8080
