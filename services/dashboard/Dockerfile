FROM node:24-alpine AS build
WORKDIR /build
COPY services/dashboard/package*.json ./
RUN npm ci --omit=dev
COPY services/dashboard/ ./
RUN npm run build
FROM nginx:1.28-alpine
COPY services/dashboard/nginx.conf /etc/nginx/nginx.conf
COPY --from=build /build/dist /usr/share/nginx/html
EXPOSE 8080
