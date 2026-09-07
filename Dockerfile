FROM node:22-alpine

WORKDIR /app

# Dependencies first so a source edit does not invalidate the install layer.
COPY package.json package-lock.json ./
RUN npm ci --omit=dev && npm i -g tsx@4

COPY tsconfig.json ./
COPY src ./src

ENV NODE_ENV=production
ENV PORT=8080
EXPOSE 8080

# Alpine's default shell is not a PID-1 init; without one, SIGTERM would not
# reach node and the metrics buffer would never flush on shutdown.
RUN apk add --no-cache tini
ENTRYPOINT ["/sbin/tini", "--"]
CMD ["tsx", "src/server/index.ts"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s \
  CMD wget -qO- http://127.0.0.1:8080/health || exit 1
