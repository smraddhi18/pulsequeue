# syntax=docker/dockerfile:1.7

# ---- build ----
FROM node:20-bookworm-slim AS build
WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci

COPY tsconfig.json tsconfig.build.json ./
COPY src ./src
RUN npm run build \
  && npm prune --omit=dev

# ---- runtime ----
FROM node:20-bookworm-slim AS runtime
WORKDIR /app

ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=3000

RUN useradd --system --uid 10001 --create-home pulsequeue

COPY --from=build --chown=pulsequeue:pulsequeue /app/package.json /app/package-lock.json* ./
COPY --from=build --chown=pulsequeue:pulsequeue /app/node_modules ./node_modules
COPY --from=build --chown=pulsequeue:pulsequeue /app/dist ./dist
# SQL migrations ship with the image; `node dist/db/migrate-cli.js` applies them.
COPY --chown=pulsequeue:pulsequeue migrations ./migrations

USER pulsequeue
EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:'+(process.env.PORT||3000)+'/health').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"

CMD ["node", "dist/server.js"]
