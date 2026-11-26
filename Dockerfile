# Build stage: install all deps and bundle the entrypoints with esbuild.
FROM node:22-bookworm-slim AS build

WORKDIR /app

COPY package*.json ./
# Skip install scripts: phantasma-sdk-ts pulls in Ledger USB transports
# (node-hid, usb) whose native builds the API never loads.
RUN npm ci --ignore-scripts

COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# Runtime stage: the bundles are self-contained, so no node_modules are needed.
FROM node:22-bookworm-slim

WORKDIR /app
ENV NODE_ENV=production

COPY --from=build --chown=node:node /app/dist ./dist

USER node
EXPOSE 3000

CMD ["node", "dist/startApiServer.js"]
