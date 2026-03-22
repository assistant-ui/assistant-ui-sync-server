FROM node:22-alpine
WORKDIR /app

RUN npm install -g pnpm

# Copy workspace manifests (needed for pnpm workspace resolution)
COPY pnpm-workspace.yaml pnpm-lock.yaml package.json tsconfig.json ./

# Copy all package.json files so pnpm can resolve the workspace
COPY packages/sync-server/package.json ./packages/sync-server/
COPY packages/scaler/package.json ./packages/scaler/
COPY packages/test-server/package.json ./packages/test-server/
COPY packages/test-client/package.json ./packages/test-client/

# Install only this package's deps
RUN pnpm install --frozen-lockfile --filter @assistant-ui/sync-server

# Copy source
COPY packages/sync-server/src ./packages/sync-server/src
COPY packages/sync-server/tsconfig.json ./packages/sync-server/

EXPOSE 8787
CMD ["pnpm", "--filter", "@assistant-ui/sync-server", "dev"]
