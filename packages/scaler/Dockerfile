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
RUN pnpm install --frozen-lockfile --filter @assistant-ui/scaler

# Copy source
COPY packages/scaler/src ./packages/scaler/src
COPY packages/scaler/tsconfig.json ./packages/scaler/

EXPOSE 8788
CMD ["pnpm", "--filter", "@assistant-ui/scaler", "dev"]
