FROM node:22.20.0-bookworm-slim AS dependencies

ENV PNPM_HOME=/pnpm
ENV PATH=$PNPM_HOME:$PATH
RUN corepack enable && corepack prepare pnpm@10.17.1 --activate

WORKDIR /workspace

COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml web/tsconfig.base.json ./
COPY web/api/package.json ./api/package.json
COPY web/submitqueue/package.json ./submitqueue/package.json
COPY web/service/submitqueue/package.json ./service/submitqueue/package.json
RUN pnpm install --frozen-lockfile

FROM dependencies AS build

COPY web/api ./api
COPY web/submitqueue ./submitqueue
COPY web/service/submitqueue ./service/submitqueue
RUN pnpm --filter @submitqueue/api build \
    && pnpm --filter @submitqueue/web-submitqueue build \
    && pnpm --filter @submitqueue/service-submitqueue-web build

FROM node:22.20.0-bookworm-slim AS runtime

ENV HOSTNAME=0.0.0.0
ENV NODE_ENV=production
ENV PORT=3000

WORKDIR /app

COPY --chown=node:node --from=build /workspace/service/submitqueue/.next/standalone ./
COPY --chown=node:node --from=build /workspace/service/submitqueue/.next/static ./service/submitqueue/.next/static

EXPOSE 3000

USER node
CMD ["node", "service/submitqueue/server.js"]
