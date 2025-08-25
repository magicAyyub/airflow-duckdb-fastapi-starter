# 1. Build stage
FROM node:20-alpine AS builder

WORKDIR /app

# Copy frontend package files
COPY package.json package-lock.json ./
COPY public ./public
RUN npm ci

# Copy all frontend files
COPY . .
RUN npm run build

# 2. Production stage
FROM node:20-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

# Copy built files from builder stage
COPY --from=builder /app/package.json ./
COPY --from=builder /app/node_modules ./node_modules/     
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/next.config.mjs ./    
COPY --from=builder /app/next-env.d.ts ./    
COPY --from=builder /app/postcss.config.mjs ./
COPY --from=builder /app/tsconfig.json ./
COPY --from=builder /app/components.json ./

EXPOSE 3000

CMD ["npx", "next", "start"]
