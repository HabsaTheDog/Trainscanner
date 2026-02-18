import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import Redis from 'ioredis';

@Injectable()
export class CacheService implements OnModuleInit, OnModuleDestroy {
    private readonly logger = new Logger(CacheService.name);
    private client: Redis | null = null;
    private fallbackCache: Map<string, { value: string; expires: number }> = new Map();

    constructor(private readonly configService: ConfigService) { }

    async onModuleInit() {
        const redisUrl = this.configService.get<string>('REDIS_URL', 'redis://localhost:6379');

        try {
            this.client = new Redis(redisUrl, {
                maxRetriesPerRequest: 3,
                retryStrategy: (times) => {
                    if (times > 3) {
                        this.logger.warn('Redis connection failed after 3 retries. Using fallback cache.');
                        return null; // Stop retrying
                    }
                    return Math.min(times * 200, 2000);
                },
            });

            this.client.on('connect', () => {
                this.logger.log('Connected to Redis');
            });

            this.client.on('error', (err) => {
                this.logger.warn(`Redis error: ${err.message}. Using in-memory fallback.`);
            });
        } catch {
            this.logger.warn('Failed to initialize Redis. Using in-memory fallback cache.');
        }
    }

    async onModuleDestroy() {
        if (this.client) {
            await this.client.quit();
        }
    }

    /** Get a cached value */
    async get<T>(key: string): Promise<T | null> {
        try {
            if (this.client?.status === 'ready') {
                const value = await this.client.get(key);
                return value ? JSON.parse(value) : null;
            }
        } catch {
            // Fall through to in-memory cache
        }

        // Fallback: in-memory cache
        const entry = this.fallbackCache.get(key);
        if (entry && entry.expires > Date.now()) {
            return JSON.parse(entry.value);
        }
        this.fallbackCache.delete(key);
        return null;
    }

    /** Set a cached value with TTL in seconds */
    async set(key: string, value: unknown, ttlSeconds: number): Promise<void> {
        const serialized = JSON.stringify(value);

        try {
            if (this.client?.status === 'ready') {
                await this.client.setex(key, ttlSeconds, serialized);
                return;
            }
        } catch {
            // Fall through to in-memory cache
        }

        // Fallback: in-memory cache
        this.fallbackCache.set(key, {
            value: serialized,
            expires: Date.now() + ttlSeconds * 1000,
        });
    }

    /** Delete a cached value */
    async del(key: string): Promise<void> {
        try {
            if (this.client?.status === 'ready') {
                await this.client.del(key);
            }
        } catch {
            // Ignore
        }
        this.fallbackCache.delete(key);
    }

    /** Check if Redis is connected */
    isConnected(): boolean {
        return this.client?.status === 'ready';
    }
}
