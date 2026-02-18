import { Injectable, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

interface BucketState {
    tokens: number;
    lastRefill: number;
    maxTokens: number;
    refillRate: number; // tokens per second
}

@Injectable()
export class RateLimiterService {
    private readonly logger = new Logger(RateLimiterService.name);
    private buckets: Map<string, BucketState> = new Map();

    constructor(private readonly configService: ConfigService) { }

    /** Check and consume a rate limit token for an endpoint */
    async tryAcquire(endpoint: string): Promise<boolean> {
        const bucket = this.getOrCreateBucket(endpoint);
        this.refillBucket(bucket);

        if (bucket.tokens >= 1) {
            bucket.tokens -= 1;
            return true;
        }

        this.logger.warn(`Rate limit exceeded for endpoint: ${endpoint}`);
        return false;
    }

    /** Get remaining tokens for an endpoint */
    getRemaining(endpoint: string): number {
        const bucket = this.getOrCreateBucket(endpoint);
        this.refillBucket(bucket);
        return Math.floor(bucket.tokens);
    }

    private getOrCreateBucket(endpoint: string): BucketState {
        if (!this.buckets.has(endpoint)) {
            // Read rate limits from config (requests per minute)
            const configKey = `RATE_LIMIT_${endpoint.toUpperCase()}`;
            const reqPerMin = this.configService.get<number>(configKey, 60);

            this.buckets.set(endpoint, {
                tokens: reqPerMin,
                lastRefill: Date.now(),
                maxTokens: reqPerMin,
                refillRate: reqPerMin / 60, // Convert to per-second
            });
        }
        return this.buckets.get(endpoint)!;
    }

    private refillBucket(bucket: BucketState): void {
        const now = Date.now();
        const elapsed = (now - bucket.lastRefill) / 1000; // seconds
        bucket.tokens = Math.min(
            bucket.maxTokens,
            bucket.tokens + elapsed * bucket.refillRate,
        );
        bucket.lastRefill = now;
    }
}
