import { Module } from '@nestjs/common';
import { RateLimiterService } from './rate-limiter.service';
import { CacheModule } from '../cache/cache.module';

@Module({
    imports: [CacheModule],
    providers: [RateLimiterService],
    exports: [RateLimiterService],
})
export class RateLimiterModule { }
