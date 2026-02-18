import { Module } from '@nestjs/common';
import { OjpService } from './ojp.service';
import { CacheModule } from '../cache/cache.module';
import { RateLimiterModule } from '../rate-limiter/rate-limiter.module';

@Module({
    imports: [CacheModule, RateLimiterModule],
    providers: [OjpService],
    exports: [OjpService],
})
export class OjpModule { }
