import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { RoutingModule } from './routing/routing.module';
import { MotisModule } from './motis/motis.module';
import { OjpModule } from './ojp/ojp.module';
import { StationsModule } from './stations/stations.module';
import { StitchingModule } from './stitching/stitching.module';
import { CacheModule } from './cache/cache.module';
import { RateLimiterModule } from './rate-limiter/rate-limiter.module';
import { HealthModule } from './health/health.module';

@Module({
    imports: [
        ConfigModule.forRoot({
            isGlobal: true,
            envFilePath: ['.env', '../.env'],
        }),
        CacheModule,
        RateLimiterModule,
        StationsModule,
        MotisModule,
        OjpModule,
        StitchingModule,
        RoutingModule,
        HealthModule,
    ],
})
export class AppModule { }
