import { Module } from '@nestjs/common';
import { HealthController } from './health.controller';
import { CacheModule } from '../cache/cache.module';
import { MotisModule } from '../motis/motis.module';

@Module({
    imports: [CacheModule, MotisModule],
    controllers: [HealthController],
})
export class HealthModule { }
