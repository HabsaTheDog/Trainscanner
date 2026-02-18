import { Module } from '@nestjs/common';
import { MotisService } from './motis.service';
import { CacheModule } from '../cache/cache.module';

@Module({
    imports: [CacheModule],
    providers: [MotisService],
    exports: [MotisService],
})
export class MotisModule { }
