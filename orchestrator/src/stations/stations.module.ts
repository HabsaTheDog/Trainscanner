import { Module } from '@nestjs/common';
import { StationsService } from './stations.service';

@Module({
    providers: [StationsService],
    exports: [StationsService],
})
export class StationsModule { }
