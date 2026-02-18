import { Module } from '@nestjs/common';
import { StitchingService } from './stitching.service';
import { StationsModule } from '../stations/stations.module';

@Module({
    imports: [StationsModule],
    providers: [StitchingService],
    exports: [StitchingService],
})
export class StitchingModule { }
