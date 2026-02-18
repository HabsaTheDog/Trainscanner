import { Module } from '@nestjs/common';
import { RoutingController } from './routing.controller';
import { RoutingService } from './routing.service';
import { MotisModule } from '../motis/motis.module';
import { OjpModule } from '../ojp/ojp.module';
import { StationsModule } from '../stations/stations.module';
import { StitchingModule } from '../stitching/stitching.module';

@Module({
    imports: [MotisModule, OjpModule, StationsModule, StitchingModule],
    controllers: [RoutingController],
    providers: [RoutingService],
})
export class RoutingModule { }
