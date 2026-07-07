// src/redis/redis.module.ts
import { Module } from '@nestjs/common';
import { RedisModule } from '@nestjs-modules/ioredis';
//import { ConfigModule, ConfigService } from '@nestjs/config';
import { RedisService } from '@/redis/redis.service';
import { RedisController } from './redis.controller';

@Module({
  imports: [
    /*RedisModule.forRootAsync({
      imports: [ConfigModule],      
      useFactory: (configService: ConfigService) => ({
        config: {
          host: configService.get<string>('REDIS_HOST'),
          port: configService.get<number>('REDIS_PORT'),
        },
      }),
      inject: [ConfigService],
    },),*/
    RedisModule.forRoot({
        type: 'single',
        //url: 'redis://localhost:6379',
        url: 'redis://localhost:6379',
      }),
  ],
  controllers:[RedisController],
  providers: [RedisService],
  exports: [RedisService],
})
export class CustomRedisModule {}
