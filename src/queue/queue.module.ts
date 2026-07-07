import { Module } from '@nestjs/common';
import { MailModule } from '@/email/email.module';
import { EnvModule } from '@/env/env.module';
import { PrismaModule } from '@/prisma/prisma.module';
import { CustomRedisModule } from '@/redis/redis.module';
import { BullModule } from '@nestjs/bullmq';
import { QueueController } from './queue.controller';
import { QueueService } from './queue.service';

@Module({
  imports: [
    MailModule,
    PrismaModule,
    EnvModule,
    CustomRedisModule,
    /*BullModule.registerQueue({
      name: 'dynamicQueue',
    }),*/
  ],
  providers: [QueueService,],
  exports: [QueueService,],
  controllers: [QueueController],
})
export class QueueModule { }
