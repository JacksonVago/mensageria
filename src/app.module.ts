import { Module } from '@nestjs/common';
import { queueConfig } from './queue/config/queue.config';
import { bullboardConfig } from './queue/config/bull-board.config';
import { PrismaModule } from './prisma/prisma.module';
import { QueueModule } from './queue/queue.module';
import { MailModule } from './email/email.module';
import { EnvModule } from './env/env.module';

@Module({
  imports: [
    queueConfig,
    bullboardConfig,
    EnvModule,
    PrismaModule,
    QueueModule,
    MailModule,
  ],
  controllers: [],
  providers: [],
})
export class AppModule {}
