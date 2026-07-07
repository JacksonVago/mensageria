import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { envSChema } from './env';
import { EnvService } from './env.service';

// EnvModule with the ConfigModule.forRoot
@Module({
  imports: [
    ConfigModule.forRoot({
      validate: (env) => envSChema.parse(env),
      isGlobal: true,
    }),
  ],
  providers: [EnvService],
  exports: [EnvService],
})
export class EnvModule {}
