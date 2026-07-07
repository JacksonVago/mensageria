import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Env } from './env';

@Injectable()
export class EnvService {
  constructor(private configService: ConfigService<Env, true>) {}

  get<T extends keyof Env>(key: T): Env[T] {
    const value = this.configService.get<Env[T]>(key);

    if (value === undefined) {
      throw new Error(`${key} is not defined in the environment variables`);
    }

    return value;
  }
}
