// src/redis/redis.service.ts
import { Injectable } from '@nestjs/common';
import { InjectRedis } from '@nestjs-modules/ioredis';
import Redis from 'ioredis';

@Injectable()
export class RedisService {
  constructor(@InjectRedis() private readonly redis: Redis) {}

  async getAllQueues(): Promise<string[]> {
    const queues = new Set<string>();
    let cursor = '0';

    do {
      const [newCursor, keys] = await this.redis.scan(cursor, 'MATCH', 'bull:*:id', 'COUNT', 100);
      cursor = newCursor;

      keys.forEach(key => {
        const queueName = key.split(':')[1];
        queues.add(queueName);
      });

    } while (cursor !== '0');

    return Array.from(queues);
  }

  async delQueues(queue: string){
    console.log(queue);
    const stream = this.redis.scanStream({ match:`bull:${queue}:*`});

    stream.on("data", (keys) => {
        if (keys.length) {
         const pipeline = this.redis.pipeline();
          keys.forEach((key) => {
           pipeline.del(key);
          });
          pipeline.exec();
        }
       });
       
       stream.on("end", () => {
        console.log(`Queue removing.`);
       });

       stream.on("error", (e) => {
        console.log(`Queue ${e}.`);
       });


  }
}
