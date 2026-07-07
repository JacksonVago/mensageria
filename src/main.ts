import { ValidationPipe } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';
import { EnvService } from './env/env.service';

async function bootstrap() {
  const app = await NestFactory.create(AppModule);

  const configService = app.get(EnvService);
  const port = configService.get('PORT');

  app.enableCors();
  //enable validation pipe globally
  app.useGlobalPipes(
    new ValidationPipe({
      transform: true,
    }),
  );

  await app.listen(port);

}

bootstrap();
