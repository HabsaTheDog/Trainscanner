import { NestFactory } from '@nestjs/core';
import { ValidationPipe } from '@nestjs/common';
import { AppModule } from './app.module';

async function bootstrap() {
    const app = await NestFactory.create(AppModule);

    // Enable CORS for frontend
    app.enableCors({
        origin: [
            'http://localhost:5173',
            'http://localhost:4173',
            'http://frontend:5173',
        ],
        methods: ['GET', 'POST'],
        credentials: true,
    });

    // Global validation pipe
    app.useGlobalPipes(
        new ValidationPipe({
            whitelist: true,
            forbidNonWhitelisted: true,
            transform: true,
        }),
    );

    const port = process.env.PORT || 3000;
    await app.listen(port);
    console.log(`🚂 Rail Meta-Router Orchestrator running on port ${port}`);
}

bootstrap();
