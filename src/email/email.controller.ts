import { Body, Controller, Param, Post } from '@nestjs/common';
import { MailService } from './email.service';

@Controller('emails')
export class MailController {
    constructor(
        private readonly mailService: MailService,
    ) { }

    @Post('send-email/:empresaId')
    async sendMail(
        @Param('empresaId') empresaId: number,
        @Body() sendMailDto: { email: string; subject: string; text?: string },
    ): Promise<string> {
        await this.mailService.sendMail(empresaId, sendMailDto);

        return 'Email sent successfully';
    }
}