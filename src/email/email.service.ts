import { PrismaService } from '@/prisma/prisma.service';
import { Injectable } from '@nestjs/common';
import * as nodemailer from 'nodemailer';

interface SendMailConfiguration {
    email: string;
    subject: string;
    text?: string;
    //template: any;
}

@Injectable()
export class MailService {
    private transporter: nodemailer.Transporter;

    constructor(private readonly prismaService: PrismaService
        //private envService: EnvService
    ) {

    }

    async sendMail(empresaId: number, { email, subject, text }: SendMailConfiguration) {

        //Buscar dados de acessos 
        const empresa = await this.prismaService.empresa.findUnique({
            where: {
                id: empresaId,
            },
        });

        const PORT: number | undefined = empresa.portSmtp == void 0 ? 0 : empresa.portSmtp;
        const HOST: string | undefined = empresa.smtpHost == void 0 ? undefined : empresa.smtpHost;
        const SECURE: boolean | undefined = empresa.secureSmtp == void 0 ? undefined : empresa.secureSmtp;
        const USER: string | undefined = empresa.userSmtp == void 0 ? undefined : empresa.userSmtp;
        const PWD: string | undefined = empresa.pwdSmtp == void 0 ? undefined : empresa.pwdSmtp;

        this.transporter = nodemailer.createTransport(
            {
                host: HOST,
                port: PORT,
                secure: SECURE,
                auth: {
                    user: USER,
                    pass: PWD,
                },
                tls: {
                    rejectUnauthorized: false
                },
            },
        );

        /*const PORT: number | undefined = parseInt(this.envService.get('SMTP_PORT').toString() == void 0 ? "0" : this.envService.get('SMTP_PORT').toString());

        console.log(this.envService.get('SMTP_HOST').toString())
        console.log(this.envService.get('SMTP_USER').toString());
        console.log(this.envService.get('SMTP_PASSWORD').toString());
        this.transporter = nodemailer.createTransport(
            {
                host: this.envService.get('SMTP_HOST').toString(),
                port: PORT,
                secure: true,
                auth: {
                    user: this.envService.get('SMTP_USER').toString(),
                    pass: this.envService.get('SMTP_PASSWORD').toString(),
                    //user: 'jackson@natividadesolucoes.com.br',
                    //pass: 'JjmlS2021@',
                },
                tls: {
                    rejectUnauthorized: false
                },
            },
            {
                from: {
                    name: 'NestJs + React Emails Test App',
                    address: 'Test App',
                },
            },
        );*/

        this.transporter.sendMail(
            {

                from: USER,
                to: email,
                subject: subject,
                text: text || "I hope this message gets delivered!",
            },
            (err, info) => {
                if (err) {
                    console.error('erro', err);
                    return;
                }
            }
        );
    }
}