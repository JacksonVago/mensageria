import { EnvService } from '@/env/env.service';
import { BullBoardInstance, InjectBullBoard } from '@bull-board/nestjs';
import { Injectable, Logger } from "@nestjs/common";
import { Job, JobType, Queue, Worker } from "bullmq";
import { jobMessageDto } from "./dto/job-message.dto";
//import { BaseAdapter } from "@bull-board/api/dist/src/queueAdapters/base";
import { MailService } from '@/email/email.service';
import { PrismaService } from "@/prisma/prisma.service";
import { RedisService } from "@/redis/redis.service";
import { BaseAdapter } from "@bull-board/api/baseAdapter";
import { BullMQAdapter } from "@bull-board/api/bullMQAdapter";
import { FrequenciaEnvio, Prisma, TipoAgendamento, TipoIntervaloEnvio } from "@prisma/client";
import { jobs_status_enum } from "./enums/recurrence.enum";
//import { WhatsAppService } from "src/whatsapp/whatsapp.service";

/**
 * Será criado uma fila para cada empresa com uma Job que irá verificar todo dia 1 de cada mês a configuração dos alertar para a geração das filas 
 * por tipo de alerta. 
 * Essas filas (Renovação de contrato, aviso de pagamento, etc) irão verificar conforme o tipo de alerta, 
 * quais jobs devem ser criados naquele mês para aquele tipo de alerta conforme (contrato, renovação, pagamentos, etc)
 * Cada job será a solicitação do envio de um email, informando o vencimento de um contrato, ou aviso de pagamento, etc, 
 * e irá conter as informações do contato a ser enviado a mensagem, e o conteúdo da mensagem a ser enviada.
 * No caso de pagamento o código de barras do boleto, ou link de download, ou informações do contrato para renovação, etc.
 * Um texo padrão será criado para cada tipo de alerta, 
 * e as informações do contato e do conteúdo da mensagem serão preenchidas conforme o cadastro do cliente,
 * 
 */


export const queuePool: Set<Queue> = new Set<Queue>();
export const queues: { name: string, queue: Queue, worker: Worker }[] = [];

@Injectable()
export class QueueService {
    protected readonly logger = new Logger(QueueService.name);

    constructor(
        @InjectBullBoard() private readonly boardInstance: BullBoardInstance,
        //private readonly prismaService: PrismaService,
        private readonly configService: EnvService,
        private readonly redisService: RedisService,
        private readonly emailService: MailService,
        //private readonly whatsappService: WhatsAppService,
    ) {

        //Cria Fila da recorrência diária para gerar o Jobs das recorrência
        /*this.createQueue({
            id: "1",
            id_channel: "Create jobs",
            id_recurrence: "1",
            str_recurrence: "Create jobs",
            id_message: "0",
            id_contact: "0",
            int_sequence: 1,
            int_block: 1,
            str_message: "",
            str_start_date: "0",
            str_end_date: "0",
            str_start_time: "0",
            str_end_time: "0",
            str_cron: "0 20 20 * * *",
            int_delay: 0,
            dtm_created: "2001-01-01 20:00:00",
            dtm_updated: "2001-01-01 20:00:00",
            id_user: "1",
            enum_status: jobs_status_enum.WAITING_TO_START,
            enum_type: campaign_type_enum.PROGRAMMED
        });*/
        //Limpa filas
    }

    async createQueue(dto: jobMessageDto): Promise<string | undefined> {
        //console.log('inicio');
        let queueAux: Queue | undefined;
        let workerAux: Worker | undefined;
        let jobData = dto;
        let int_delay_schedule = 0;
        let int_limit = 0;
        let str_queue = "";
        const newDate = new Date();

        queuePool.clear();

        //Verifica se o JOB é um disparo ou recorrência
        /*if (dto.enum_type == campaign_type_enum.PUNCTUAL) {
            str_queue = dto.id_recurrence + ' - ' + dto.str_recurrence + ' (Disparo)';
        }
        else {
            str_queue = dto.id_recurrence + ' - ' + dto.str_recurrence + ' (Recorrência)';
        }*/

        //Verifica se o registro ja exite no banco de dados
        if (dto.id === "") {
            const prismaService: PrismaService = new PrismaService();
            const result = await prismaService.jobs.create({
                data: {
                    str_message: dto.str_message,
                    str_start_date: dto.str_start_date,
                    str_end_date: dto.str_end_date,
                    str_start_time: dto.str_start_time,
                    str_end_time: dto.str_end_time,
                    str_cron: dto.str_cron,
                    int_delay: dto.int_delay,
                    dtm_created: dto.dtm_created,
                    dtm_updated: dto.dtm_updated,
                    status: dto.status,

                    user: dto.userId ? { connect: { id: dto.userId } } : undefined,
                    alerta: dto.alertaId ? { connect: { id: dto.alertaId } } : undefined,
                    empresa: dto.empresaId ? { connect: { id: dto.empresaId } } : undefined,
                    pessoa: dto.pessoaId ? { connect: { id: dto.pessoaId } } : undefined,
                    imovel: dto.imovelId ? { connect: { id: dto.imovelId } } : undefined,
                    locacao: dto.locacaoId ? { connect: { id: dto.locacaoId } } : undefined,
                },
                include: {
                    empresa: true,
                    alerta: true,
                    pessoa: true,
                    imovel: true,
                    locacao: true,
                },
            });
            dto.id = result.id;
            prismaService.$disconnect();
        }

        str_queue = dto.empresaId + '-' + dto.alertaId + ' ' + dto.descAlerta;

        if (queues.length > 0) {
            if (queues.find(x => x.name == str_queue) != undefined) {
                queueAux = queues.find(x => x.name == str_queue)?.queue;
                workerAux = queues.find(x => x.name == str_queue)?.worker;
            }
        }

        if (queueAux == undefined) {

            //Criar fila
            queueAux = new Queue(str_queue, { connection: { host: this.configService.get('REDIS_HOST'), port: Number(this.configService.get('REDIS_PORT')) } });

            //Limpa fila
            queueAux.drain(true);
            //queueAux.remove()
            let repeatableJobs = await queueAux?.getRepeatableJobs()
            repeatableJobs.forEach(async job => {
                await queueAux?.removeRepeatableByKey(job.key);
            });

            //Cria processor quem executa os jobs
            workerAux = new Worker(str_queue,
                async (job: Job) => {

                    //Envia para o whatsApp                    
                    //this.whatsappService.sendMessage(data.id + '@s.whatsapp.net', data.message + ' channel id ' + data.channelId);

                    //Verificar se já passou do horário limite
                    /*if (dto.str_end_date != "0" && dto.str_end_date != "" && dto.str_end_time != "0" && dto.str_end_time != "") {
                        if ((new Date()).toISOString() > (new Date(dto.str_end_date + ' ' + dto.str_end_time)).toISOString()) {
                            //Pausa processo ew reagenda para o dia seguinte.
                            workerAux?.pause(true);
                            await this.PauseJobs(str_queue);
                        }
                    }*/
                },
                { connection: { host: this.configService.get('REDIS_HOST'), port: Number(this.configService.get('REDIS_PORT')) } }

            );

            //Evento quando ativa a JOB
            await workerAux.on('active', async (job: Job, returnValue: any) => {
                const { id, name, queueName, finishedOn, returnvalue } = job;
                const completionTime = finishedOn ? new Date(finishedOn).toISOString() : '';
                this.logger.log(
                    `Job id: ${id}, name: ${name} active in queue ${queueName} on ${completionTime}. Result: ${returnvalue}`,
                );

                const data: jobMessageDto = job.data;
                this.AtualizaStatusJob(data, jobs_status_enum.RUNNING).then(result => {
                    //Verificar qual tipo de job está sendo processada para executar a ação correspondente
                    switch (data.descAlerta) {
                        case "Geração de Alertas":
                            //console.log('Create JOBS from daily recurrence.');
                            //console.log('-------------------------------');
                            //const create_result = await this.CreateJobsToProcess(data.empresaId);
                            this.CreateJobsToProcess(data.empresaId).then(result => {
                                //console.log('-------------------------------');
                                //console.log('Fim Create JOBS from daily recurrence.');
                                //console.log('Chama jobs para processamento..');
                                //console.log('-------------------------------');
                                //console.log('-------------------------------');
                                //console.log('Fim jobs para processamento..');
                            });

                            break;

                        case "Geração de Alertas boletos vencidos":
                            //console.log('Create JOBS from daily recurrence.');
                            //console.log('-------------------------------');
                            this.CreateJobsBoletosVencidos(data.empresaId).then(result => {
                                //console.log('-------------------------------');
                                //console.log('Fim Create JOBS boletos vencidos.');
                                //console.log('Chama jobs para processamento..');
                                //console.log('-------------------------------');
                                //const processJobsVen = this.getJobsToProcess(data.empresaId);
                                //console.log('-------------------------------');
                                //console.log('Fim jobs para processamento..');
                            });
                            break;

                        case 'Execução de Alertas':
                            const processJobsVen = this.getJobsToProcess(data.empresaId);
                            break;

                        //Demais JOBs são os alertas à serem enviados por email.
                        default:
                            this.emailService.sendMail(data.empresaId, { email: data.str_email, subject: data.descAlerta, text: data.str_message });
                        //console.log('Processa envio de mensagem para o alerta ' + data.descAlerta);
                    }

                });

            });

            await workerAux.on('completed', async (job: Job, returnValue: any) => {
                const { id, name, queueName, finishedOn, returnvalue } = job;
                const completionTime = finishedOn ? new Date(finishedOn).toISOString() : '';

                let str_json: string = "";

                this.logger.log(
                    `Job id: ${id}, name: ${name} completed in queue ${queueName} on ${completionTime}. Result: ${returnvalue}`,
                );
                const newDate = new Date();

                //Atualiza data e hora de finalização
                jobData.str_end_time = newDate.getHours().toString() + ":" + newDate.getMinutes().toString() + ":" + newDate.getSeconds().toString() + ":" + newDate.getMilliseconds().toString();
                jobData.str_end_date = newDate.getDay().toString() + "/" + newDate.getMonth().toString() + "/" + newDate.getFullYear().toString();
                job.updateData(jobData);
                job.updateProgress(100);

                //Atualiza status do JOB no banco de dados                                
                if (jobData.str_cron !== null && jobData.str_cron.length > 0 && jobData.status !== jobs_status_enum.RUNNING) {
                    this.AtualizaStatusJob(jobData, jobs_status_enum.RUNNING);
                }
                else {
                    this.AtualizaStatusJob(jobData, jobs_status_enum.FINISHED);
                }
                /*
                if (jobData.descAlerta != "Geração de Alertas") {
                    this.AtualizaStatusJob(jobData, jobs_status_enum.FINISHED);
                }
                else {                    
                    this.AtualizaStatusJob(jobData, jobs_status_enum.RUNNING);
                }*/
            });

            workerAux.on('failed', async (job, error: Error) => {
                const data: jobMessageDto = job.data;
                this.AtualizaStatusJob(data, jobs_status_enum.ERROR, error.message);

                //console.log('Falhou ', job?.id)
                //console.error('worker fail', job, error, new Date());
            });

            //Verificar se precisar tratar o evento de finalização do job para atualizar o status no banco de dados, 
            // ou se é melhor atualizar o status no evento de completed, ou se é necessário tratar ambos para atualizar o status de forma mais precisa, 
            // considerando possíveis falhas ou erros durante a execução do job.

            queues.push({ name: str_queue, queue: queueAux, worker: workerAux })
        }

        queuePool.add(queueAux);

        const Aux_queues = [...queuePool].reduce((acc: BaseAdapter[], val) => {
            acc.push(new BullMQAdapter(val))
            return acc
        }, []);

        this.boardInstance.addQueue(Aux_queues[queuePool.size - 1]);

        //Aplica delay quando for agendamento
        if (dto.str_end_date != "0" && dto.str_end_date != "" && dto.str_end_time != "0" && dto.str_end_time != "") {
            console.log('Datas :', dto.str_start_date, dto.str_start_time);
            if ((new Date(dto.str_start_date + ' ' + dto.str_start_time)).toISOString() > (new Date()).toISOString()) {
                int_delay_schedule = Number(new Date(dto.str_start_date + ' ' + dto.str_start_time)) - Date.now();
            }
            console.log('Datas end :', dto.str_end_date, new Date(dto.str_end_time).getDate());
            console.log('Datas start :', dto.str_start_date, new Date(dto.str_start_time).getDate());
            int_limit = new Date(dto.str_end_date).getDate() - new Date(dto.str_start_date).getDate();
        }
        else {
            int_delay_schedule = 0;
        }

        let jobNew: Job;
        //console.log(dto);
        if (dto.str_cron.length > 0 && dto.str_cron != null) {
            jobNew = await queueAux.add(dto.id,
                dto,
                {
                    jobId: dto.id,
                    delay: dto.int_delay + int_delay_schedule,
                    repeat: {
                        pattern: dto.str_cron,
                        limit: int_limit > 0 ? int_limit : undefined,
                        tz: 'America/Sao_Paulo',
                    }
                });
        }
        else {
            jobNew = await queueAux.add(dto.id,
                dto,
                {
                    jobId: dto.id,
                    delay: dto.int_delay + int_delay_schedule,

                });

        }

        /*console.log('carrega queues');
        const arr_queue = await this.redisService.getAllQueues();
        console.log(arr_queue);*/

        return jobNew.id || '';
    }

    async CreateJobsBoletosVencidos(empresaId: number) {
        let now = new Date();
        let textoAlerta = "";
        let int_pos: number = 0;
        let int_tam: number = 0;
        let str_campo: string = "";

        //Identifica parametrização da empresa
        //console.log(empresaId);
        const prismaService: PrismaService = new PrismaService();

        const user = await prismaService.user.findFirst({
            where: {
                empresaId: empresaId,
            },
        });

        //Idendifica os alertas
        const element = await prismaService.configuracaoAlertas.findFirst({
            where: {
                empresaId: empresaId,
                descricao: "Aviso boleto atrasado",
                ativo: true,
            },
            include: {
                empresa: true,
                alerta: true,
            }
        });

        //console.log('alerata: ', element);
        console.log('Data :', new Date(now.getFullYear() + '-' + (now.getMonth() + 1) + '-' + now.getDate()));
        //Guarda o texto do alerta para processamento, substituindo os campos dinâmicos pelas informações correspondentes de cada contrato, locação, boleto, etc.
        textoAlerta = element.textoAlerta ? element.textoAlerta : "O boleto com vencimento em <Data de Vencimento> no valor de <Valor Original> referente a locação do imóvel <Imóvel> está vencido. <Email>";

        const boletosVenc = await prismaService.boleto.findMany({
            where: {
                empresaId: empresaId,
                status: 'CONFIRMADO',
                dataVencimento: {
                    lt: new Date(now.getFullYear() + '-' + (now.getMonth() + 1) + '-' + now.getDate())
                }
            },
            include: {
                locacao: {
                    include: {
                        imovel: true,
                        locatarios: {
                            include: {
                                pessoa: true,
                            }
                        },
                    }
                },
                imovel: {
                    include: {
                        proprietarios: {
                            include: {
                                pessoa: true,
                            }
                        },
                    }
                },
                documentos: true,
                lancamentoImovels: true,
                lanctoCondominio: true,
                lanctoLocacao: true,
            }
        });


        //console.log('Boletos vencidos: ', boletosVenc.length);
        //console.log('Texto : ', textoAlerta);
        boletosVenc.forEach(async boleto => {

            //Muda dados da mensagem de acordo com o boleto para enviar email para o locatário e proprietário do imóvel, caso exista email cadastrado. Se não tiver email cadastrado, não cria o job.
            if (textoAlerta.length > 0) {
                while (textoAlerta.indexOf('<', int_pos) > -1) {
                    int_pos = textoAlerta.indexOf('<', int_pos);
                    int_tam = textoAlerta.indexOf('>', int_pos);
                    str_campo = textoAlerta.substring(int_pos, int_tam + 1);


                    //Troca campo por dados do boleto
                    //console.log(str_campo);
                    switch (str_campo) {
                        case "<mes>":
                            if (boleto.dataVencimento && boleto.dataVencimento.toISOString().length > 0) {
                                textoAlerta = textoAlerta.replace(str_campo, boleto.dataVencimento.toLocaleString('pt-BR', { month: 'long' }));
                            }
                            else {
                                textoAlerta = textoAlerta.replace(str_campo, "");
                            }
                            break;

                        case "<Data de Emissão>":
                            if (boleto.dataEmissao && boleto.dataEmissao.toISOString().length > 0) {
                                textoAlerta = textoAlerta.replace(str_campo, boleto.dataEmissao.toISOString().split('T')[0]);
                            }
                            else {
                                textoAlerta = textoAlerta.replace(str_campo, "");
                            }
                            break;

                        case "<Data de Vencimento>":
                            if (boleto.dataVencimento && boleto.dataVencimento.toISOString().length > 0) {
                                textoAlerta = textoAlerta.replace(str_campo, boleto.dataVencimento.toISOString().split('T')[0]);
                            }
                            else {
                                textoAlerta = textoAlerta.replace(str_campo, "");
                            }
                            break;

                        case "<Valor Original>":
                            if (boleto.dataVencimento && boleto.valorOriginal.toString().length > 0) {
                                textoAlerta = textoAlerta.replace(str_campo, boleto.valorOriginal.toLocaleString('pt-BR'));
                            }
                            else {
                                textoAlerta = textoAlerta.replace(str_campo, "");
                            }
                            break;

                        case "<Email>":
                            if (boleto.locacao) {
                                if (boleto.locacao.locatarios && boleto.locacao.locatarios.length > 0) {
                                    textoAlerta = textoAlerta.replace(str_campo, boleto.locacao.locatarios.map(loc => loc.pessoa.email).join(";"));
                                }
                                else {
                                    textoAlerta = textoAlerta.replace(str_campo, "");
                                }
                            }
                            else {
                                if (boleto.imovel && boleto.imovel.proprietarios && boleto.imovel.proprietarios.length > 0) {
                                    textoAlerta = textoAlerta.replace(str_campo, boleto.locacao.locatarios.map(loc => loc.pessoa.email).join(";"));
                                }
                                else {
                                    textoAlerta = textoAlerta.replace(str_campo, "");
                                }
                            }
                            break;

                        case "<Link do Documento>":
                            if (boleto.documentos && boleto.documentos.length > 0) {
                                textoAlerta = textoAlerta.replace(str_campo, boleto.documentos.map(doc => doc.url ? this.configService.get('AZURE_BLOB_CONTAINER').toString() + doc.url : "").join("\n"));
                            }
                            else {
                                textoAlerta = textoAlerta.replace(str_campo, "");
                            }
                            break;

                        case "<Linha Digitável Boleto>":
                            if (boleto.linhaDigitavel && boleto.linhaDigitavel.length > 0) {
                                textoAlerta = textoAlerta.replace(str_campo, boleto.linhaDigitavel);
                            }
                            else {
                                textoAlerta = textoAlerta.replace(str_campo, "");
                            }
                            break;

                        case "<Linha Digitável Lançamento>":
                            if (boleto.lancamentoImovels && boleto.lancamentoImovels.length > 0) {
                                textoAlerta = textoAlerta.replace(str_campo, boleto.lancamentoImovels.map(lan => lan.linhaDigitavel ? lan.linhaDigitavel : "").join("\n"));
                            }
                            else {
                                if (boleto.lanctoCondominio && boleto.lanctoCondominio.length > 0) {
                                    textoAlerta = textoAlerta.replace(str_campo, boleto.lanctoCondominio.map(lan => lan.linhaDigitavel ? lan.linhaDigitavel : "").join("\n"));
                                }
                                else {
                                    if (boleto.lanctoLocacao && boleto.lanctoLocacao.length > 0) {
                                        textoAlerta = textoAlerta.replace(str_campo, boleto.lanctoLocacao.map(lan => lan.linhaDigitavel ? lan.linhaDigitavel : "").join("\n"));
                                    }
                                    else {
                                        textoAlerta = textoAlerta.replace(str_campo, "");
                                    }
                                }
                            }
                            break;
                    }

                    int_pos++;
                }
            }

            const jobData: jobMessageDto = {
                id: '',
                empresaId: empresaId,
                alertaId: element.id,
                descAlerta: element.alerta.descricao,
                pessoaId: (boleto.imovelId !== null && boleto.imovelId > 0 ?
                    ((boleto.imovel && boleto.imovel.proprietarios && boleto.imovel.proprietarios.length > 0) ? boleto.imovel.proprietarios[0]?.pessoaId : undefined) :
                    boleto.locacaoId !== null && boleto.locacaoId > 0 ?
                    ((boleto.locacao && boleto.locacao.locatarios && boleto.locacao.locatarios.length > 0) ? boleto.locacao.locatarios[0]?.pessoaId : undefined) :
                    undefined),
                imovelId: (boleto.imovelId !== null && boleto.imovelId > 0 ? boleto.imovelId : null),
                locacaoId: (boleto.locacaoId !== null && boleto.locacaoId > 0 ? boleto.locacaoId : null),
                str_email: (boleto.imovelId !== null && boleto.imovelId > 0 ?
                    ((boleto.imovel && boleto.imovel.proprietarios && boleto.imovel.proprietarios.length > 0) ? boleto.imovel.proprietarios[0]?.pessoa.email : undefined) :
                    boleto.locacaoId !== null && boleto.locacaoId > 0 ?
                    ((boleto.locacao && boleto.locacao.locatarios && boleto.locacao.locatarios.length > 0) ? boleto.locacao.locatarios[0]?.pessoa.email : undefined) :
                    undefined),
                str_error: null,
                str_message: textoAlerta,
                str_start_date: new Date().toISOString().split('T')[0],
                str_end_date: new Date().toISOString().split('T')[0],
                str_start_time: new Date().toTimeString().split(' ')[0],
                str_end_time: new Date().toTimeString().split(' ')[0],
                str_cron: '',
                int_delay: 0,
                dtm_created: new Date(),
                dtm_updated: new Date(),
                status: jobs_status_enum.WAITING_TO_START,
                userId: user.id,
            }
            await prismaService.jobs.create({
                data: {
                    str_message: jobData.str_message,
                    str_start_date: jobData.str_start_date,
                    str_end_date: jobData.str_end_date,
                    str_start_time: jobData.str_start_time,
                    str_end_time: jobData.str_end_time,
                    str_cron: jobData.str_cron,
                    int_delay: jobData.int_delay,
                    dtm_created: jobData.dtm_created,
                    dtm_updated: jobData.dtm_updated,
                    status: jobData.status,
                    alerta: jobData.alertaId ? { connect: { id: jobData.alertaId } } : undefined,
                    empresa: jobData.empresaId ? { connect: { id: jobData.empresaId } } : undefined,
                    pessoa: jobData.pessoaId ? { connect: { id: jobData.pessoaId } } : undefined,
                    imovel: jobData.imovelId ? { connect: { id: jobData.imovelId } } : undefined,
                    locacao: jobData.locacaoId ? { connect: { id: jobData.locacaoId } } : undefined,
                    user: jobData.userId ? { connect: { id: jobData.userId } } : undefined,
                },
                include: {
                    empresa: true,
                    alerta: true,
                    pessoa: true,
                    imovel: true,
                    locacao: true,
                }
            });
        });
        prismaService.$disconnect();
    }

    async delayedJob(str_queue: string, str_jobId: string, int_delay: number): Promise<any> {
        var msg: string = "";
        if (queues.length > 0) {
            if (int_delay = 0) {
                int_delay = 40000;
            }
            if (queues.find(x => x.name == str_queue) != undefined) {
                const queueAux = queues.find(x => x.name == str_queue)?.queue;
                const workerAux = queues.find(x => x.name == str_queue)?.worker;
                //let job:Job | undefined;
                const job = queueAux?.getJob(str_jobId);
                (await job)?.changeDelay(int_delay);
                msg = `Paused for ${int_delay / 1000} seconds.`;
            }
            else {
                msg = `Queue ${str_queue} not exists.`;
            }
        }
        else {
            msg = `Job ${str_jobId} not exists.`;
        }

        return msg;

    }

    async getJobsToProcess(empresaId: number): Promise<any> {

        let int_cont = 0;
        const prismaService: PrismaService = new PrismaService();
        const jobs_data = await prismaService.jobs.findMany({
            where: {
                empresaId: empresaId,
                status: jobs_status_enum.WAITING_TO_START
            },
            include: {
                alerta: {
                    include: {
                        alerta: true,
                    }
                },
                pessoa: true,
                imovel: true,
                locacao: true,
            }
        });

        //const jobs: jobMessageDto[] = JSON.parse((await jobs_data).toString());

        if (jobs_data != null) {
            //console.log('Carregando jobs para processamento...');
            (await jobs_data).forEach(element => {
                int_cont = 1;
                //console.log('job: ', element);
                //console.log('Tipo: ', element.alerta.descricao.includes("Geração"));
                //Verificar se é job de envio de e-mail ou apenas geração de JOB´s
                if (element.alerta.descricao.includes("Geração")) {
                    let jobData: jobMessageDto = {
                        id: element.id,
                        empresaId: element.empresaId,
                        alertaId: element.alertaId,
                        descAlerta: element.alerta.alerta.descricao,
                        pessoaId: element.pessoaId,
                        imovelId: element.imovelId,
                        locacaoId: element.locacaoId,
                        str_email: element.pessoa ? element.pessoa.email : null,
                        str_message: element.str_message,
                        str_error: null,
                        str_start_date: element.str_start_date,
                        str_end_date: element.str_end_date,
                        str_start_time: element.str_start_time,
                        str_end_time: element.str_end_time,
                        str_cron: element.str_cron,
                        int_delay: element.int_delay,
                        dtm_created: element.dtm_created,
                        dtm_updated: element.dtm_updated,
                        status: element.status,
                        userId: element.userId,
                    }
                    this.AtualizaStatusJob(jobData, jobs_status_enum.WAITING_TO_PROCESS).then(result => {
                        this.createQueue(jobData);
                    });
                }
                else {
                    if (element.pessoa.email !== null) {
                        let jobData: jobMessageDto = {
                            id: element.id,
                            empresaId: element.empresaId,
                            alertaId: element.alertaId,
                            descAlerta: element.alerta.alerta.descricao,
                            pessoaId: element.pessoaId,
                            imovelId: element.imovelId,
                            locacaoId: element.locacaoId,
                            str_email: element.pessoa.email,
                            str_message: element.str_message,
                            str_error: null,
                            str_start_date: element.str_start_date,
                            str_end_date: element.str_end_date,
                            str_start_time: element.str_start_time,
                            str_end_time: element.str_end_time,
                            str_cron: element.str_cron,
                            int_delay: element.int_delay,
                            dtm_created: element.dtm_created,
                            dtm_updated: element.dtm_updated,
                            status: element.status,
                            userId: element.userId,
                        }
                        this.AtualizaStatusJob(jobData, jobs_status_enum.WAITING_TO_PROCESS).then(result => {
                            this.createQueue(jobData);
                        });
                    }
                    else {
                        console.log('Não foi possível processar o job ' + element.id + ' pois o contato não possui email cadastrado.');
                    }
                }
            });
        }
        if (int_cont == 0) {
            console.log('Não encontrou jobs para serem disparadas.')
        }
        prismaService.$disconnect();
        return jobs_data;
    }

    async CreateJobsToProcess(empresaId: number): Promise<any> {

        //console.log('Iniciando criação de jobs para processamento.');
        const prismaService: PrismaService = new PrismaService();

        //Identifica parametrização da empresa
        const empresaConfig = await prismaService.empresa.findUnique({
            where: {
                id: empresaId,
            },
        });

        const user = await prismaService.user.findFirst({
            where: {
                empresaId: empresaId,
            },
        });

        //Idendifica os alertas        
        const result = await prismaService.configuracaoAlertas.findMany({
            where: {
                empresaId: empresaId,
                ativo: true,
            },
            include: {
                empresa: true,
                alerta: true,
            }
        });

        //Criar jobs para cada alerta encontrado
        result.forEach(async element => {

            let textoAlerta = "";
            let int_pos: number = 0;
            let int_tam: number = 0;
            let str_campo: string = "";
            let str_cron = "";
            let str_hora = '*';
            let str_minuto = '*';
            let str_dia = '*';
            let str_mes = '*';
            let str_diaSemana = '*';

            //Guarda o texto do alerta para processamento, substituindo os campos dinâmicos pelas informações correspondentes de cada contrato, locação, boleto, etc.
            textoAlerta = element.textoAlerta;

            //Monta o cron de acordo com a confifuração do alerta
            if (element.tipoAgendamento === TipoAgendamento.UNICO) {
                str_hora = element.horarioEnvio ? element.horarioEnvio.split(':')[0] : '00';
                str_minuto = element.horarioEnvio ? element.horarioEnvio.split(':')[1] : '00';
                str_dia = element.dataInicio ? element.dataInicio.getDate().toString() : '*';
                str_mes = element.dataInicio ? (element.dataInicio.getMonth() + 1).toString() : '*';
                str_cron = str_minuto + ' ' + str_hora + ' ' + str_dia + ' ' + str_mes + ' ' + str_diaSemana;
            }
            else {
                switch (element.frequenciaEnvio) {
                    case FrequenciaEnvio.DIARIO:
                        switch (element.tipoIntervaloEnvio) {
                            case TipoIntervaloEnvio.HORAS:
                                if (element.ocorreAcada === 24) {
                                    str_hora = element.horarioEnvio ? element.horarioEnvio.split(':')[0] : '00';
                                    str_minuto = element.horarioEnvio ? element.horarioEnvio.split(':')[1] : '01';
                                    str_cron = str_minuto + ' ' + str_hora + ' * * *';
                                }
                                else {
                                    str_hora = (element.ocorreAcada ? element.ocorreAcada.toString() : '01');
                                    str_minuto = '01';
                                    str_dia = (element.grupoEnvio && element.grupoEnvio.length > 0 ? element.grupoEnvio : '*').toString();
                                    str_cron = str_minuto + ' */' + str_hora + ' ' + str_dia + ' * *';
                                }
                                break;

                            case TipoIntervaloEnvio.MINUTOS:
                                str_hora = '*';
                                str_minuto = (element.ocorreAcada ? element.ocorreAcada.toString() : '01');
                                str_dia = (element.grupoEnvio && element.grupoEnvio.length > 0 ? element.grupoEnvio : '*').toString();
                                str_cron = '*/' + str_minuto + ' ' + str_hora + ' ' + str_dia + ' * *';
                                break;
                        }
                        break;

                    case FrequenciaEnvio.SEMANAL:
                        str_cron = '0 0 * * 0';
                        break;
                    case FrequenciaEnvio.MENSAL:
                        str_cron = '0 0 1 * *';
                        break;
                }
            }
            console.log('Processa alerta: ', element.alerta.descricao);
            switch (element.alerta.descricao) {
                case "Geração de Alertas":
                    //console.log('Geração de Alertas.');
                    //Veririca se já existe essa JOB criada, para não criar duplicada
                    const existingJobExec = await prismaService.jobs.findFirst({
                        where: {
                            alertaId: element.id,
                            empresaId: empresaId,
                            status: jobs_status_enum.WAITING_TO_START,
                        }
                    });

                    if (existingJobExec) {
                        console.log('Job para Execução de Alertas já existe.');
                        return;
                    }

                    let jobDataExec: jobMessageDto = {
                        id: '',
                        empresaId: empresaId,
                        alertaId: element.id,
                        descAlerta: 'Execução de Alertas',
                        pessoaId: null,
                        imovelId: null,
                        locacaoId: null,
                        str_email: null,
                        str_message: 'Job responsável por executar os alertas aguardando para serem processados.',
                        str_error: null,
                        str_start_date: new Date().toISOString().split('T')[0],
                        str_end_date: new Date().toISOString().split('T')[0],
                        str_start_time: new Date().toTimeString().split(' ')[0],
                        str_end_time: new Date().toTimeString().split(' ')[0],
                        str_cron: '*/5 * * * *',
                        int_delay: 0,
                        dtm_created: new Date(),
                        dtm_updated: new Date(),
                        status: jobs_status_enum.WAITING_TO_PROCESS,
                        userId: user.id,
                    }
                    try {
                        const job = await prismaService.jobs.create({
                            data: {
                                str_message: jobDataExec.str_message,
                                str_start_date: jobDataExec.str_start_date,
                                str_end_date: jobDataExec.str_end_date,
                                str_start_time: jobDataExec.str_start_time,
                                str_end_time: jobDataExec.str_end_time,
                                str_cron: jobDataExec.str_cron,
                                int_delay: jobDataExec.int_delay,
                                dtm_created: jobDataExec.dtm_created,
                                dtm_updated: jobDataExec.dtm_updated,
                                status: jobDataExec.status,
                                alerta: jobDataExec.alertaId ? { connect: { id: jobDataExec.alertaId } } : undefined,
                                empresa: jobDataExec.empresaId ? { connect: { id: jobDataExec.empresaId } } : undefined,
                                pessoa: jobDataExec.pessoaId ? { connect: { id: jobDataExec.pessoaId } } : undefined,
                                imovel: jobDataExec.imovelId ? { connect: { id: jobDataExec.imovelId } } : undefined,
                                locacao: jobDataExec.locacaoId ? { connect: { id: jobDataExec.locacaoId } } : undefined,
                                user: jobDataExec.userId ? { connect: { id: jobDataExec.userId } } : undefined,
                            }

                        });
                        if (job && job.id.toString() != '') {
                            jobDataExec.id = job.id;
                            this.createQueue(jobDataExec);
                            //console.log('Job para Execução de Alertas criado');
                        }
                        else {
                            console.log('Job para Execução de Alertas não foi criado');
                        }
                    }
                    catch (error) {
                        console.log('Erro ao criar job para Execução de Alertas: ', error);
                    }

                    //console.log('Fim Geração de Alertas');
                    break;

                case "Geração de Alertas boletos vencidos":
                    //console.log('Geração de Alertas boletos vencidos');
                    //Veririca se já existe essa JOB criada, para não criar duplicada
                    const existingJob = await prismaService.jobs.findFirst({
                        where: {
                            alertaId: element.id,
                            empresaId: empresaId,
                        }
                    });

                    if (existingJob) {
                        console.log('Job para Geração de Alertas boletos vencidos já existe.');
                        return;
                    }

                    const jobData: jobMessageDto = {
                        id: '',
                        empresaId: empresaId,
                        alertaId: element.id,
                        descAlerta: element.alerta.descricao,
                        pessoaId: null,
                        imovelId: null,
                        locacaoId: null,
                        str_email: null,
                        str_message: element.textoAlerta,
                        str_error: null,
                        str_start_date: new Date().toISOString().split('T')[0],
                        str_end_date: new Date().toISOString().split('T')[0],
                        str_start_time: new Date().toTimeString().split(' ')[0],
                        str_end_time: new Date().toTimeString().split(' ')[0],
                        str_cron: str_cron,
                        int_delay: 0,
                        dtm_created: new Date(),
                        dtm_updated: new Date(),
                        status: jobs_status_enum.WAITING_TO_START,
                        userId: user.id,
                    }
                    try {
                        const job = await prismaService.jobs.create({
                            data: {
                                str_message: jobData.str_message,
                                str_start_date: jobData.str_start_date,
                                str_end_date: jobData.str_end_date,
                                str_start_time: jobData.str_start_time,
                                str_end_time: jobData.str_end_time,
                                str_cron: jobData.str_cron,
                                int_delay: jobData.int_delay,
                                dtm_created: jobData.dtm_created,
                                dtm_updated: jobData.dtm_updated,
                                status: jobData.status,
                                alerta: jobData.alertaId ? { connect: { id: jobData.alertaId } } : undefined,
                                empresa: jobData.empresaId ? { connect: { id: jobData.empresaId } } : undefined,
                                pessoa: jobData.pessoaId ? { connect: { id: jobData.pessoaId } } : undefined,
                                imovel: jobData.imovelId ? { connect: { id: jobData.imovelId } } : undefined,
                                locacao: jobData.locacaoId ? { connect: { id: jobData.locacaoId } } : undefined,
                                user: jobData.userId ? { connect: { id: jobData.userId } } : undefined,
                            }

                        });
                        if (job && job.id.toString() != '') {
                            console.log('Job para Geração de Alertas boletos vencidos não foi criado');
                        }
                        else {
                            console.log('Job para Geração de Alertas boletos vencidos criado com sucesso');
                        }
                    }
                    catch (error) {
                        console.log('Erro ao criar job para Geração de Alertas boletos vencidos: ', error);
                    }
                    //console.log('Fim Geração de Alertas boletos vencidos');

                    break;

                case "Aviso reajuste Locação":
                    const locacao = await prismaService.locacao.findMany({
                        where: {
                            empresaId: empresaId,
                            status: 'ATIVA',
                        },
                    });

                    if (locacao && locacao.length > 0) {
                        console.log('Criar JOB para aviso reajuste Locação');
                    }
                    else {

                    }
                    break;

                case "Aviso renovação contrato":
                    const datInicio = new Date();
                    datInicio.setDate(datInicio.getDate() + empresaConfig?.avisosRenovacaoContrato);

                    const renovacaoContrato = await prismaService.locacao.findMany({
                        where: {
                            empresaId: empresaId,
                            status: 'ATIVA',
                            dataFim: {
                                equals: datInicio,
                            }
                        },
                        include: {
                            imovel: {
                                include: {
                                    proprietarios: {
                                        include: {
                                            pessoa: true,
                                        }
                                    },
                                }
                            },
                            locatarios: {
                                include: {
                                    pessoa: true,
                                }
                            },
                        }
                    });

                    renovacaoContrato.forEach(async locacao => {
                        if ((
                            (locacao.imovel.proprietarios &&
                                locacao.imovel.proprietarios.length > 0 &&
                                locacao.imovel.proprietarios[0].pessoa.email !== '') &&
                            (locacao.locatarios &&
                                locacao.locatarios.length > 0 &&
                                locacao.locatarios[0].pessoa.email !== '')
                        )
                        ) {
                            const jobData: jobMessageDto = {
                                id: '',
                                empresaId: empresaId,
                                alertaId: element.id,
                                descAlerta: element.alerta.descricao,
                                pessoaId: locacao.locatarios[0]?.pessoaId,
                                imovelId: locacao.imovelId,
                                locacaoId: locacao.id,
                                str_email: locacao.imovel.proprietarios.map(loc => loc.pessoa.email).join(";") + ";" + locacao.locatarios.map(loc => loc.pessoa.email).join(";"),
                                str_error: null,
                                str_message: element.textoAlerta,
                                str_start_date: new Date().toISOString().split('T')[0],
                                str_end_date: new Date().toISOString().split('T')[0],
                                str_start_time: new Date().toTimeString().split(' ')[0],
                                str_end_time: new Date().toTimeString().split(' ')[0],
                                str_cron: str_cron,
                                int_delay: 0,
                                dtm_created: new Date(),
                                dtm_updated: new Date(),
                                status: jobs_status_enum.WAITING_TO_START,
                                userId: user.id,
                            }
                            /*this.prismaService.jobs.create({
                                data: {
                                    str_message: jobData.str_message,
                                    str_start_date: jobData.str_start_date,
                                    str_end_date: jobData.str_end_date,
                                    str_start_time: jobData.str_start_time,
                                    str_end_time: jobData.str_end_time,
                                    str_cron: jobData.str_cron,
                                    int_delay: jobData.int_delay,
                                    dtm_created: jobData.dtm_created,
                                    dtm_updated: jobData.dtm_updated,
                                    status: jobData.status,
                                    alerta: jobData.alertaId ? { connect: { id: jobData.alertaId } } : undefined,
                                    empresa: jobData.empresaId ? { connect: { id: jobData.empresaId } } : undefined,
                                    pessoa: jobData.pessoaId ? { connect: { id: jobData.pessoaId } } : undefined,
                                    imovel: jobData.imovelId ? { connect: { id: jobData.imovelId } } : undefined,
                                    locacao: jobData.locacaoId ? { connect: { id: jobData.locacaoId } } : undefined,
                                    user: jobData.userId ? { connect: { id: jobData.userId } } : undefined,
                                }
    
                            });*/
                        }
                    });
                    console.log('Criar JOB para aviso renovação contrato');
                    break;

                case "Aviso seguro fiança":
                    const seguroFianca = await prismaService.locacao.findMany({
                        where: {
                            empresaId: empresaId,
                            status: 'ATIVA',
                        },
                        include: {
                            imovel: true,
                            locatarios: {
                                include: {
                                    pessoa: true,
                                }
                            },
                        }
                    });

                    seguroFianca.forEach(locacao => {

                    });

                    console.log('Criar JOB para aviso seguro fiança');
                    break;

                case "Aviso seguro incêndio":
                    console.log('Criar JOB para aviso seguro incêndio');
                    break;

                case "Aviso Título capitalização":
                    console.log('Criar JOB para aviso Título capitalização');
                    break;

                case "Aviso depósito calção":
                    console.log('Criar JOB para aviso depósito calção');
                    break;

                case "Aviso vencimento boleto":
                    let dataInicial = new Date();
                    let dataFinal = new Date();
                    dataFinal.setDate(dataFinal.getDate() + empresaConfig.avisosVencBoleto);
                    console.log('Data Inicial: ', dataInicial);
                    console.log('Data Final: ', dataFinal);
                    const boletos = await prismaService.boleto.findMany({
                        where: {
                            empresaId: empresaId,
                            status: 'PENDENTE',
                            dataVencimento: {
                                gt: dataInicial,
                                lte: dataFinal,
                            }
                        },
                        include: {
                            locacao: {
                                include: {
                                    imovel: true,
                                    locatarios: {
                                        include: {
                                            pessoa: true,
                                        }
                                    },
                                }
                            },
                            imovel: {
                                include: {
                                    proprietarios: {
                                        include: {
                                            pessoa: true,
                                        }
                                    }
                                }
                            },
                            documentos: true,
                            lanctoCondominio: true,
                            lanctoLocacao: true,
                            lancamentoImovels: true,
                        }
                    });

                    console.log('Boletos a vencer :', boletos);
                    boletos.forEach(async boleto => {

                        //Muda dados da mensagem de acordo com o boleto para enviar email para o locatário e proprietário do imóvel, caso exista email cadastrado. Se não tiver email cadastrado, não cria o job.
                        if (textoAlerta.length > 0) {
                            while (textoAlerta.indexOf('<', int_pos) > -1) {
                                int_pos = textoAlerta.indexOf('<', int_pos);
                                int_tam = textoAlerta.indexOf('>', int_pos);
                                str_campo = textoAlerta.substring(int_pos, int_tam + 1);


                                //Troca campo por dados do boleto
                                //console.log(str_campo);
                                switch (element.alerta.descricao) {
                                    case "Aviso reajuste Locação":
                                        break;

                                    case "Aviso renovação contrato":
                                        break;

                                    case "Aviso seguro incêndio":
                                        break;

                                    case "Aviso vencimento boleto":
                                        switch (str_campo) {
                                            case "<mes>":
                                                if (boleto.dataVencimento && boleto.dataVencimento.toISOString().length > 0) {
                                                    textoAlerta = textoAlerta.replace(str_campo, boleto.dataVencimento.toLocaleString('pt-BR', { month: 'long' }));
                                                }
                                                else {
                                                    textoAlerta = textoAlerta.replace(str_campo, "");
                                                }
                                                break;

                                            case "<Data de Emissão>":
                                                if (boleto.dataEmissao && boleto.dataEmissao.toISOString().length > 0) {
                                                    textoAlerta = textoAlerta.replace(str_campo, boleto.dataEmissao.toISOString().split('T')[0]);
                                                }
                                                else {
                                                    textoAlerta = textoAlerta.replace(str_campo, "");
                                                }
                                                break;

                                            case "<Data de Vencimento>":
                                                if (boleto.dataVencimento && boleto.dataVencimento.toISOString().length > 0) {
                                                    textoAlerta = textoAlerta.replace(str_campo, boleto.dataVencimento.toISOString().split('T')[0]);
                                                }
                                                else {
                                                    textoAlerta = textoAlerta.replace(str_campo, "");
                                                }
                                                break;

                                            case "<Valor Original>":
                                                if (boleto.dataVencimento && boleto.valorOriginal.toString().length > 0) {
                                                    textoAlerta = textoAlerta.replace(str_campo, boleto.valorOriginal.toLocaleString('pt-BR'));
                                                }
                                                else {
                                                    textoAlerta = textoAlerta.replace(str_campo, "");
                                                }
                                                break;

                                            case "<Email>":
                                                if (boleto.locacao) {
                                                    if (boleto.locacao.locatarios && boleto.locacao.locatarios.length > 0) {
                                                        textoAlerta = textoAlerta.replace(str_campo, boleto.locacao.locatarios.map(loc => loc.pessoa.email).join(";"));
                                                    }
                                                    else {
                                                        textoAlerta = textoAlerta.replace(str_campo, "");
                                                    }
                                                }
                                                else {
                                                    if (boleto.imovel && boleto.imovel.proprietarios && boleto.imovel.proprietarios.length > 0) {
                                                        textoAlerta = textoAlerta.replace(str_campo, boleto.locacao.locatarios.map(loc => loc.pessoa.email).join(";"));
                                                    }
                                                    else {
                                                        textoAlerta = textoAlerta.replace(str_campo, "");
                                                    }
                                                }
                                                break;

                                            case "<Link do Documento>":
                                                if (boleto.documentos && boleto.documentos.length > 0) {
                                                    textoAlerta = textoAlerta.replace(str_campo, boleto.documentos.map(doc => doc.url ? this.configService.get('AZURE_BLOB_CONTAINER').toString() + doc.url : "").join("\n"));
                                                }
                                                else {
                                                    textoAlerta = textoAlerta.replace(str_campo, "");
                                                }
                                                break;

                                            case "<Linha Digitável Boleto>":
                                                if (boleto.linhaDigitavel && boleto.linhaDigitavel.length > 0) {
                                                    textoAlerta = textoAlerta.replace(str_campo, boleto.linhaDigitavel);
                                                }
                                                else {
                                                    textoAlerta = textoAlerta.replace(str_campo, "");
                                                }
                                                break;

                                            case "<Linha Digitável Lançamento>":
                                                if (boleto.lancamentoImovels && boleto.lancamentoImovels.length > 0) {
                                                    textoAlerta = textoAlerta.replace(str_campo, boleto.lancamentoImovels.map(lan => lan.linhaDigitavel ? lan.linhaDigitavel : "").join("\n"));
                                                }
                                                else {
                                                    if (boleto.lanctoCondominio && boleto.lanctoCondominio.length > 0) {
                                                        textoAlerta = textoAlerta.replace(str_campo, boleto.lanctoCondominio.map(lan => lan.linhaDigitavel ? lan.linhaDigitavel : "").join("\n"));
                                                    }
                                                    else {
                                                        if (boleto.lanctoLocacao && boleto.lanctoLocacao.length > 0) {
                                                            textoAlerta = textoAlerta.replace(str_campo, boleto.lanctoLocacao.map(lan => lan.linhaDigitavel ? lan.linhaDigitavel : "").join("\n"));
                                                        }
                                                        else {
                                                            textoAlerta = textoAlerta.replace(str_campo, "");
                                                        }
                                                    }
                                                }
                                                break;
                                        }
                                        break;

                                    case "Aviso boleto atrasado":
                                        /*arr_campos = [
                                          { check: false, campo: "dataEmissao", descricao: "Data de Emissão" },
                                          { check: false, campo: "dataVencimento", descricao: "Data de Vencimento" },
                                          { check: false, campo: "valorOriginal", descricao: "Valor Original" },
                                          { check: false, campo: "email", descricao: "Email" },
                                          { check: false, campo: "linkDocumento", descricao: "Link do Documento" },
                                          { check: false, campo: "linhaDigitavelBol", descricao: "Linha Digitável Boleto" },
                                          { check: false, campo: "linhaDigitavelLan", descricao: "Linha Digitável Lançamento" },
                                        ]*/
                                        break;

                                    default:
                                        break;
                                }

                                int_pos++;
                            }
                        }

                        let str_data: string = new Date(boleto.dataVencimento.getFullYear() + '-' +
                            (boleto.dataVencimento.getMonth() + 1) + '-' +
                            (boleto.dataVencimento.getDate() - empresaConfig.avisosVencBoleto)).toLocaleDateString('pt-BR', { timeZone: 'UTC' });
                        const jobData: jobMessageDto = {
                            id: '',
                            empresaId: empresaId,
                            alertaId: element.id,
                            descAlerta: element.alerta.descricao,
                            pessoaId: (boleto.locacao && boleto.locacao.locatarios) ? boleto.locacao.locatarios[0]?.pessoaId : (boleto.imovel && boleto.imovel.proprietarios) ? boleto.imovel.proprietarios[0]?.pessoaId : null,
                            imovelId: boleto.imovelId ? boleto.imovelId : null,
                            locacaoId: boleto.locacaoId ? boleto.locacaoId : null,
                            str_email: (boleto.locacao && boleto.locacao.locatarios) ? boleto.locacao.locatarios.map(loc => loc.pessoa.email).join(";") : (boleto.imovel && boleto.imovel.proprietarios) ? boleto.imovel.proprietarios.map(prop => prop.pessoa.email).join(";") : null,
                            str_error: null,
                            str_message: textoAlerta,
                            str_start_date: new Date().toISOString().split('T')[0],
                            str_end_date: boleto.dataVencimento.toISOString().split('T')[0],
                            str_start_time: new Date().toISOString().split('T')[1],
                            str_end_time: new Date().toISOString().split('T')[1],
                            str_cron: str_cron,
                            int_delay: 0,
                            dtm_created: new Date(),
                            dtm_updated: new Date(),
                            status: jobs_status_enum.WAITING_TO_START,
                            userId: user.id,
                        }
                        const job = await prismaService.jobs.create({
                            data: {
                                str_error: jobData.str_error,
                                str_message: jobData.str_message,
                                str_start_date: jobData.str_start_date,
                                str_end_date: jobData.str_end_date,
                                str_start_time: jobData.str_start_time,
                                str_end_time: jobData.str_end_time,
                                str_cron: jobData.str_cron,
                                int_delay: jobData.int_delay,
                                dtm_created: jobData.dtm_created,
                                dtm_updated: jobData.dtm_updated,
                                status: jobData.status,
                                alerta: jobData.alertaId ? { connect: { id: jobData.alertaId } } : undefined,
                                empresa: jobData.empresaId ? { connect: { id: jobData.empresaId } } : undefined,
                                pessoa: jobData.pessoaId ? { connect: { id: jobData.pessoaId } } : undefined,
                                imovel: jobData.imovelId ? { connect: { id: jobData.imovelId } } : undefined,
                                locacao: jobData.locacaoId ? { connect: { id: jobData.locacaoId } } : undefined,
                                user: jobData.userId ? { connect: { id: jobData.userId } } : undefined,
                            },
                            include: {
                                empresa: true,
                                alerta: true,
                                pessoa: true,
                                imovel: true,
                                locacao: true,
                            }
                        });
                        if (job && job.id.toString() != '') {
                            console.log('Job para aviso vencimento boleto criado com socesso');
                        }
                    });
                    //console.log('Fim Aviso vencimento boleto');
                    break;

                case "Aviso boleto atrasado":
                    /*const boletosVenc = await this.prismaService.boleto.findMany({
                        where: {
                            empresaId: empresaId,
                            status: 'PENDENTE',
                            dataVencimento: {
                                gt: new Date(now.getFullYear() + '-' + (now.getMonth() + 1) + '-' + now.getDate())
                            }
                        },
                        include: {
                            locacao: {
                                include: {
                                    imovel: true,
                                    locatarios: {
                                        include: {
                                            pessoa: true,
                                        }
                                    },
                                }
                            },
                            imovel: {
                                include: {
                                    proprietarios: {
                                        include: {
                                            pessoa: true,
                                        }
                                    },
                                }
                            },
                            documentos: true,
                            lancamentoImovels: true,
                            lanctoCondominio: true,
                            lanctoLocacao: true,
                        }
                    });

                    boletosVenc.forEach(boleto => {

                        //Muda dados da mensagem de acordo com o boleto para enviar email para o locatário e proprietário do imóvel, caso exista email cadastrado. Se não tiver email cadastrado, não cria o job.
                        if (textoAlerta.length > 0) {
                            while (textoAlerta.indexOf('<', int_pos) > -1) {
                                int_pos = textoAlerta.indexOf('<', int_pos);
                                int_tam = textoAlerta.indexOf('>', int_pos);
                                str_campo = textoAlerta.substring(int_pos, int_tam + 1);


                                //Troca campo por dados do boleto
                                console.log(str_campo);
                                switch (str_campo) {
                                    case "<Data de Emissão>":
                                        textoAlerta = textoAlerta.replace(str_campo, boleto.dataEmissao.toISOString().split('T')[0]);
                                        break;

                                    case "<Data de Vencimento>":
                                        textoAlerta = textoAlerta.replace(str_campo, boleto.dataEmissao.toISOString().split('T')[0]);
                                        break;

                                    case "<Valor Original>":
                                        textoAlerta = textoAlerta.replace(str_campo, boleto.valorOriginal.toLocaleString('pt-BR'));
                                        break;

                                    case "<Email>":
                                        textoAlerta = textoAlerta.replace(str_campo, boleto.locacao.locatarios.map(loc => loc.pessoa.email).join(";"));
                                        break;

                                    case "<Link do Documento>":
                                        if (boleto.documentos && boleto.documentos.length > 0) {
                                            textoAlerta = textoAlerta.replace(str_campo, boleto.documentos.map(doc => doc.url ? this.configService.get('AZURE_BLOB_CONTAINER').toString() + doc.url : "").join("\n"));
                                        }
                                        break;

                                    case "<Linha Digitável Boleto>":
                                        textoAlerta = textoAlerta.replace(str_campo, boleto.linhaDigitavel);
                                        break;

                                    case "<Linha Digitável Lançamento>":
                                        if (boleto.lancamentoImovels && boleto.lancamentoImovels.length > 0) {
                                            textoAlerta = textoAlerta.replace(str_campo, boleto.lancamentoImovels.map(lan => lan.linhaDigitavel ? lan.linhaDigitavel : "").join("\n"));
                                        }
                                        else {
                                            if (boleto.lanctoCondominio && boleto.lanctoCondominio.length > 0) {
                                                textoAlerta = textoAlerta.replace(str_campo, boleto.lanctoCondominio.map(lan => lan.linhaDigitavel ? lan.linhaDigitavel : "").join("\n"));
                                            }
                                            else {
                                                if (boleto.lanctoLocacao && boleto.lanctoLocacao.length > 0) {
                                                    textoAlerta = textoAlerta.replace(str_campo, boleto.lanctoLocacao.map(lan => lan.linhaDigitavel ? lan.linhaDigitavel : "").join("\n"));
                                                }
                                                else {
                                                    textoAlerta = textoAlerta.replace(str_campo, "");
                                                }
                                            }
                                        }
                                        break;
                                }

                                int_pos++;
                            }
                        }

                        const jobData: jobMessageDto = {
                            id: '',
                            empresaId: empresaId,
                            alertaId: element.alertaId,
                            descAlerta: element.alerta.descricao,
                            pessoaId: (boleto.imovelId !== null && boleto.imovelId > 0 ? boleto.imovel.proprietarios[0]?.pessoaId : boleto.locacao.locatarios[0]?.pessoaId),
                            imovelId: boleto.locacao.imovelId,
                            locacaoId: boleto.locacaoId,
                            str_email: (boleto.imovelId !== null && boleto.imovelId > 0 ? boleto.imovel.proprietarios[0]?.pessoa.email : boleto.locacao.locatarios[0]?.pessoa.email),
                            str_error: null,
                            str_message: textoAlerta,
                            str_start_date: new Date().toISOString().split('T')[0],
                            str_end_date: new Date().toISOString().split('T')[0],
                            str_start_time: null,
                            str_end_time: null,
                            str_cron: '',
                            int_delay: 0,
                            dtm_created: new Date(),
                            dtm_updated: new Date(),
                            status: jobs_status_enum.WAITING_TO_START,
                            userId: '',
                        }
                        this.prismaService.jobs.create({
                            data: {
                                str_message: jobData.str_message,
                                str_start_date: jobData.str_start_date,
                                str_end_date: jobData.str_end_date,
                                str_start_time: jobData.str_start_time,
                                str_end_time: jobData.str_end_time,
                                str_cron: jobData.str_cron,
                                int_delay: jobData.int_delay,
                                dtm_created: jobData.dtm_created,
                                dtm_updated: jobData.dtm_updated,
                                status: jobData.status,
                                alerta: jobData.alertaId ? { connect: { id: jobData.alertaId } } : undefined,
                                empresa: jobData.empresaId ? { connect: { id: jobData.empresaId } } : undefined,
                                pessoa: jobData.pessoaId ? { connect: { id: jobData.pessoaId } } : undefined,
                                imovel: jobData.imovelId ? { connect: { id: jobData.imovelId } } : undefined,
                                locacao: jobData.locacaoId ? { connect: { id: jobData.locacaoId } } : undefined,
                                user: jobData.userId ? { connect: { id: jobData.userId } } : undefined,
                            },
                            include: {
                                empresa: true,
                                alerta: true,
                                pessoa: true,
                                imovel: true,
                                locacao: true,
                            }
                        });
                    });*/
                    console.log('Fim Aviso boleto atrasado - vazio');
                    break;

                default:
                    console.log('Default Criar JOB para o alerta ' + element.alerta.descricao);
            }
        });
        prismaService.$disconnect();

        return result;
    }

    async getSelectTable(table: string, filters?: string[]): Promise<any> {
        //Variável que irá montar os filtros
        let str_filters: string = '';

        //Query que ira efetuar a consulta no banco
        let str_query: string = 'select * from f_sel_tbl_';

        //Identificar os filtros informado 
        filters?.forEach(element => {
            str_filters += "'" + element + "',";
        });

        //Junta query e filtros
        str_query += table + '(' + str_filters.substring(0, str_filters.length - 1) + ')'

        const prismaService: PrismaService = new PrismaService();

        //Transforma string em query e execurta no banco de dados
        const result = await prismaService.$queryRaw(Prisma.sql([str_query]));
        prismaService.$disconnect();
        return result;

    }

    async maintenanceTable(table: string, dados: any[]): Promise<any> {
        //Envia JSON contendo os registros e a tabela para serem processados no banco de dados
        ///--Não esquecer
        //const result = await this.prismaService.maintenanceTable(JSON.stringify(dados), table);
        //return result;
        return '';
    }

    //Pausa os jobs de uma queue
    private async PauseJobs(queueName: string) {
        let int_delay_schedule = 0;

        const queueJob = queues.find(x => x.name == queueName)?.queue;
        const workerJob = queues.find(x => x.name == queueName)?.worker;
        const jobs = queueJob?.getJobs(["paused"]);
        let dataAtu = new Date();
        dataAtu.setDate(dataAtu.getDate() + 1);

        if (jobs != null && (await jobs).length > 0) {
            (await jobs).forEach(element => {
                int_delay_schedule = Number(new Date(this.formatDate(dataAtu) + ' ' + element.data.str_start_time)) - Date.now();
                element.changeDelay(int_delay_schedule);
            });

            //Reiniciar processor
            workerJob?.resume();
        }
    }

    private formatDate(date: Date): string {
        const day = date.getDate().toString().padStart(2, '0');
        const month = (date.getMonth() + 1).toString().padStart(2, '0');
        const year = date.getFullYear();
        return `${year}-${month}-${day}`;
    }

    private async AtualizaStatusJob(job: jobMessageDto, statusJobs: jobs_status_enum, errorMessage: string = null) {
        //console.log('Inicio Atualiza status do JOB no banco de dados : ', new Date().toISOString());
        const prismaService: PrismaService = new PrismaService();
        const result = await prismaService.jobs.update({
            where: {
                id: job.id,
            },
            data: {
                str_error: errorMessage,
                status: statusJobs,
            },
        });
        //console.log('Fim Atualiza status do JOB no banco de dados : ', new Date().toISOString());
        prismaService.$disconnect();
        return result;

    }

    //Limpa Jobs de uma queue
    private async cleanQueue(queueName: string, jobtype?: JobType[], jobdate?: Date) {
        const queueJob = queues.find(x => x.name == queueName)?.queue;
        const workerJob = queues.find(x => x.name == queueName)?.worker;
        const jobs = queueJob?.getJobs(jobtype)

        if (jobs != null && (await jobs).length > 0) {
            (await jobs).forEach(element => {
                if (jobdate != undefined) {
                    //Caso tenha informado a data valida se o jobs esta com data de término menor que data informada como parametro
                    if (new Date(element.data.str_end_date).toISOString() < jobdate.toISOString()) {
                        queueJob?.remove(element.data.id);
                    }
                }
                else {
                    //Senão informou data remove todo os Jobs encontrados
                    queueJob?.remove(element.data.id);
                }
            });
        }
    }
}


