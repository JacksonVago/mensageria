//import { Roles } from '@/auth/decorators/roles.decorator';
//import { Role } from '@/auth/enums/roles.enum';
import { Body, Controller, Get, Param, Post, Put } from '@nestjs/common';
//import { ApiTags } from '@nestjs/swagger';
import { Queue } from 'bullmq';
import { jobMessageDto } from './dto/job-message.dto';
import { QueueService } from './queue.service';

interface param_cons {
  table: string;
  filters?: string[];
};

interface param_main {
  table: string;
  dados: any[];
};

interface param_pause {
  id_chanel: string;
  id_job: string;
  int_delay: number;
};

export const queuePool: Set<Queue> = new Set<Queue>();

//@ApiTags('Queues')
//@Roles(Role.PUBLIC)
@Controller('queue')
export class QueueController {
  constructor(
    //@InjectPronctualQueue() private mathBinaryQueue: Queue,
    private readonly queueService: QueueService,
  ) { }


  @Post('jobAdd')
  async jobAdd(@Body() dto: jobMessageDto): Promise<string | undefined> {
    return this.queueService.createQueue(dto);
  }

  @Get('Select')
  async getUsers(@Body() param: param_cons): Promise<any> {
    return this.queueService.getSelectTable(param.table, param.filters);
  }

  @Get('getJobs')
  async getJobs(@Body() param: param_cons): Promise<any> {
    /*let dataAtu = new Date();
    dataAtu.setDate(dataAtu.getDate() + 1);
    console.log(dataAtu);
    console.log((new Date()).toISOString());
 
    const day = dataAtu.getDate().toString().padStart(2, '0');
    const month = (dataAtu.getMonth() + 1).toString().padStart(2, '0');
    const year = dataAtu.getFullYear();
    console.log(`${year}-${month}-${day}`);
 
    return 1;*/
    return this.queueService.getJobsToProcess(0);
  }

  @Post('create')
  async create(@Body() param: param_main): Promise<string | undefined> {
    return this.queueService.maintenanceTable(param.table, param.dados);
  }

  @Put('pauseJob')
  async pauseJob(@Body() param: param_pause): Promise<string | undefined> {
    return this.queueService.delayedJob(param.id_chanel, param.id_job, param.int_delay);
  }
  @Post('createRecurrence')
  async createRecurrence(): Promise<string | undefined> {
    return this.queueService.CreateJobsToProcess(0);
  }

  @Post('createJobs/:empresaId')
  async createJobs(@Param('empresaId') empresaId: number): Promise<string | undefined> {
    return this.queueService.CreateJobsToProcess(empresaId);
  }
}
