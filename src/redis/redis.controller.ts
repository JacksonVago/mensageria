//import { Roles } from '@/auth/decorators/roles.decorator';
//import { Role } from '@/auth/enums/roles.enum';
import { Controller, Delete, Get, HttpCode, HttpStatus, Param } from '@nestjs/common';
//import { ApiOperation, ApiResponse, ApiTags } from '@nestjs/swagger';
import { RedisService } from './redis.service';

//@ApiTags('redis')
//@Roles(Role.PUBLIC)
@Controller('redis')
export class RedisController {

  constructor(private readonly redisService: RedisService) { }

  @Get('selectQueues')
  @HttpCode(HttpStatus.OK)
  //@ApiOperation({ summary: 'Select queues', description: 'select queues from redis' })
  //@ApiResponse({ status: HttpStatus.BAD_REQUEST, description: 'Bad Request' })
  async getRecurrence() {
    return this.redisService.getAllQueues();
  }

  @Delete('deleteQueue/:name')
  @HttpCode(HttpStatus.OK)
  //@ApiOperation({ summary: 'Delete queues from redis', description: 'Delete queues from redis' })
  //@ApiResponse({ status: HttpStatus.OK, description: 'Queue deleted successfully' })
  //@ApiResponse({ status: HttpStatus.BAD_REQUEST, description: 'Bad Request' })
  async delQueue(@Param('name') id: string) {
    //console.log(params.table);
    //console.log(params.data);
    return this.redisService.delQueues(id);
  }
}