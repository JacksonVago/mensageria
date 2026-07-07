import { ExpressAdapter } from '@bull-board/express';
import { BullBoardModule } from '@bull-board/nestjs';


export const bullboardConfig = BullBoardModule.forRoot({
  route: '/queues',
  adapter: ExpressAdapter,
  boardOptions:
  {
    uiConfig: {
      boardTitle: '',
      boardLogo: {
        path: 'https://appimg.blob.core.windows.net/appimage/NaIvidade_transp.gif',
        width: '124px',
        height: '60px',
      }
    },
  }
});
