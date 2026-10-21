import {
  OnGatewayInit,
  ConnectedSocket,
  MessageBody,
  SubscribeMessage,
  WebSocketGateway,
  WebSocketServer,
} from '@nestjs/websockets';
import { HttpException } from '@nestjs/common';
import { AuthService, isPatientId } from '../auth/auth.service';
import { Server, Socket } from 'socket.io';
import { Measurement } from '../measurements/measurement.entity';
import { PublicationContext } from './realtime-bus.service';
import { checkpoint } from '../notifications/checkpoint';

@WebSocketGateway()
export class RealtimeGateway implements OnGatewayInit {
  @WebSocketServer()
  private server!: Server;

  constructor(private readonly auth: AuthService) {}

  afterInit(server: Server): void {
    server.use((client, next) => {
      try {
        client.data.principal = this.auth.authenticate(client.handshake.auth?.token);
        next();
      } catch {
        const error = Object.assign(new Error('Valid credentials required'), {
          data: { code: 'UNAUTHENTICATED' },
        });
        next(error);
      }
    });
  }

  @SubscribeMessage('subscribe')
  async subscribe(
    @ConnectedSocket() client: Socket,
    @MessageBody() body: unknown,
  ) {
    if (!body || typeof body !== 'object' || Array.isArray(body) ||
        Object.keys(body).some((key) => key !== 'userId') ||
        !isPatientId((body as { userId?: unknown }).userId)) {
      return { subscribed: false, error: { code: 'INVALID_SUBSCRIPTION', message: 'A valid userId is required' } };
    }
    const { userId } = body as { userId: string };
    try {
      this.auth.requirePermission(client.data.principal, userId, 'read');
    } catch (error) {
      if (error instanceof HttpException) {
        return { subscribed: false, error: error.getResponse() };
      }
      throw error;
    }
    await client.join(`user:${userId}`);
    return { subscribed: true, userId };
  }

  publishMeasurement(measurement: Measurement, context: PublicationContext = {}): void {
    const room = this.server.sockets.adapter.rooms.get(`user:${measurement.userId}`);
    if (!room) return;
    for (const id of room) {
      const client = this.server.sockets.sockets.get(id);
      if (!client) continue;
      client.emit('measurement', measurement,
        { cursor: context.position ? checkpoint(measurement.userId, context.position) : null });
    }
  }
}
