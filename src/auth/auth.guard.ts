import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';
import { AuthService, Principal } from './auth.service';

export interface AuthenticatedRequest {
  headers: { authorization?: string };
  principal?: Principal;
}

@Injectable()
export class AuthGuard implements CanActivate {
  constructor(private readonly auth: AuthService) {}

  canActivate(context: ExecutionContext): boolean {
    const request = context.switchToHttp().getRequest<AuthenticatedRequest>();
    const match = /^Bearer ([^\s]+)$/i.exec(request.headers.authorization ?? '');
    request.principal = this.auth.authenticate(match?.[1]);
    return true;
  }
}
