import { Subject, Observable, of, from } from 'rxjs';
import { first, timeout, catchError } from 'rxjs/operators';

interface Message {
    [key: string]: unknown;
}

export class MessageQueueManager {
    private queues: Map<string, { messages: Message[]; subject: Subject<Message> }> = new Map();

    constructor(private defaultTimeout: number = 10000) { }

    private ensureQueueExists(queueName: string) {
        if (!this.queues.has(queueName)) {
            this.queues.set(queueName, { messages: [], subject: new Subject<Message>() });
        }
    }

    public postMessage(queueName: string, message: Message) {
        this.ensureQueueExists(queueName);
        const queue = this.queues.get(queueName)!;
        if (queue.subject.observers.length > 0) {
            queue.subject.next(message);
        } else {
            queue.messages.push(message);
        }
    }

    public getMessage(queueName: string, timeoutValue?: number): Observable<Message | 'timeout'> {
        this.ensureQueueExists(queueName);
        const queue = this.queues.get(queueName)!;
        const useTimeout = timeoutValue || this.defaultTimeout;

        if (queue.messages.length > 0) {
            const message = queue.messages.shift()!;
            return of(message);
        } else {
            return queue.subject.asObservable().pipe(
                first(),
                timeout(useTimeout),
                catchError(() => of('timeout' as const))
            );
        }
    }
}
