import { MessageQueueManager } from "./queue";

type Message = Record<string, unknown>;

describe('MessageQueueManager', () => {
    let queueManager: MessageQueueManager;

    beforeEach(() => {
        queueManager = new MessageQueueManager(1000); // Using a shorter timeout for testing
    });


    test('should serve messages in the order they were posted', done => {
        const testQueueName = 'orderedQueue';
        const messages = [
            { content: 'First message' },
            { content: 'Second message' },
            { content: 'Third message' },
        ];

        // Post all messages
        messages.forEach(message => queueManager.postMessage(testQueueName, message));

        // Retrieve messages and verify order
        const receivedMessages: Message[] = [];
        queueManager.getMessage(testQueueName).subscribe(firstMessage => {
            receivedMessages.push(firstMessage as Message);
            queueManager.getMessage(testQueueName).subscribe(secondMessage => {
                receivedMessages.push(secondMessage as Message);
                queueManager.getMessage(testQueueName).subscribe(thirdMessage => {
                    receivedMessages.push(thirdMessage as Message);

                    expect(receivedMessages).toEqual(messages);
                    done();
                });
            });
        });
    });

    test('should allow posting and retrieving from multiple queues independently', done => {
        const queueNames = ['queue1', 'queue2'];
        const messages = { queue1: { content: 'Message for queue1' }, queue2: { content: 'Message for queue2' } };

        // Post a message to each queue
        queueManager.postMessage(queueNames[0], messages.queue1);
        queueManager.postMessage(queueNames[1], messages.queue2);

        // Retrieve and verify each message from its respective queue
        const retrievedMessages: Partial<{ [key: string]: Message }> = {};

        queueManager.getMessage(queueNames[0]).subscribe(message1 => {
            retrievedMessages.queue1 = message1 as Message;
            queueManager.getMessage(queueNames[1]).subscribe(message2 => {
                retrievedMessages.queue2 = message2 as Message;

                expect(retrievedMessages.queue1).toEqual(messages.queue1);
                expect(retrievedMessages.queue2).toEqual(messages.queue2);
                done();
            });
        });
    });

    test('should not serve the same message to multiple subscribers', done => {
        const testQueueName = 'exclusiveMessageQueue';
        const message = { content: 'Exclusive message' };

        queueManager.postMessage(testQueueName, message);

        // Attempt to get the message with two concurrent subscribers
        const receivedMessages: ({} | 'timeout')[] = [];

        queueManager.getMessage(testQueueName).subscribe(msg1 => {
            receivedMessages.push(msg1);
            if (receivedMessages.length === 2) {
                // Ensure one message and one timeout
                expect(receivedMessages).toContainEqual(message);
                expect(receivedMessages).toContain('timeout');
                done();
            }
        });

        queueManager.getMessage(testQueueName).subscribe(msg2 => {
            receivedMessages.push(msg2);
            if (receivedMessages.length === 2) {
                // Ensure one message and one timeout
                expect(receivedMessages).toContainEqual(message);
                expect(receivedMessages).toContain('timeout');
                done();
            }
        });
    });


    test('should post and retrieve a message successfully', done => {
        const testQueueName = 'testQueue';
        const testMessage = { content: 'Hello, world!' };

        queueManager.postMessage(testQueueName, testMessage);

        queueManager.getMessage(testQueueName).subscribe(message => {
            expect(message).toEqual('testMessage');
            done();
        });
    });

    test('should handle timeouts when no message is posted', done => {
        const testQueueName = 'emptyQueue';

        queueManager.getMessage(testQueueName, 500).subscribe(result => {
            expect(result).toBe('timeout');
            done();
        });
    });

    test('should ensure a message is consumed by only one subscriber', done => {
        const testQueueName = 'testQueueSingleUse';
        const testMessage = { content: 'Single use message' };

        queueManager.postMessage(testQueueName, testMessage);

        const subscriber1 = queueManager.getMessage(testQueueName);
        const subscriber2 = queueManager.getMessage(testQueueName);

        subscriber1.subscribe(message => {
            expect(message).toEqual(testMessage);
            subscriber2.subscribe(result => {
                expect(result).toBe('timeout');
                done();
            });
        });
    });

});
