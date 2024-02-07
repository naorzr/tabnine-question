import express, { Request, Response } from 'express';
import bodyParser from 'body-parser';
import { MessageQueueManager } from './queue';

const app = express();
const port = 3000;
const messageQueueManager = new MessageQueueManager();

app.use(bodyParser.json());

app.post('/api/:queue_name', (req: Request, res: Response) => {
    const queueName = req.params.queue_name;
    const message = req.body;
    messageQueueManager.postMessage(queueName, message);
    res.status(200).send({ message: 'Message added to the queue.' });
});

app.get('/api/:queue_name', (req: Request, res: Response) => {
    const queueName = req.params.queue_name;
    const timeoutValue = parseInt(req.query.timeout as string);

    messageQueueManager.getMessage(queueName, timeoutValue).subscribe(result => {
        if (result === 'timeout') {
            return res.status(204).end();
        }
        res.status(200).json(result);
    });
});

app.listen(port, () => {
    console.log(`Server listening on port ${port}`);
});
