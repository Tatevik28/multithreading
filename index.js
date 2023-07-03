
import {Worker, isMainThread, parentPort, workerData} from 'worker_threads';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import os from 'os';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const csvDirectory = process.argv[2];
let processedCount;
let workersCount;
let workerScript;
let startTime;

if (!csvDirectory) {
    console.error('Please enter directory name.');
    process.exit(1);
}

const convertedDirectory = path.join(__dirname, 'converted');
if (!fs.existsSync(convertedDirectory)) {
    fs.mkdirSync(convertedDirectory);
}

function distributeTasks(csvFiles) {
    const threadsCount = Math.min(csvFiles.length, os.cpus().length);
    const filesPerThread = Math.ceil(csvFiles.length / threadsCount);

    for (let i = 0; i < threadsCount; i++) {
        const start = i * filesPerThread;
        const end = start + filesPerThread;
        const workerFiles = csvFiles.slice(start, end);
        const worker = new Worker(workerScript, { workerData: workerFiles });

        worker.on('message', (count) => {
            processedCount += count;
            workersCount++;
            if (workersCount === threadsCount) {
                const endTime = new Date().getTime();
                const duration = endTime - startTime;
                console.log('Overall count:', processedCount);
                console.log('Parsing duration (ms):', duration);
                process.exit(0);
            }
        });
        worker.on('error', (error) => {
            console.error(error);
        });
        worker.on('exit', (code) => {
            if (code !== 0) {
                console.error(`Worker stopped with exit code ${code}`);
            }
        });
    }
}

if (isMainThread) {
    const csvFiles = fs
        .readdirSync(csvDirectory)
        .filter((file) => file.endsWith('.csv'))
        .map((file) => path.join(csvDirectory, file));

    workerScript = new URL('./worker.js', import.meta.url).pathname;
    console.log(workerScript)

    startTime = new Date().getTime();

    processedCount = 0;
    workersCount = 0;

    distributeTasks(csvFiles);
}

