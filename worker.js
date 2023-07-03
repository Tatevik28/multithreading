import { parentPort, workerData } from 'worker_threads';
import fs from 'fs';
import path from 'path';
import csvParser from 'csv-parser';
import {fileURLToPath} from "url";
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
function convertCSVToJSON(csvPath) {
    const results = [];
    return new Promise((resolve, reject) => {
        console.log(csvPath)
        fs.createReadStream(csvPath)
            .pipe(csvParser())
            .on('data', (data) => {
                console.log(data, 'data')
                results.push(data.toString())
            })
            .on('end', () => {
                const jsonPath = path.join(
                    __dirname,
                    'converted',
                    path.basename(csvPath, '.csv') + '.json'
                );
                fs.writeFile(jsonPath, JSON.stringify(results), (error) => {
                    if (error) {
                        reject(error);
                    } else {
                        resolve(results.length);
                    }
                });
            })
            .on('error', (error) => {
                reject(error);
            });
        console.log(results)

    });

}

const workerFiles = workerData;

let totalCount = 0;
(async () => {
    for (const file of workerFiles) {
        const count = await convertCSVToJSON(file);
        totalCount += count;
    }
    parentPort.postMessage(totalCount);
    parentPort.close();
})();
