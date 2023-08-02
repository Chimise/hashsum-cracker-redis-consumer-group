import Redis from "ioredis";
import { generateTasks } from "./generate-task.js";

const ALPHABETS = "abcdefghijklmnopqrstuvwxyz";
const BATCH_SIZE = 10000;

const pub = new Redis();

const [, , maxLength, searchHash] = process.argv;

async function main() {
  const generatorObj = generateTasks(
    searchHash,
    ALPHABETS,
    maxLength,
    BATCH_SIZE
  );

  for (const task of generatorObj) {
    await pub.xadd("tasks_stream", "*", "task", Buffer.from(JSON.stringify(task)));
  }

  pub.disconnect();
}

main().catch((err) => console.log(err));
