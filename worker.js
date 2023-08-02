import Redis from "ioredis";
import { processTask } from "./process-task.js";

const sub = new Redis();
const GROUP = "workers_group";
const STREAM = "tasks_stream";

const [, , consumerName] = process.argv;

if (!consumerName) {
  console.error("Must provide a consumer name");
  process.exit(1);
}


async function main() {
  await sub
    .xgroup("CREATE", STREAM, GROUP, "$", "MKSTREAM")
    .catch((err) => console.log("Consumer group already exists"));

  const [[, records]] = await sub.xreadgroup(
    "GROUP",
    GROUP,
    consumerName,
    "STREAMS",
    STREAM,
    "0"
  );

  for (const [id, [, task]] of records) {
    await processAndAck(id, task);
  }

  while (true) {
    const [[, records]] = await sub.xreadgroup(
      "GROUP",
      GROUP,
      consumerName,
      "BLOCK",
      "0",
      "COUNT",
      "1",
      "STREAMS",
      STREAM,
      ">"
    );

    for (const [id, [, task]] of records) {
      await processAndAck(id, task);
    }
  }
}

async function processAndAck(recordId, task) {
  const found = processTask(JSON.parse(task.toString()));
  if (found) {
    console.log(`Found! => ${found}`);
    await sub.xadd("results_stream", "*", "result", `Found: ${found}`);
  }

  await sub.xack(STREAM, GROUP, recordId);
}

main().catch((err) => console.log(err));
