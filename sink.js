import Redis from "ioredis";

const sub = new Redis();

async function main() {
  let lastId = "$";
  while (true) {
    const [[, records]] = await sub.xread(
      "BLOCK",
      0,
      "STREAMS",
      "results_stream",
      lastId
    );
    for (const [id, [, message]] of records) {
      console.log("Message from sink %s", message.toString());
      lastId = id;
    }
  }
}

main().catch((err) => console.log(err));
