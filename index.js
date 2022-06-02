const Redis = require('ioredis');
const Resque = require('node-resque');
const { parseExpression } = require('cron-parser');
const stringHash = require('string-hash');

const connectionDetails = {
    host: "127.0.0.1",
    options: {
        password: 1234,
        tls: false
    },
    port: 6379
};
const resqueConnection = { ...connectionDetails, pkg: 'ioredis' };
const redis = new Redis({
    port: connectionDetails.port,
    host: connectionDetails.host,
    password: connectionDetails.options.password,
    tls: connectionDetails.options.tls
});
const queue = new Resque.Queue({ connection: resqueConnection });

const cronAnnotation = "screwdriver.cd/buildPeriodically";
const periodicBuildConfigs = "periodicBuildConfigs";
const periodicBuildQueue = 'periodicBuilds';

function evaluateHash(hash, min, max) {
    return (hash % (max + 1 - min)) + min;
};

function transformValue(cronValue, min, max, hashValue) {
    const values = cronValue.split(',');

    // Transform each ',' seperated value
    // Ignore values that do not have a valid 'H' symbol
    values.forEach((value, i) => {
        // 'H' should evaluate to some value within the range (e.g. [0-59])
        if (value === 'H') {
            values[i] = evaluateHash(hashValue, min, max);

            return;
        }

        // e.g. H/5 -> #/5
        if (value.match(/H\/\d+/)) {
            values[i] = value.replace('H', evaluateHash(hashValue, min, max));

            return;
        }

        // e.g. H(0-5) -> #
        if (value.match(/H\(\d+-\d+\)/)) {
            const newMin = Number(value.substring(2, value.lastIndexOf('-')));
            const newMax = Number(value.substring(value.lastIndexOf('-') + 1, value.lastIndexOf(')')));

            // Range is invalid, throw an error
            if (newMin < min || newMax > max || newMin > newMax) {
                throw new Error(`${value} has an invalid range, expected range ${min}-${max}`);
            }

            values[i] = evaluateHash(hashValue, newMin, newMax);
        }
    });

    return values.join(',');
};

function transformCron(cronExp, jobId) {
    const fields = cronExp.trim().split(/\s+/);

    // The seconds field is not allowed (e.g. '* * * * * *')
    if (fields.length !== 5) {
        throw new Error(`${cronExp} does not have exactly 5 fields`);
    }

    const jobIdHash = stringHash(jobId.toString());

    if (fields[0] !== 'H' && !fields[0].match(/H\(\d+-\d+\)/)) {
        fields[0] = 'H';
    }

    // Minutes [0-59]
    fields[0] = transformValue(fields[0], 0, 59, jobIdHash);
    // Hours [0-23]
    fields[1] = transformValue(fields[1], 0, 23, jobIdHash);
    // Day of month [1-31]
    fields[2] = transformValue(fields[2], 1, 31, jobIdHash);
    // Months [1-12]
    fields[3] = transformValue(fields[3], 1, 12, jobIdHash);
    // Day of week [0-6]
    fields[4] = transformValue(fields[4], 0, 6, jobIdHash);

    const newCronExp = fields.join(' ');

    // Perform final validation before returning
    parseExpression(newCronExp);

    return newCronExp;
};

function nextExecution(cronExp) {
    const interval = parseExpression(cronExp);

    return interval.next().getTime();
};

async function main() {
    await queue.connect();

    const stream = redis.hscanStream(periodicBuildConfigs);

    stream.on("data", (resultKeys) => {
        stream.pause();

        Promise.all(resultKeys.map((key) => {
            if (isNaN(key)) {
                const { job } = JSON.parse(key);
                const cron = job.permutations[0].annotations[cronAnnotation];
                const next = nextExecution(transformCron(cron, job.id));
                console.log(`processing job ${job.id}`);

                queue.enqueueAt(next, periodicBuildQueue, 'startDelayed', [{ jobId: job.id }])
                    .then(() => {
                        console.log(`reenqueued job ${job.id} into periodicBuildQueue`);
                    })
                    .catch((err) => {
                        console.log(`failed to enqueue for job ${job.id}: ${err}`);
                    });
            }
        })).then(() => {
            stream.resume();
        });
    });

    stream.on("end", () => {
        console.log('done with failsafe');
    });
}

if (require.main === module) {
    main();
}
