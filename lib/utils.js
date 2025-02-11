import chalk from 'chalk';
import safeStringify from 'fast-safe-stringify';

function handleErrorObject(key, value) {
    if (value instanceof Error) {
        return Object.getOwnPropertyNames(value).reduce(function(error, key) {
            error[key] = value[key]
            return error
        }, {})
    }
    return value
}

export function stringify(o) { return safeStringify(o, handleErrorObject, '  '); }

export function debug() {
    if (!process.env.WINSTON_DYNAMODB_DEBUG) return;
    const args = [].slice.call(arguments);
    const lastParam = args.pop();
    var color = chalk.red;
    if (lastParam !== true) {
        args.push(lastParam);
        color = chalk.green;
    }

    args[0] = color(args[0]);
    args.unshift(chalk.blue('DEBUG:'));
    console.log.apply(console, args);
}