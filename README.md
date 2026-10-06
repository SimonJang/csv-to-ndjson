# csv-to-ndjson [![CI](https://github.com/SimonJang/csv-to-ndjson/actions/workflows/ci.yml/badge.svg?branch=master&event=push)](https://github.com/SimonJang/csv-to-ndjson/actions/workflows/ci.yml?query=branch%3Amaster+event%3Apush)

> Convert a CSV file to [ndjson](http://ndjson.org/) format stream or file.


## Install

Requires Node.js 26 or later.

```
$ npm install csv-to-ndjson
```


## Usage

```js
const csvToNdjson = require('csv-to-ndjson');

const ndjsonStream = csvToNdjson('financialdata.csv', {
	delimiter: ';',
	header: ['Q1', 'Q2', 'Q3', 'Q4']
});
ndjsonStream.pipe(process.stdout);
// => Returns a readable stream of ndjson

csvToNdjson('financialdata.csv', {
	delimiter: ';',
	destination: 'financialdata.json',
	header: ['Q1', 'Q2', 'Q3', 'Q4']
}).then(() => {
	// The destination file has been written.
});
// => Returns a promise when the file is written
```

Both the returned stream and destination file contain one JSON object per line.
Each record ends with LF (`\n`) on every platform, including Windows.

## Migrating to 2.0.0

- Node.js 26 or later is required. Earlier Node.js versions are no longer supported.
- On Windows, output now uses LF (`\n`) instead of CRLF (`\r\n`). Update consumers
  that compare exact bytes or split records using the platform's line ending.
- CSV parsing errors now follow csv-parse 7. If you inspect error codes or messages,
  update those checks; inconsistent field counts use
  `CSV_RECORD_INCONSISTENT_FIELDS_LENGTH`.

## API

### csvToNdjson(input, [options])

#### path

Type: `string`

Path of the CSV file to be read. The file has to end with the `.csv` file extension.

#### options

##### destination

Type: `string`<br>

When destination exists on the options object, the results of the transformation are persisted in a file. <b>When a destination is specified the function doesn't return a readable stream but a promise when the file is written.</b>

##### delimiter

Type: `char`<br>
Default: `,`

If the CSV uses `;` delimiter instead of `,` then this needs to be declared explicitly in the options object.

##### header

Type: `String[]`<br>

When you want to use custom attribute names and not the headers of the CSV file as attribute names, then you can specify an array of attribute names.


## License

MIT © [Simon](https://github.com/SimonJang)
