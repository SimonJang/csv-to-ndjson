# csv-to-ndjson [![CI](https://github.com/SimonJang/csv-to-ndjson/actions/workflows/ci.yml/badge.svg?branch=master&event=push)](https://github.com/SimonJang/csv-to-ndjson/actions/workflows/ci.yml?query=branch%3Amaster+event%3Apush)

> Convert a CSV file to [ndjson](http://ndjson.org/) format stream or file.


## Install

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
