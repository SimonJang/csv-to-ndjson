'use strict';
const fs = require('fs');
const eol = require('os').EOL;
const csv = require('csv');
const isValidPath = require('is-valid-path');

const validateHeader = header => header.filter(x => typeof x !== 'string').length === 0;
let getHeader = false;
let csvHeader;

module.exports = (path, options) => {
	if (!isValidPath(path)) {
		throw new Error('Invalid path');
	}

	if (options.header) {
		if (!Array.isArray(options.header) || options.header.length > 0) {
			throw new Error('Invalid header argument');
		}

		if (!validateHeader(options.header)) {
			throw new Error('Header argument can only contains strings');
		}

		getHeader = true;
		csvHeader = options.header;
	}

	if (options.destination && !isValidPath(options.destination)) {
		throw new Error('Invalid destination path');
	}

	try {
		const stream = fs.createReadStream(path)
			.pipe(csv.parse())
			.pipe(csv.transform(record => {
				const result = {};

				if (!getHeader) {
					getHeader = true;
					csvHeader = record;
					return;
				}

				for (const attribute of csvHeader) {
					result[csvHeader[attribute]] = record[attribute];
				}
				return JSON.stringify(result) + eol;
			}));

		if (options.destination) {
			fs.createReadStream(stream).pipe(fs.createWriteStream(options.destination));
			return;
		}

		return stream;
	} catch (err) {
		console.error(err);
		throw new Error('Tranforming CSV to ndjson failed');
	}
};
