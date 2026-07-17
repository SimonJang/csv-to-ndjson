'use strict';
const fs = require('fs');
const Buffer = require('buffer').Buffer;
const Transform = require('stream').Transform;
const isValidPath = require('is-valid-path');

if (!Buffer.from) {
	Buffer.from = function (value, encoding) {
		return new Buffer(value, encoding);
	};
}

if (!Buffer.alloc) {
	Buffer.alloc = function (size) {
		const buffer = new Buffer(size);
		buffer.fill(0);
		return buffer;
	};
}

const parse = require('csv-parse/lib/es5');

const validateHeader = header => {
	for (let index = 0; index < header.length; index++) {
		if (typeof header[index] !== 'string') {
			return false;
		}
	}

	return true;
};
const delimiters = [',', ';'];
const pattern = /\.csv$/i;

const teardown = streams => {
	for (const stream of streams) {
		if (typeof stream.unpipe === 'function') {
			stream.unpipe();
		}
	}

	for (const stream of streams) {
		if (typeof stream.destroy === 'function') {
			stream.destroy();
		} else if (typeof stream.end === 'function') {
			stream.end();
		}
	}
};

module.exports = (path, options) => {
	let getHeader = false;
	let csvHeader;

	if (!isValidPath(path)) {
		throw new Error('Invalid path');
	}

	if (!path.match(pattern)) {
		throw new Error('Invalid file, not a CSV');
	}

	if (options) {
		if (options.header) {
			if (!Array.isArray(options.header) || options.header.length === 0) {
				throw new Error('Invalid header argument');
			}

			if (!validateHeader(options.header)) {
				throw new Error('Header argument can only contains strings');
			}

			getHeader = true;
			csvHeader = options.header.map(item => item.toLowerCase());
		}

		if (options.destination && !isValidPath(options.destination)) {
			throw new Error('Invalid destination path');
		}

		if (options && options.delimiter) {
			if (delimiters.indexOf(options.delimiter) === -1) {
				throw new Error('Invalid delimiter');
			}
		}
	}

	const source = fs.createReadStream(path);
	const parser = parse({delimiter: options && options.delimiter ? options.delimiter : ','});
	const stream = new Transform({objectMode: true});
	stream._transform = (record, encoding, callback) => {
		try {
			const result = Object.create(null);

			if (!getHeader) {
				getHeader = true;
				csvHeader = record.map(item => item.toLowerCase());
				callback();
				return;
			}

			for (const attribute of csvHeader) {
				result[attribute] = record[csvHeader.indexOf(attribute)];
			}
			callback(null, JSON.stringify(result) + '\n');
		} catch (error) {
			callback(error);
		}
	};

	if (options && options.destination) {
		const destination = fs.createWriteStream(options.destination);
		const completion = new Promise((resolve, reject) => {
			let settled = false;
			const fail = error => {
				if (!settled) {
					settled = true;
					teardown([source, parser, stream, destination]);
					reject(error);
				}
			};
			const finish = () => {
				if (!settled) {
					settled = true;
					resolve();
				}
			};

			source.on('error', fail);
			parser.on('error', fail);
			stream.on('error', fail);
			destination.on('error', fail);
			destination.on('finish', finish);
		});

		source.pipe(parser).pipe(stream).pipe(destination);
		return completion;
	}

	let completed = false;
	let failed = false;
	const forwardFailure = error => {
		if (!failed) {
			failed = true;
			teardown([source, parser, stream]);
			stream.emit('error', error);
		}
	};
	const closeOnTransformFailure = () => {
		if (!failed) {
			failed = true;
			teardown([source, parser, stream]);
		}
	};
	const closeUpstreamOnCancellation = () => {
		if (!completed && !failed) {
			failed = true;
			teardown([source, parser]);
		}
	};

	source.on('error', forwardFailure);
	parser.on('error', forwardFailure);
	stream.on('end', () => {
		completed = true;
	});
	stream.on('error', closeOnTransformFailure);
	stream.on('close', closeUpstreamOnCancellation);
	source.pipe(parser).pipe(stream);
	return stream;
};
