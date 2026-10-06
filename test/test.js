'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {after, test} = require('node:test');
const csvToNdjson = require('../');

const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'csv-to-ndjson-'));

after(() => {
	fs.rmSync(directory, {recursive: true, force: true});
});

const collect = stream => new Promise((resolve, reject) => {
	const chunks = [];
	stream.on('data', chunk => chunks.push(chunk.toString()));
	stream.on('end', () => resolve(chunks));
	stream.on('error', reject);
});

const collectStreamFailure = stream => new Promise(resolve => {
	let failure;
	let closed = false;
	const finish = () => {
		if (failure && closed) {
			resolve(failure);
		}
	};

	stream.once('error', error => {
		failure = error;
		finish();
	});
	stream.once('close', () => {
		closed = true;
		finish();
	});
});

const capturePipeline = action => {
	const originalCreateReadStream = fs.createReadStream;
	const originalCreateWriteStream = fs.createWriteStream;
	let source;
	let parser;
	let transform;
	let destination;

	fs.createReadStream = function () {
		source = originalCreateReadStream.apply(fs, arguments);
		const sourcePipe = source.pipe;
		source.pipe = function (target, ...args) {
			parser = target;
			const parserPipe = parser.pipe;
			parser.pipe = function (target, ...args) {
				transform = target;
				parser.pipe = parserPipe;
				return parserPipe.call(this, target, ...args);
			};
			source.pipe = sourcePipe;
			return sourcePipe.call(this, target, ...args);
		};
		return source;
	};
	fs.createWriteStream = function () {
		destination = originalCreateWriteStream.apply(fs, arguments);
		return destination;
	};

	try {
		return {completion: action(), source, parser, transform, destination};
	} finally {
		fs.createReadStream = originalCreateReadStream;
		fs.createWriteStream = originalCreateWriteStream;
	}
};

const waitForClose = stream => stream.closed ? Promise.resolve() : new Promise(resolve => {
	stream.once('close', resolve);
});

const waitForPipelineClose = async captured => {
	const streams = [captured.source, captured.parser, captured.transform];
	if (captured.destination) {
		streams.push(captured.destination);
	}

	await Promise.all(streams.map(waitForClose));
	for (const stream of streams) {
		assert.equal(stream.closed, true);
		assert.equal(stream.destroyed, true);
	}
};

test('rejects an invalid path', () => {
	assert.throws(() => csvToNdjson('!foobar?.csv'), {message: 'Invalid path'});
});

test('rejects a file without a CSV extension', () => {
	assert.throws(() => csvToNdjson('txt.bla'), {message: 'Invalid file, not a CSV'});
	assert.throws(() => csvToNdjson('data.csv.backup'), {message: 'Invalid file, not a CSV'});
});

test('rejects an invalid header argument', () => {
	assert.throws(() => csvToNdjson('./test/csv-test-noheader.csv', {
		header: {name: 'string'}
	}), {message: 'Invalid header argument'});
});

test('rejects invalid header values', () => {
	const sparseHeader = [];
	sparseHeader.length = 1;

	assert.throws(() => csvToNdjson('./test/csv-test-noheader.csv', {
		header: ['name', {age: 'ageTemplate'}, 12]
	}), {message: 'Header argument can only contains strings'});
	assert.throws(() => csvToNdjson('./test/csv-test-noheader.csv', {
		header: sparseHeader
	}), {message: 'Header argument can only contains strings'});
});

test('rejects an invalid destination path', () => {
	assert.throws(() => csvToNdjson('./test/csv-test-noheader.csv', {
		destination: '!foobar?.json'
	}), {message: 'Invalid destination path'});
});

test('rejects an unsupported delimiter', () => {
	assert.throws(() => csvToNdjson('./test/csv-test-noheader.csv', {
		delimiter: ':'
	}), {message: 'Invalid delimiter'});
});

test('returns a stream of NDJSON', async () => {
	assert.deepEqual(await collect(csvToNdjson('./test/csv-test.csv')), [
		'{"name":"Foo","age":"20","place":"Belgium"}\n',
		'{"name":"Bar","age":"30","place":"Belgium"}\n'
	]);
});

test('returns a stream of NDJSON with a custom header', async () => {
	assert.deepEqual(await collect(csvToNdjson('./test/csv-test-noheader.csv', {
		header: ['Name', 'agE', 'pLace'],
		delimiter: ';'
	})), [
		'{"name":"Foo","age":"20","place":"Belgium"}\n',
		'{"name":"Bar","age":"30","place":"Belgium"}\n'
	]);
});

for (const fixture of [
	{
		name: 'comma-separated inferred headers',
		csv: 'name,age\nAda,20\n\nBob,30\n\n'
	},
	{
		name: 'semicolon-separated inferred headers',
		csv: 'name;age\nAda;20\n\nBob;30\n\n',
		options: {delimiter: ';'}
	},
	{
		name: 'custom headers',
		csv: 'Ada,20\n\nBob,30\n\n',
		options: {header: ['Name', 'Age']}
	},
	{
		name: 'quoted empty records',
		csv: 'name,age\nAda,20\n""\nBob,30\n""\n'
	}
]) {
	test(`preserves middle and trailing empty records with ${fixture.name}`, async () => {
		const filePath = path.join(directory, `empty-records-${fixture.name}.csv`);
		fs.writeFileSync(filePath, fixture.csv);

		assert.deepEqual(await collect(csvToNdjson(filePath, fixture.options)), [
			'{"name":"Ada","age":"20"}\n',
			'{"name":""}\n',
			'{"name":"Bob","age":"30"}\n',
			'{"name":""}\n'
		]);
	});
}

for (const fixture of [
	{name: 'missing columns', record: 'Bob'},
	{name: 'extra columns', record: 'Bob,30,Belgium'},
	{name: 'a whitespace-only field', record: ' '}
]) {
	test(`still rejects a nonempty record with ${fixture.name} after a blank record`, async () => {
		const filePath = path.join(directory, `invalid-record-${fixture.name}.csv`);
		fs.writeFileSync(filePath, 'name,age\nAda,20\n\n' + fixture.record + '\n');

		await assert.rejects(collect(csvToNdjson(filePath)), {
			code: 'CSV_RECORD_INCONSISTENT_FIELDS_LENGTH'
		});
	});
}

test('writes empty records to a destination and continues with subsequent rows', async () => {
	const inputPath = path.join(directory, 'empty-records-destination.csv');
	const outputPath = path.join(directory, 'empty-records-destination.json');
	fs.writeFileSync(inputPath, 'name,age\nAda,20\n\nBob,30\n\n');

	await csvToNdjson(inputPath, {destination: outputPath});

	assert.equal(fs.readFileSync(outputPath, 'utf8'),
		'{"name":"Ada","age":"20"}\n' +
		'{"name":""}\n' +
		'{"name":"Bob","age":"30"}\n' +
		'{"name":""}\n');
});

test('removes the UTF-8 BOM before reading inferred headers', async () => {
	const filePath = path.join(directory, 'bom-header.csv');
	fs.writeFileSync(filePath, '\uFEFFName,Age\nAda,20\n');

	assert.deepEqual(await collect(csvToNdjson(filePath)), [
		'{"name":"Ada","age":"20"}\n'
	]);
});

test('removes the UTF-8 BOM from data when using custom headers', async () => {
	const filePath = path.join(directory, 'bom-custom-header.csv');
	fs.writeFileSync(filePath, '\uFEFFAda;20\n');

	assert.deepEqual(await collect(csvToNdjson(filePath, {
		header: ['Name', 'Age'],
		delimiter: ';'
	})), ['{"name":"Ada","age":"20"}\n']);
});

test('parses a quoted first header after the UTF-8 BOM', async () => {
	const filePath = path.join(directory, 'bom-quoted-header.csv');
	fs.writeFileSync(filePath, '\uFEFF"Name",Age\nAda,20\n');

	assert.deepEqual(await collect(csvToNdjson(filePath)), [
		'{"name":"Ada","age":"20"}\n'
	]);
});

test('writes BOM-prefixed CSV to a destination without contaminating JSON keys', async () => {
	const inputPath = path.join(directory, 'bom-destination.csv');
	const outputPath = path.join(directory, 'bom-destination.json');
	fs.writeFileSync(inputPath, '\uFEFFName,Age\nAda,20\n');

	await csvToNdjson(inputPath, {destination: outputPath});

	assert.equal(fs.readFileSync(outputPath, 'utf8'), '{"name":"Ada","age":"20"}\n');
});

test('preserves U+FEFF characters inside headers and values', async () => {
	const filePath = path.join(directory, 'embedded-bom.csv');
	fs.writeFileSync(filePath, '\uFEFFNa\uFEFFme,Age\nA\uFEFFda,20\n');

	assert.deepEqual(await collect(csvToNdjson(filePath)), [
		'{"na\uFEFFme":"A\uFEFFda","age":"20"}\n'
	]);
});

test('decodes UTF-16LE CSV identified by its BOM', async () => {
	const filePath = path.join(directory, 'bom-utf16le.csv');
	fs.writeFileSync(filePath, '\uFEFFName,Age\nAda,20\n', 'utf16le');

	assert.deepEqual(await collect(csvToNdjson(filePath)), [
		'{"name":"Ada","age":"20"}\n'
	]);
});

test('decodes UTF-16LE BOM CSV with CRLF records through both APIs', async () => {
	const inputPath = path.join(directory, 'bom-utf16le-crlf.csv');
	const outputPath = path.join(directory, 'bom-utf16le-crlf.json');
	fs.writeFileSync(inputPath, '\uFEFF"Name",Age\r\nZoë,20\r\n', 'utf16le');
	const expected = '{"name":"Zoë","age":"20"}\n';

	assert.deepEqual(await collect(csvToNdjson(inputPath)), [expected]);
	await csvToNdjson(inputPath, {destination: outputPath});
	assert.equal(fs.readFileSync(outputPath, 'utf8'), expected);
});

for (const encoding of ['utf8', 'utf16le']) {
	test(`decodes ${encoding} BOM CSV when characters and CRLF span input chunks`, {timeout: 1000}, async t => {
		const inputPath = path.join(directory, `fragmented-bom-${encoding}.csv`);
		const outputPath = path.join(directory, `fragmented-bom-${encoding}.json`);
		fs.writeFileSync(inputPath, '\uFEFF"Name",Age\r\nZoë,20\r\n', encoding);
		const originalCreateReadStream = fs.createReadStream;
		t.mock.method(fs, 'createReadStream', (file, options) => originalCreateReadStream(file, {
			...options,
			highWaterMark: 1
		}));
		const expected = '{"name":"Zoë","age":"20"}\n';

		assert.deepEqual(await collect(csvToNdjson(inputPath)), [expected]);
		await csvToNdjson(inputPath, {destination: outputPath});
		assert.equal(fs.readFileSync(outputPath, 'utf8'), expected);
	});
}

test('preserves special object-property names from CSV headers', async () => {
	const filePath = path.join(directory, 'special-header.csv');
	fs.writeFileSync(filePath, '__proto__,name\nsafe,Ada\n');

	assert.deepEqual(await collect(csvToNdjson(filePath)), [
		'{"__proto__":"safe","name":"Ada"}\n'
	]);
});

test('forwards input errors and closes the stream pipeline', {timeout: 500}, async () => {
	const captured = capturePipeline(() => csvToNdjson(path.join(directory, 'missing.csv')));
	const [error] = await Promise.all([
		collectStreamFailure(captured.completion),
		waitForPipelineClose(captured)
	]);

	assert.equal(error.code, 'ENOENT');
});

test('forwards CSV parser errors and closes the stream pipeline', {timeout: 500}, async () => {
	const filePath = path.join(directory, 'invalid.csv');
	fs.writeFileSync(filePath, 'name,age\n"unterminated,20\n');

	const captured = capturePipeline(() => csvToNdjson(filePath));
	const [error] = await Promise.all([
		collectStreamFailure(captured.completion),
		waitForPipelineClose(captured)
	]);

	assert.match(error.message, /quote/i);
});

test('forwards inconsistent semicolon record errors with custom headers and closes the pipeline', {timeout: 500}, async () => {
	const captured = capturePipeline(() => csvToNdjson('./test/csv-erroneous-test.csv', {
		header: ['Name', 'agE', 'pLace'],
		delimiter: ';'
	}));
	const [error] = await Promise.all([
		collectStreamFailure(captured.completion),
		waitForPipelineClose(captured)
	]);

	assert.equal(error.code, 'CSV_RECORD_INCONSISTENT_FIELDS_LENGTH');
	assert.equal(error.lines, 2);
});

test('closes every pipeline stream when a consumer destroys the returned stream', {timeout: 1000}, async () => {
	const filePath = path.join(directory, 'large.csv');
	fs.writeFileSync(filePath, 'name,value\n' + 'Ada,1\n'.repeat(500000));
	const captured = capturePipeline(() => csvToNdjson(filePath));
	const stream = captured.completion;

	stream.once('data', () => stream.destroy());
	await waitForPipelineClose(captured);
});

test('resolves the destination promise only after the file writer finishes', {timeout: 1000}, async t => {
	const filePath = path.join(directory, 'result.json');
	const writeBlocked = Promise.withResolvers();
	const conversionEnded = Promise.withResolvers();
	const originalCreateWriteStream = fs.createWriteStream;
	let destination;
	let holdWrite = true;
	let releaseWrite;

	// Use real filesystem writes, withholding only the first completion callback.
	const gatedWrite = method => (...args) => {
		const callback = args.pop();
		fs[method](...args, (...result) => {
			if (!holdWrite || result[0]) {
				callback(...result);
				return;
			}
			holdWrite = false;
			releaseWrite = () => {
				releaseWrite = undefined;
				callback(...result);
			};
			writeBlocked.resolve();
		});
	};

	t.mock.method(fs, 'createWriteStream', (file, options) => {
		destination = originalCreateWriteStream(file, {
			...options,
			fs: {
				open: fs.open,
				close: fs.close,
				write: gatedWrite('write'),
				writev: gatedWrite('writev')
			}
		});
		destination.once('pipe', source => source.once('end', conversionEnded.resolve));
		return destination;
	});
	t.after(async () => {
		holdWrite = false;
		releaseWrite?.();
		if (destination && !destination.closed) {
			destination.destroy();
			await waitForClose(destination);
		}
	});

	const completion = csvToNdjson('./test/csv-test.csv', {destination: filePath});
	let settled = false;
	completion.then(() => { settled = true; }, () => { settled = true; });

	try {
		await Promise.all([writeBlocked.promise, conversionEnded.promise]);
		assert.equal(destination.writableFinished, false);
		assert.equal(settled, false, 'conversion ending must not settle the destination promise');
	} finally {
		releaseWrite?.();
	}

	await completion;
	assert.equal(destination.writableFinished, true);
	assert.equal(fs.readFileSync(filePath, 'utf8'),
		'{"name":"Foo","age":"20","place":"Belgium"}\n' +
		'{"name":"Bar","age":"30","place":"Belgium"}\n');
});

test('rejects read failures and closes the destination pipeline', {timeout: 500}, async () => {
	const captured = capturePipeline(() => csvToNdjson(path.join(directory, 'missing.csv'), {
		destination: path.join(directory, 'missing-result.json')
	}));

	await Promise.all([
		assert.rejects(captured.completion, {code: 'ENOENT'}),
		waitForPipelineClose(captured)
	]);
});

test('rejects write failures and closes the destination pipeline', {timeout: 1000}, async () => {
	const filePath = path.join(directory, 'write-failure.csv');
	fs.writeFileSync(filePath, 'name,age\n' + 'Ada,20\n'.repeat(500000));
	const captured = capturePipeline(() => csvToNdjson(filePath, {
		destination: path.join(directory, 'missing-directory', 'result.json')
	}));

	await Promise.all([
		assert.rejects(captured.completion, {code: 'ENOENT'}),
		waitForPipelineClose(captured)
	]);
});

test('rejects an early CSV parser failure and closes the destination pipeline', {timeout: 1000}, async () => {
	const filePath = path.join(directory, 'invalid-destination.csv');
	fs.writeFileSync(filePath, 'name,age\nAda,"20"oops\n' + 'Bob,30\n'.repeat(500000));

	const captured = capturePipeline(() => csvToNdjson(filePath, {
		destination: path.join(directory, 'invalid-result.json')
	}));

	await Promise.all([
		assert.rejects(captured.completion, {message: /quote/i}),
		waitForPipelineClose(captured)
	]);
});

test('writes a custom-header result to a file', async () => {
	const filePath = path.join(directory, 'custom-result.json');

	await csvToNdjson('./test/csv-test-noheader.csv', {
		destination: filePath,
		header: ['name', 'age', 'place'],
		delimiter: ';'
	});

	assert.equal(fs.readFileSync(filePath, 'utf8'),
		'{"name":"Foo","age":"20","place":"Belgium"}\n' +
		'{"name":"Bar","age":"30","place":"Belgium"}\n');
});
