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

const captureFileStreams = action => {
	const originalCreateReadStream = fs.createReadStream;
	const originalCreateWriteStream = fs.createWriteStream;
	let source;
	let destination;

	fs.createReadStream = function () {
		source = originalCreateReadStream.apply(fs, arguments);
		return source;
	};
	fs.createWriteStream = function () {
		destination = originalCreateWriteStream.apply(fs, arguments);
		return destination;
	};

	try {
		return {completion: action(), destination, source};
	} finally {
		fs.createReadStream = originalCreateReadStream;
		fs.createWriteStream = originalCreateWriteStream;
	}
};

const waitForClose = stream => stream.closed ? Promise.resolve() : new Promise(resolve => {
	stream.once('close', resolve);
});

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

test('preserves special object-property names from CSV headers', async () => {
	const filePath = path.join(directory, 'special-header.csv');
	fs.writeFileSync(filePath, '__proto__,name\nsafe,Ada\n');

	assert.deepEqual(await collect(csvToNdjson(filePath)), [
		'{"__proto__":"safe","name":"Ada"}\n'
	]);
});

test('forwards input errors and closes the returned stream', {timeout: 500}, async () => {
	const stream = csvToNdjson(path.join(directory, 'missing.csv'));
	const error = await collectStreamFailure(stream);

	assert.equal(error.code, 'ENOENT');
	assert.equal(stream.destroyed, true);
});

test('forwards CSV parser errors and closes the returned stream', {timeout: 500}, async () => {
	const filePath = path.join(directory, 'invalid.csv');
	fs.writeFileSync(filePath, 'name,age\n"unterminated,20\n');

	const stream = csvToNdjson(filePath);
	const error = await collectStreamFailure(stream);

	assert.match(error.message, /quote/i);
	assert.equal(stream.destroyed, true);
});

test('closes the input when a consumer destroys the returned stream', {timeout: 1000}, async () => {
	const filePath = path.join(directory, 'large.csv');
	fs.writeFileSync(filePath, 'name,value\n' + 'Ada,1\n'.repeat(500000));
	const captured = captureFileStreams(() => csvToNdjson(filePath));
	const stream = captured.completion;

	stream.once('data', () => stream.destroy());
	await Promise.all([
		waitForClose(stream),
		waitForClose(captured.source)
	]);

	assert.equal(stream.destroyed, true);
	assert.equal(captured.source.destroyed, true);
});

test('writes the result to a file', async () => {
	const filePath = path.join(directory, 'result.json');

	await csvToNdjson('./test/csv-test.csv', {destination: filePath});

	assert.equal(fs.readFileSync(filePath, 'utf8'),
		'{"name":"Foo","age":"20","place":"Belgium"}\n' +
		'{"name":"Bar","age":"30","place":"Belgium"}\n');
});

test('rejects read failures and closes the destination pipeline', {timeout: 500}, async () => {
	const captured = captureFileStreams(() => csvToNdjson(path.join(directory, 'missing.csv'), {
		destination: path.join(directory, 'missing-result.json')
	}));

	await Promise.all([
		assert.rejects(captured.completion, {code: 'ENOENT'}),
		waitForClose(captured.source),
		waitForClose(captured.destination)
	]);
	assert.equal(captured.source.destroyed, true);
	assert.equal(captured.destination.destroyed, true);
});

test('rejects write failures and closes the destination pipeline', {timeout: 500}, async () => {
	const captured = captureFileStreams(() => csvToNdjson('./test/csv-test.csv', {
		destination: path.join(directory, 'missing-directory', 'result.json')
	}));

	await Promise.all([
		assert.rejects(captured.completion, {code: 'ENOENT'}),
		waitForClose(captured.source),
		waitForClose(captured.destination)
	]);
	assert.equal(captured.source.destroyed, true);
	assert.equal(captured.destination.destroyed, true);
});

test('rejects malformed CSV and closes the destination pipeline', {timeout: 500}, async () => {
	const filePath = path.join(directory, 'invalid-destination.csv');
	fs.writeFileSync(filePath, 'name,age\n"unterminated,20\n');

	const captured = captureFileStreams(() => csvToNdjson(filePath, {
		destination: path.join(directory, 'invalid-result.json')
	}));

	await Promise.all([
		assert.rejects(captured.completion, {message: /quote/i}),
		waitForClose(captured.source),
		waitForClose(captured.destination)
	]);
	assert.equal(captured.source.destroyed, true);
	assert.equal(captured.destination.destroyed, true);
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
