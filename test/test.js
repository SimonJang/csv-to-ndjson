import path from 'path';
import fs from 'fs';
import test from 'ava';
import getStream from 'get-stream';
import tempy from 'tempy';
import csvToNDJSON from '../';

const directory = tempy.directory();

test('should fail on invalid path', t => {
	t.throws(() => csvToNDJSON('!foobar?.csv'));
});

test('should fail on wrong file extension', t => {
	t.throws(() => csvToNDJSON('txt.bla'));
});

test('should fail on invalid header argument', t => {
	t.throws(() => csvToNDJSON('./test/csv-test-noheader.csv', {header: {
		name: 'string'
	}}));
});

test('should fail because of internal parser error', async t => {
	let resultError;
	const header = ['Name', 'agE', 'pLace'];
	const ndjsonStream = csvToNDJSON('./test/csv-erroneous-test.csv', {header, delimiter: ';'});
	ndjsonStream.on('error', error => {
		resultError = error;
	}).on('end', () => {
		resultError = null;
	});
	await getStream.array(ndjsonStream).then(null, () => {});

	t.deepEqual(resultError.message, 'Number of columns is inconsistent on line 2');
});

test('should fail on invalid header type', t => {
	t.throws(() => csvToNDJSON('./test/csv-test-noheader.csv', {header: ['name', {age: 'ageTemplate'}, 12]}));
});

test('should fail on invalid destination path', t => {
	t.throws(() => csvToNDJSON('./test/csv-test-noheader.csv', {destination: '!foobar?.json'}));
});

test('should fail on invalid delimiter', t => {
	t.throws(() => csvToNDJSON('./test/csv-test-noheader.csv', {delimiter: ':'}));
});

test('should return a stream of ndjson', async t => {
	const ndjsonStream = csvToNDJSON('./test/csv-test.csv');
	const result = await getStream.array(ndjsonStream);

	t.deepEqual(result, [
		'{"name":"Foo","age":"20","place":"Belgium"}\n',
		'{"name":"Bar","age":"30","place":"Belgium"}\n'
	]);
});

test('should return a stream of ndjson with custom header', async t => {
	const header = ['Name', 'agE', 'pLace'];
	const ndjsonStream = csvToNDJSON('./test/csv-test-noheader.csv', {header, delimiter: ';'});
	const result = await getStream.array(ndjsonStream);

	t.deepEqual(result, [
		'{"name":"Foo","age":"20","place":"Belgium"}\n',
		'{"name":"Bar","age":"30","place":"Belgium"}\n'
	]);
});

test('should write result to a file', async t => {
	const destFile = 'test.json';
	const filePath = path.join(directory, destFile);

	await csvToNDJSON('./test/csv-test.csv', {destination: filePath});

	t.true(fs.existsSync(filePath));

	const fileStream = fs.createReadStream(filePath);
	const result = await getStream(fileStream);

	t.deepEqual(result,
		`{"name":"Foo","age":"20","place":"Belgium"}\n{"name":"Bar","age":"30","place":"Belgium"}\n`
	);
});

test('should write result to a file with custom header', async t => {
	const destFile = 'test2.json';
	const filePath = path.join(directory, destFile);
	const header = ['name', 'age', 'place'];

	await csvToNDJSON('./test/csv-test-noheader.csv', {destination: filePath, header, delimiter: ';'});

	t.true(fs.existsSync(filePath));

	const fileStream = fs.createReadStream(filePath);
	const result = await getStream(fileStream);

	t.deepEqual(result,
		`{"name":"Foo","age":"20","place":"Belgium"}\n{"name":"Bar","age":"30","place":"Belgium"}\n`
	);
});
