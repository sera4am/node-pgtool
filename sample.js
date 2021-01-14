const DB = require('./db');

(async () => {
	const db = new DB();
	console.time('Access time');

	let model = {
		table_name : 'test',
		columns: {
			id: {type: "int", no_insert: true, no_update: true},
			created_at: {type: "timestamp", no_insert: true, no_update: true},
			updated_at: {type: "timestamp", no_insert: true, default: "now()"},
			value: {type: "string"}
		}
	};

	let val = {value: 'hoge'};
	let v = await db.insert(model, val)
		.catch(e => {
			console.log(e)
		});
	console.log(v);

	let row = await db.first({text: 'SELECT * FROM test WHERE id = $1', values: [v]})
		.catch(e => {
			console.log(e);
		});
	console.log(row);
	row.value = 'fuga';

	await db.update(model, row)
		.catch(e => {
			console.log(e);
		})

	db.close();
	console.timeEnd('Access time');
})();
