const DB = require('./db');

(async () => {
	const db = new DB({
		host: 'localhost',
		database: 'test',
		user: 'test',
		port: 5432,
	});

	let model = {
		table_name: 'test',
		columns: {
			id: {type: 'int', no_insert: true, no_update: true},
			created_at: {type: 'timestamp', no_insert: true, no_update: true},
			updated_at: {type: 'timestamp', no_insert: true},
			value: {type: 'string', not_null: true}
		},
	}

	let row = {
		values: 'hello world!'
	};

	await db.insert(model, row)
		.then(ret => {
			console.log(ret);
		})
		.catch(e => {
			console.log(e);
		});

	db.close();
})();