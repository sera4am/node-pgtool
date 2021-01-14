const config = require('config');
const { Pool } = require('pg');
const Cursor = require('pg-cursor');

module.exports = class {

	constructor(option = null) {
		if (!option) {
			option = config.database;
		}
		this.pool = new Pool(option);
	}

	close = (() => {
		this.pool.end();
	})

	client = ((beginTransaction = null) => {
		return new Promise(async (resolve, reject) => {
			await this.pool.connect()
				.then(async client => {
					if (beginTransaction === true) {
						await this.begin(client)
							.then(() => {
								resolve(client);
							})
							.catch(e => {
								reject(e);
							});
					} else {
						reject(client);
					}
				})
				.catch(e => {
					reject(e);
				});
		});
	});

	begin = (client => {
		return new Promise(async (resolve, reject) => {
			if (!client) {
				return reject('Client not defined');
			}
			await client.query('BEGIN')
				.then(() => {
					resolve();
				})
				.catch(e => {
					reject(e);
				});
		});
	});

	commit = (client => {
		return new Promise(async (resolve, reject) => {
			if (!client) {
				return reject('Client not defined');
			}
			if (typeof  client !== 'object') {
				return reject('Invalid argument', typeof client);
			}
			await client.query('COMMIT')
				.then(() => {
					resolve();
				})
				.catch(e => {
					reject(e);
				})
		});
	});

	rollback = (client => {
		return new Promise(async (resolve, reject) => {
			if (!client) {
				return reject('Client not defined');
			}
			await client.query('ROLLBACK')
				.then(() => {
					resolve();
				})
				.catch(e => {
					reject(e);
				})
		});
	});

	query = ((options, client = null) => {
		return new Promise(async (resolve, reject) => {
			let isClientCreated = false;
			try {
				if (!client) {
					client = await this.pool.connect()
						.catch(e => { throw e});
					isClientCreated = true;
				}
				console.log('[Query]', options);
				await client.query(options)
					.then(ret => {
						resolve({rows:ret.rows});
					})
					.catch(e => { throw e});
			} catch(e) {
				reject(e);
			} finally {
				isClientCreated && client.release();
			}
		});
	});

	first = ((options, client = null) => {
		return new Promise(async (resolve, reject) => {
			let isClientCreated = false;
			if (!client) {
				client = await this.pool.connect();
				isClientCreated = true;
			}

			try {
				let text = options.text;
				let values = options.values;
				delete options.text;
				delete options.values;
				let cursor = client.query(new Cursor(text, values, options));
				cursor.read(1, (err, rows) => {
					cursor.close(() => {
						if (rows.length > 0) {
							resolve(rows[0]);
						} else {
							resolve(null);
						}
					});
				});
			} catch(e) {
				reject(e);
			} finally {
				isClientCreated && client.release();
			}
		});
	});

	one = ((options, client = null) => {
		return new Promise(async (resolve, reject) => {
			options.rowMode = 'array';
			try {
				await this.first(options, client)
					.then(ret => {
						resolve(ret[0]);
					})
					.catch(e => { throw e});
			} catch(e) {
				reject(e);
			}
		});
	});

	insert = ((model, data, client = null, tableName = null, idColumn = 'id', returnId = true) => {
		return new Promise(async (resolve, reject) => {
			let isClientCreated = false;
			try {
				let result = true;
				let values = [];
				let questions = [];
				let columns = [];
				if (!tableName) {
					tableName = model.table_name;
				}

				for (const [key, item] of Object.entries(model.columns)) {
					if (item.no_insert) { continue }
					if (item.type.toLowerCase() === 'foreign') { continue }

					if (data[key] === undefined || data[key] === null) {
						if (item.not_null && (item.default === undefined || item.default === null)) {
							return reject(`${tableName}:${key} is required`);
						}
						if (item.default === undefined) {
							data[key] = null;
						} else if (item.default.toLowerCase() === 'now()') {
							data[key] = 'now()';
						} else {
							data[key] = item.default;
						}
					}

					let srid = item.srid || 4326;
					switch (item.type) {
						case 'int[]':
							if (!data[key]) {
								questions.push('null');
							} else {
								let q = [];
								for (let d of data[key]) {
									values.push(d);
									q.push('$' + values.length);
								}
								questions.push(`ARRAY[${q.join(',')}]::int[]`);
							}
							break;
						case 'string[]':
							if (!data[key]) {
								questions.push('null');
							} else {
								let q = [];
								for (let d of data[key]) {
									values.push(d);
									q.push('$' + values.length);
								}
								questions.push(`ARRAY[${q.join(',')}]::text[]`);
							}
							break;
						case 'geo_point':
							if (!data[key] || data[key].length === 0) {
								questions.push('null');
							} else {
								let lat = parseFloat(data[key][0]);
								let lng = parseFloat(data[key][1]);
								if (!lat || !lng) {
									throw new Error(`Invalid geo string: ${data[key].join(',')} at [${key}]`);
								}
								questions.push(`ST_SetSRID(ST_MakePoint(${lng},${lat}),${srid})`);
							}
							break;
						case 'geo_polyline':
							if (!data[key] || data[key].length === 0) {
								questions.push('null');
							} else {
								let coords = [];
								for (let coord of data[key]) {
									let lat = parseFloat(coord[0]);
									let lng = parseFloat(coord[1]);
									if (!lat || !lng) {
										throw new Error(`Invalid geo string: ${data[key].join(',')} at [${key}]`);
									}
									coords.push(`ST_MakePoint(${lng},${lat})`);
								}
								if (coords.length > 0) {
									questions.push(`ST_SetSRID(ST_MakeLine(ARRAY[${coords.join(',')}]),${srid})`);
								} else {
									questions.push('null');
								}
							}
							break
						default:
							values.push(data[key]);
							questions.push('$' + values.length);
					}
					columns.push(key);
				}

				let sql = `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${questions.join(', ')})`;
				console.log('[Insert]', sql, values);
				if (returnId) {
					sql += ` RETURNING ${idColumn}`;
				}
				if (!client) {
					client = await this.client(true);
					isClientCreated = true;
				}

				let res = await client.query({text: sql, values: values, rowMode: 'array'});
				result = true;
				if (returnId) {
					if (res && res.rows && res.rows[0]) {
						result = res.rows[0][0];
					}
					resolve(result);
				}
				isClientCreated && await this.commit(client).catch(e => { throw e});
			} catch(e) {
				reject(e);
			} finally {
				isClientCreated && client.release();
			}
		});
	});

	update = ((model, data, client = null, tableName = null, idColumn = 'id') => {
		return new Promise(async (resolve, reject) => {
			let isClientCreated = false;
			let result = false;
			let values = [];
			let updates = [];

			try {
				if (!tableName) {
					tableName = model.table_name;
				}

				for (const [key, item] of Object.entries(model.columns)) {
					if (item.no_update) { continue }
					if (item.type.toLowerCase() === 'foreign') { continue }

					if (data[key] === undefined || data[key] === null) {
						if (item.not_null && (item.default === undefined || item.default === null)) {
							return reject(`${tableName}:${key} is required`);
						}
						if (item.default === undefined) {
							data[key] = null;
						} else if (item.default.toLowerCase() === 'now()') {
							data[key] = 'now()';
						} else {
							data[key] = item.default;
						}
					}

					let srid = item.sird || 4326;
					switch (item.type) {
						case 'array':
							if (!data[key]) {
								updates.push(`${key} = null`);
							} else {
								let n = [];
								for (const d of data[key]) {
									values.push(d);
									n.push('$' + values.length);
								}
								updates.push(`${key} = ARRAY[${n.join(', ')}]::int[]`);
							}
							break;
						case 'geo_point':
							if (!data[key]) {
								updates.push(`${key} = null`);
							} else {
								let lat = parseFloat(data[key][0]);
								let lng = parseFloat(data[key][1]);
								if (!lat || !lng) {
									throw new Error(`Invalid geo parameter: ${data[key].join(',')} at [${key}`);
								}
								updates.push(`${key} = ST_SetSRID(ST_MakePoint(${lng},${lat}), ${srid})`);
							}
							break;
						case 'geo_polyline':
							if (!data[key]) {
								updates.push(`${key} = null`);
							} else {
								let coords = [];
								for (let coord of data[key]) {
									let lat = parseFloat(coord[0]);
									let lng = parseFloat(coord[1]);
									if (!lat || !lng) {
										throw new Error(`Invalid geo parameter: ${coord.join(',')} at [${key}]`);
									}
									coords.push(`ST_MakePoint(${lng},${lat})`);
								}
								if (coords.length > 0) {
									updates.push(`${key} = ST_SetSRID(ST_MakeLine(ARRAY[${coords.join(',')}]), ${srid})`);
								} else {
									updates.push(`${key} = null`);
								}
							}
							break;
						default:
							values.push(data[key]);
							updates.push(`${key} = $${values.length}`);
					}
				}

				values.push(data[idColumn]);
				let sql = `UPDATE ${tableName} SET ${updates.join(',')} WHERE ${idColumn} = $${values.length}`;
				console.log('[DB]', 'Update', sql, values);

				if (!client) {
					client = await this.client(true);
					isClientCreated = true;
				}

				await client.query({text: sql, values: values, rowMode: 'array'})
					.catch(e => { throw e});
				isClientCreated && await this.commit(client).catch(e => { throw e});
				resolve();
			} catch(e) {
				reject(e);
			} finally {
				isClientCreated && client.release();
			}
		});
	});
};

