/**
 * sqlite adapter for jelidb
 */

(function(root, factory) {
    if (typeof define === 'function' && define.amd) {
        // AMD. Register as an anonymous module.
        define([], factory);
    } else if (typeof module === 'object' && module.exports) {
        // Node. Does not work with strict CommonJS, but 
        // only CommonJS-like environments that support module.exports, 
        // like Node. 
        module.exports = factory();
    } else {
        // Browser globals (root is window) 
        root.jSQLAdapter = factory();
    }
}(typeof self !== 'undefined' ? self : this, function() {
    /**
     * set Prototype
     * create Database
     * Param Object
     * {name: "mySQLite.db", location: 'default'}
     */

    var dbName;
    var _privateStore = {};
    var _errorTables = [];
    var _sqlFacade, type;

    /**
     * 
     * @param {*} data 
     */
    function deepClone(data) {
        return JSON.parse(JSON.stringify(data));
    }

    /**
     * 
     * @param {*} data 
     */
    function isArray(data) {
        return toString.call(data) === "[object Array]";
    }

    /**
     * 
     * @param {*} $dbName 
     * @param {*} options 
     */
    function createDB(options) {
        /**
         * check if name contains .db ext
         */
        options.name = options.name + ".db";
        if (window.sqlitePlugin) {
            return window.sqlitePlugin.openDatabase(options);
        } else if (window.openDatabase) {
            return window.openDatabase(options.name, options.version || 1, options.name + ' Storage for webSql', 50 * 1024 * 1024)
        }

        return null;
    }

    /**
     * 
     * @param {*} config 
     * @param {*} dbToStorageInstance 
     * @param {*} CB 
     */
    function dbInit(config, dbToStorageInstance, CB) {
        dbName = config.name;
        _sqlFacade = new CoreSqlFacade(createDB(config));
        var _storageFacade = new StorageFacade(dbToStorageInstance.generateStruct);

        function loadAllData() {
            _sqlFacade.query('SELECT * FROM _JELI_STORE_', [])
                .then(function(tx, results) {
                    var len = results.rows.length,
                        i;
                    for (i = 0; i < len; i++) {
                        _privateStore[results.rows.item(i)._rev] = JSON.parse(results.rows.item(i)._data);
                    }

                    loadDBData();
                }, txError);
        }

        function loadDBData() {
            if (!_privateStore.version) {
                (CB || noop)();
                return;
            }
            var tableNames = Object.keys(_privateStore[dbToStorageInstance.storeMapping.resourceName].resourceManager);

            resolveTableData();

            function resolveTableData() {
                if (!tableNames.length) {
                    // trigger our callback
                    (CB || noop)();
                    return;
                }
                var current = tableNames.shift();
                _privateStore[current + ":data"] = [];
                /**
                 * check if table has data before querying database
                 */
                var MAXIMUM_RESULT = 1000,
                    TOTAL_RECORDS = _privateStore[current].lastInsertId,
                    chunkQueries = [];
                if (TOTAL_RECORDS > 0) {
                    /**
                     * chunk query
                     */
                    if (TOTAL_RECORDS > MAXIMUM_RESULT) {
                        console.log('[JDB SQL_ADAPTER]: preparing chunking of table ' + current);
                        for (var i = 0; i <= TOTAL_RECORDS; i += MAXIMUM_RESULT) {
                            chunkQueries.push([i, MAXIMUM_RESULT])
                        }
                        startChunkQuery();
                    } else {
                        _sqlFacade.select('select * from ' + current)
                            .then(success, error);
                    }

                } else {
                    resolveTableData();
                }

                /**
                 * 
                 * @param {*} tx 
                 * @param {*} results 
                 */
                function success(tx, results) {
                    var len = results.rows.length,
                        i;
                    for (i = 0; i < len; i++) {
                        var data = ({
                                _ref: results.rows.item(i)._ref,
                                _data: {}
                            }),
                            columns = _storageFacade.getItem(current).columns[0],
                            jsonParserTypes = ['object', 'array', 'boolean'];
                        Object.keys(columns)
                            .forEach(function(key) {
                                var value = results.rows.item(i)[key];
                                // is OBJECT or ARRAY
                                if (value &&
                                    typeof value === "string" &&
                                    (
                                        jsonParserTypes.indexOf(columns[key].type.toLowerCase()) > -1
                                    )
                                ) {
                                    value = JSON.parse(value);
                                }

                                data._data[key] = value;
                            });
                        _privateStore[current + ":data"].push(data);
                    }

                    nextQuery();
                }

                function startChunkQuery() {
                    var query = chunkQueries.shift()
                    _sqlFacade.select('select * from ' + current + ' limit ?,?', query)
                        .then(success, function(err) {
                            chunkQueries.push(query);
                            error(err);
                        });
                }

                function nextQuery() {
                    if (chunkQueries.length) {
                        startChunkQuery();
                    } else {
                        // loadNextData
                        resolveTableData();
                    }
                }

                /**
                 * 
                 * @param {*} err 
                 */
                function error(err) {
                    console.log('[JDB SQL_ADAPTER]: failed to load:', current);
                    _errorTables.push(current);
                    nextQuery();
                }
            }
        }

        /**
         * 
         * @param {*} tbl 
         * @param {*} definition 
         */
        function createTable(tbl, definition) {
            var columns = ['_ref unique'];
            if (definition.columns[0]) {
                columns = columns.concat(Object.keys(definition.columns[0]));
            }
            _sqlFacade.createTable(tbl, columns);
            _storageFacade.setItem(tbl, definition);
            _privateStore[tbl + ":data"] = [];
        }

        dbToStorageInstance
            .storageEventHandler
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'insert'), function(tbl, data, insertData) {
                _privateStore[tbl].lastInsertId += data.length;
                if (insertData) {
                    _privateStore[tbl + ":data"].push.apply(_privateStore[tbl + ":data"], data);
                }

                _sqlFacade.insert(tbl, data);
            })
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'update'), function(tbl, data) {
                _sqlFacade.update(tbl, data)
                    .then(function() {}, txError);
            })
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'delete'), function(tbl, delItem) {
                /**
                 * remove the data from memory
                 */
                _sqlFacade.delete(tbl, delItem, " WHERE _ref=?", '_ref')
                    .then(function() {}, txError);
            })
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'onAlterTable'), function(tableName, columnName, action) {
                var columnData = "";
                _sqlFacade.alterTable.apply(_sqlFacade, arguments)
                    .then(function() {
                        if (action) {
                            var tblData = _privateStore[tableName + ":data"];
                            if (tblData.length) {
                                var columnData = tblData[0]._data[columnName];
                            }
                        }

                        updateTable(columnData);
                    }, handleAlterError);
                /**
                 * update a table when these actions are performed DROP | RENAME | ADD
                 */
                function updateTable() {
                    _sqlFacade.query('update ' + tableName + ' set ' + columnName + '=?', [columnData])
                }

                /**
                 * Below method helps to tackle issues with Sqlite lower version without support for 
                 * RENAME and DROP COLUMN
                 */
                function handleAlterError() {
                    // failure should be either for DROP and RENAME
                    if (typeof columnName === 'object') {
                        // rename mode
                        _sqlFacade.alterTable(tableName, columnName[1], 1);
                    } else {
                        // drop
                        if (!action) {
                            updateTable();
                        }
                    }
                }
            })
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'onCreateTable'), createTable)
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'onDropTable'), function(tbl) {
                _sqlFacade.dropTable(tbl)
                    .then(function() {
                        _storageFacade.removeItem(tbl);
                        _storageFacade.removeItem(tbl + ":data");
                    });
            })
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'onUpdateTable'), function(tbl, updates) {
                Object.keys(updates)
                    .forEach(function(key) {
                        _privateStore[tbl][key] = updates[key];
                    });
                // set the property to db
                _storageFacade.setItem(tbl, _privateStore[tbl]);
            })
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'onTruncateTable'), _sqlFacade.delete)
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'onResolveSchema'), function(version, tables) {
                _storageFacade.setItem('version', version);
                Object.keys(tables).forEach(function(tblName) {
                    createTable(tblName, tables[tblName]);
                });
            })
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'onRenameTable'), function(oldTable, newTable) {
                // rename cache first
                _privateStore[newTable] = _privateStore[oldTable];
                _privateStore[newTable].TBL_NAME = newTable;
                _privateStore[newTable + ":data"] = _privateStore[oldTable + ":data"];
                delete _privateStore[oldTable + ":data"];
                delete _privateStore[oldTable];

                _sqlFacade.query('update _JELI_STORE_ set _rev=? where _rev=?', [newTable, oldTable]);
                _sqlFacade.query('ALTER TABLE ' + oldTable + ' RENAME TO ' + newTable, []);
            })
            .subscribe(dbToStorageInstance.eventNamingIndex(dbName, 'onRenameDataBase'), function(oldName, newName, cb) {
                var newData = deepClone(_privateStore[oldName], true),
                    tablesData = {},
                    tbls = ['_JELI_STORE_'],
                    _self = this,
                    tblInstance = Object.keys(newData.tables);
                /**
                 * loop through tables
                 * store each table Data
                 */
                tblInstance.forEach(function(tblName) {
                    newData.tables[tblName].DB_NAME = newName;
                    tablesData[tblName] = newData.tables[tblName].data.slice();
                    newData.tables[tblName].data = [];
                    newData.tables[tblName].lastModified = +new Date
                    tbls.push(tblName);
                });
                var bkInstance = useDB(createDB(newName, extend(config, { name: newName })));
                dbToStorageInstance.getInstance(oldName).$get('recordResolvers').rename(newName);
                _sqlFacade.dropTables(tbls);
                /**
                 * create our store
                 */
                bkInstance.createTable('_JELI_STORE_', ["_rev unique", "_data"])
                    .then(function() {
                        /**
                         * insert into our store
                         */
                        bkInstance.insert('_JELI_STORE_', [{
                                _rev: newName,
                                _data: newData
                            }, {
                                _rev: dbToStorageInstance.storeMapping.resourceName,
                                _data: _self.getItem(dbToStorageInstance.storeMapping.resourceName)
                            }, {
                                _rev: dbToStorageInstance.storeMapping.pendingSync,
                                _data: _self.getItem(dbToStorageInstance.storeMapping.pendingSync)
                            }])
                            .then(function() {
                                tblInstance.each(createAndInsert);
                                (cb || noop)();
                                _self.clear();
                            });
                    });

                function createAndInsert(tblName) {
                    bkInstance.createTable(tblName, ['_ref unique', '_data'])
                        .then(function() {
                            bkInstance.insert(tblName, tablesData[tblName])
                            delete tablesData[tblName];
                        });
                }
            });

        // create our store table
        _sqlFacade
            .query('CREATE TABLE IF NOT EXISTS _JELI_STORE_ (_rev unique, _data)', [])
            .then(loadAllData, function() {
                throw new Error(type + " failed to initialize our store");
            });

        return _storageFacade;
    }

    /**
     * 
     * @param {*} generateStruct 
     */
    function StorageFacade(generateStruct) {
        /**
         * 
         * @param {*} name 
         */
        this.usage = function(name) {
            return JSON.stringify(this.getItem(name) || '').length;
        };

        /**
         * 
         * @param {*} name 
         */
        this.getItem = function(name) {
            if (!name) {
                return generateStruct(_privateStore);
            }

            return _privateStore[name];
        };

        /**
         * 
         * @param {*} name 
         * @param {*} item 
         */
        this.setItem = function(name, item) {
            _privateStore[name] = item;
            _sqlFacade.query('INSERT OR REPLACE INTO _JELI_STORE_ (_rev, _data) VALUES (?,?)', [name, JSON.stringify(item)])
                .then(function() {});
        };
    };

    StorageFacade.prototype.removeItem = function(name) {
        _sqlFacade.query('DELETE FROM _JELI_STORE_ WHERE _rev=?', [name])
            .then(function() {
                delete _privateStore[name];
            });

        return true;
    };

    StorageFacade.prototype.clear = function() {
        _sqlFacade.query('DELETE FROM _JELI_STORE_', [])
            .then(function() {
                _privateStore = {};
            });
    };

    StorageFacade.prototype.isExists = function(key) {
        return _privateStore.hasOwnProperty(key);
    };

    /**
     * 
     * @param {*} sqlInstance 
     */
    function CoreSqlFacade(sqlInstance) {

        if (!sqlInstance) {
            throw new TypeError('No Plugin Support for ' + type);
        }

        /**
         * 
         * @param {*} data 
         * @param {*} setCol 
         * @param {*} addRef 
         */
        function _setData(data, setCol, addRef) {
            var col = [],
                val = [],
                keys = [];
            if (addRef) {
                col.push('?');
                val.push(data._ref);
                keys.push('_ref');
                data = data._data;
            }

            for (var i in data) {
                col.push((setCol ? i + "=" : "") + "?");
                keys.push(i);
                if (typeof data[i] === "object") {
                    val.push(JSON.stringify(data[i]));
                } else {
                    val.push(data[i]);
                }
            }

            return ({
                col: col,
                val: val,
                keys: keys
            });
        }

        /**
         * 
         * @param {*} total 
         */
        function promiseHandler(total) {
            var succ = 0,
                err = 0,
                total = total || 0,
                sucCB = function() {},
                errCB = function() {};
            this.success = function() {
                succ++;
                finalize.apply(null, arguments);
            };

            this.error = function() {
                err++;
                finalize.apply(null, arguments);
            };

            var finalize = function() {
                if (err == total) {
                    errCB.apply(null, arguments);
                } else if (succ === total) {
                    sucCB.apply(null, arguments);
                } else if ((succ + err) == total) {
                    sucCB({
                        success: succ,
                        failed: err
                    });
                }
            };

            this.setTotal = function(val) {
                total = val;
            };

            /**
             * @param succ
             * @param err
             */
            this.then = function(succ, err) {
                sucCB = succ || sucCB;
                errCB = err || errCB;
                /**
                 * trigger finalize
                 */
                finalize();
            };
        }


        this.createTable = function(tableName, columns) {
            var qPromise = new promiseHandler(1);
            sqlInstance.transaction(function(transaction) {
                transaction.executeSql('CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + columns.join(',') + ')', [], qPromise.success, qPromise.error);
            });

            return qPromise;
        };

        this.insert = function(table, data) {
            if (!table || !isArray(data)) {
                errorBuilder('ERROR[SQL] : Table and data is required');
            }
            var qPromise = new promiseHandler(data.length);

            function run(item, tx) {
                var _cData = _setData(item, false, true),
                    executeQuery = "INSERT OR REPLACE INTO " + table + " (" + _cData.keys.join(',') + ") VALUES (" + _cData.col.join(',') + ")";
                tx.executeSql(executeQuery, _cData.val, qPromise.success, qPromise.error);
            }



            sqlInstance.transaction(function(transaction) {
                data.forEach(function(item) {
                    run(item, transaction);
                });
            });

            return qPromise;
        };

        this.select = function(executeQuery, data) {
            if (!executeQuery) {
                throw new Error('ERROR[SQL] : Table is required');
            }

            var qPromise = new promiseHandler(1);
            sqlInstance.transaction(function(transaction) {
                transaction.executeSql(executeQuery, data || [], qPromise.success, qPromise.error);
            });

            return qPromise;
        };

        this.delete = function(table, data, where, ex) {
            if (!table) {
                throw new Error('ERROR[SQL] : Table and data is required');
            }

            var qPromise = new promiseHandler(data ? data.length : 1);
            /**
             * 
             * @param {*} item 
             * @param {*} tx 
             */
            function run(item, tx) {
                executeQuery = "DELETE FROM " + table;
                if (where) {
                    executeQuery += " " + where;
                    ex = [item[ex] || item];
                }

                tx.executeSql(executeQuery, ex || [], qPromise.success, qPromise.error);
            }

            sqlInstance.transaction(function(transaction) {
                if (data) {
                    data.forEach(function(item) {
                        run(item, transaction);
                    });
                } else {
                    run({}, transaction);
                }
            });

            return qPromise;
        };

        this.update = function(table, data) {
            if (!table || !isArray(data)) {
                throw new Error('ERROR[SQL] : Table and data is required');
            }

            var qPromise = new promiseHandler(data.length);

            function run(item, tx) {
                var _cData = _setData(item._data, true);
                var executeQuery = "UPDATE " + table + " SET " + _cData.col.join(',');
                executeQuery += "  WHERE _ref='" + item._ref + "'";
                tx.executeSql(executeQuery, _cData.val, qPromise.success, qPromise.error);
            }

            sqlInstance.transaction(function(transaction) {
                data.forEach(function(item) {
                    run(item, transaction);
                });
            });

            return qPromise;
        };

        this.dropTable = function(table) {
            if (!table) {
                throw new Error('ERROR[SQL] : Table is required');
            }

            var qPromise = new promiseHandler(1),
                executeQuery = "DROP TABLE IF EXISTS " + table;

            sqlInstance.transaction(function(transaction) {
                transaction.executeSql(executeQuery, [], qPromise.success, qPromise.error);
            });

            return qPromise;
        };

        this.alterTable = function(tbl, columnName, addRemoved) {
            var qPromise = new promiseHandler(1),
                executeQuery = "ALTER TABLE " + tbl;

            if (typeof columnName === "object") {
                // RENAME process
                executeQuery += 'RENAME COLUMN ' + columnName[0] + ' TO ' + columnName[1];
            } else {
                executeQuery += (addRemoved ? " ADD " : " DROP ") + columnName;
            }

            sqlInstance.transaction(function(transaction) {
                transaction.executeSql(executeQuery, [], qPromise.success, qPromise.error);
            });

            return qPromise;
        }

        this.dropTables = function(tables) {
            if (!isArray(tables)) {
                throw new Error('ERROR[SQL] : expected ArrayList<tbl>');
            }

            var qPromise = new promiseHandler(tables.length);

            function run(tbl, tx) {
                executeQuery = "DROP TABLE  IF EXISTS " + tbl;
                tx.executeSql(executeQuery, [], qPromise.success, qPromise.error);
            }

            sqlInstance.transaction(function(transaction) {
                tables.forEach(function(tbl) {
                    run(tbl, transaction);
                });
            });

            return qPromise;
        };

        this.query = function(query, data) {
            var qPromise = new promiseHandler(1);
            sqlInstance.transaction(function(tx) {
                tx.executeSql(query, data, qPromise.success, qPromise.error);
            });

            return qPromise;
        };
    };


    function txError(tx, txError) {
        console.group('JDB SQL_ADAPTER');
        console.log(txError);
        console.groupEnd();
    }


    /**
     * register the storage
     */
    return dbInit;
}));