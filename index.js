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

    var dbName, _privateStore = {},
        _errorTables = [],
        _dbApi, type;
    /**
     * 
     * @param {*} config 
     * @param {*} CB 
     */
    function dbInit(config, CB) {
        dbName = config.name;
        _dbApi = useDB(createDB(config));

        function loadAllData() {
            _dbApi.query('SELECT * FROM _JELI_STORE_', [])
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
            var tableNames = Object.keys(_privateStore[dbInit.privateApi.storeMapping.resourceName].resourceManager);

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
                        console.log('preparing chunking of query result');
                        for (var i = 0; i <= TOTAL_RECORDS; i += MAXIMUM_RESULT) {
                            chunkQueries.push([i, MAXIMUM_RESULT])
                        }
                        startChunkQuery();
                    } else {
                        _dbApi.select('select * from ' + current)
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
                        });
                        Object.keys(_publicMethods.getItem(current).columns[0])
                            .forEach(function(column) {
                                var value = results.rows.item(i)[column];
                                try {
                                    data._data[column] = JSON.parse(value);
                                } catch (e) {
                                    data._data[column] = value;
                                }
                            });

                        _privateStore[current + ":data"].push(data);
                    }

                    nextQuery();
                }

                function startChunkQuery() {
                    var query = chunkQueries.shift()
                    _dbApi.select('select * from ' + current + ' limit ?,?', query)
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
                    console.log('failed to load:', current);
                    _errorTables.push(current);
                    nextQuery();
                }
            }
        }

        var _publicMethods = new publicApis();

        dbInit.privateApi.storageEventHandler
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'insert'), function(tbl, data, insertData) {
                _privateStore[tbl].lastInsertId += data.length;
                if (insertData) {
                    _privateStore[tbl + ":data"].push.apply(_privateStore[tbl + ":data"], data);
                }

                _dbApi.insert(tbl, data);
            })
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'update'), function(tbl, data) {
                _dbApi.update(tbl, data)
                    .then(function() {}, txError);
            })
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'delete'), function(tbl, delItem) {
                /**
                 * remove the data from memory
                 */
                _dbApi.delete(tbl, delItem, " WHERE _ref=?", '_ref')
                    .then(function() {}, txError);
            })
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'onAlterTable'), function(tableName, columnName, action) {
                _dbApi.alterTable.apply(_dbApi, arguments)
                    .then(function() {
                        if (action) {
                            var tblData = _privateStore[tableName + ":data"];
                            if (tblData.length) {
                                var columnData = tblData[0]._data[columnName];
                                if (columnData) {
                                    // update the table with the columnData
                                    _dbApi.query('update ' + tableName + ' set ' + columnName + '=?', [columnData]);
                                }
                            }
                        }
                    });
            })
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'onCreateTable'), createTable)
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'onDropTable'), function(tbl) {
                _dbApi.dropTable(tbl)
                    .then(function() {
                        _publicMethods.removeItem(tbl);
                        _publicMethods.removeItem(tbl + ":data");
                    });
            })
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'onUpdateTable'), function(tbl, updates) {
                Object.keys(updates)
                    .forEach(function(key) {
                        _privateStore[tbl][key] = updates[key];
                    });
                // set the property to db
                _publicMethods.setItem(tbl, _privateStore[tbl]);
            })
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'onTruncateTable'), _dbApi.delete)
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'onResolveSchema'), function(version, tables) {
                _publicMethods.setItem('version', version);
                Object.keys(tables).forEach(function(tblName) {
                    createTable(tblName, tables[tblName]);
                });
            })
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'onRenameTable'), function(oldTable, newTable) {
                // rename cache first
                _privateStore[newTable] = _privateStore[oldTable];
                _privateStore[newTable].TBL_NAME = newTable;
                _privateStore[newTable + ":data"] = _privateStore[oldTable + ":data"];
                delete _privateStore[oldTable + ":data"];
                delete _privateStore[oldTable];

                _dbApi.query('update _JELI_STORE_ set _rev=? where _rev=?', [newTable, oldTable]);
                _dbApi.query('ALTER TABLE ' + oldTable + ' RENAME TO ' + newTable, []);
            })
            .subscribe(dbInit.privateApi.eventNamingIndex(dbName, 'onRenameDataBase'), function(oldName, newName, cb) {
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
                dbInit.privateApi.$getActiveDB(oldName).$get('recordResolvers').rename(newName);
                _dbApi.dropTables(tbls);
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
                                _rev: dbInit.privateApi.storeMapping.resourceName,
                                _data: _self.getItem(dbInit.privateApi.storeMapping.resourceName)
                            }, {
                                _rev: dbInit.privateApi.storeMapping.pendingSync,
                                _data: _self.getItem(dbInit.privateApi.storeMapping.pendingSync)
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
        _dbApi.query('CREATE TABLE IF NOT EXISTS _JELI_STORE_ (_rev unique, _data)', [])
            .then(loadAllData, function() {
                throw new Error(type + " catched to initialize our store");
            });

        function createTable(tbl, definition) {
            var columns = ['_ref unique'];
            if (definition.columns[0]) {
                columns = columns.concat(Object.keys(definition.columns[0]));
            }
            _dbApi.createTable(tbl, columns);
            _publicMethods.setItem(tbl, definition);
            _privateStore[tbl + ":data"] = [];
        }

        return _publicMethods;
    }

    function publicApis() {
        function deepClone(data) {
            return JSON.parse(JSON.stringify(data));
        }
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
                return dbInit.privateApi.generateStruct(_privateStore);
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
            _dbApi.query('INSERT OR REPLACE INTO _JELI_STORE_ (_rev, _data) VALUES (?,?)', [name, JSON.stringify(item)])
                .then(function() {});
        };
    };

    publicApis.prototype.removeItem = function(name) {
        _dbApi.query('DELETE FROM _JELI_STORE_ WHERE _rev=?', [name])
            .then(function() {
                delete _privateStore[name];
            });

        return true;
    };



    publicApis.prototype.clear = function() {
        _dbApi.query('DELETE FROM _JELI_STORE_', [])
            .then(function() {
                _privateStore = {};
            });
    };

    publicApis.prototype.isExists = function(key) {
        return _privateStore.hasOwnProperty(key);
    };

    /**
     * 
     * @param {*} $dbName 
     * @param {*} options 
     */
    function createDB(options) {
        if (window.sqlitePlugin) {
            return window.sqlitePlugin.openDatabase(options);
        } else if (window.openDatabase) {
            return window.openDatabase(options.name, '1.0', options.name + ' Storage for webSql', 50 * 1024 * 1024)
        }

        return null;
    }

    function isArray(data) {
        return toString.call(data) === "[object Array]";
    }

    /**
     * 
     * @param {*} sqlInstance 
     */
    function useDB(sqlInstance) {

        if (!sqlInstance) {
            throw new TypeError('No Plugin Support for ' + type);
        }

        var _pub = {};

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
         * @param {*} $promise 
         */
        function promiseHandler() {
            var succ = 0,
                err = 0,
                total = 0,
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

            this.then = function(succ, err) {
                sucCB = succ || sucCB;
                errCB = err || errCB;
            };
        }


        _pub.createTable = function(tableName, columns) {
            var qPromise = new promiseHandler();
            sqlInstance.transaction(function(transaction) {
                qPromise.setTotal(1);
                transaction.executeSql('CREATE TABLE IF NOT EXISTS ' + tableName + ' (' + columns.join(',') + ')', [], qPromise.success, qPromise.error);
            });

            return qPromise;
        };

        _pub.insert = function(table, data) {
            if (!table || !isArray(data)) {
                errorBuilder('ERROR[SQL] : Table and data is required');
            }
            var qPromise = new promiseHandler();

            function run(item, tx) {
                var _cData = _setData(item, false, true),
                    executeQuery = "INSERT OR REPLACE INTO " + table + " (" + _cData.keys.join(',') + ") VALUES (" + _cData.col.join(',') + ")";
                tx.executeSql(executeQuery, _cData.val, qPromise.success, qPromise.error);
            }



            sqlInstance.transaction(function(transaction) {
                qPromise.setTotal(data.length);
                data.forEach(function(item) {
                    run(item, transaction)
                });
            });

            return qPromise;
        };

        _pub.select = function(executeQuery, data) {
            if (!executeQuery) {
                throw new Error('ERROR[SQL] : Table is required');
            }

            var qPromise = new promiseHandler();
            sqlInstance.transaction(function(transaction) {
                qPromise.setTotal(1);
                transaction.executeSql(executeQuery, data || [], qPromise.success, qPromise.error);
            });

            return qPromise;
        };

        _pub.delete = function(table, data, where, ex) {
            if (!table) {
                throw new Error('ERROR[SQL] : Table and data is required');
            }

            var qPromise = new promiseHandler();

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
                    qPromise.setTotal(data.length);
                    data.forEach(function(item) {
                        run(item, transaction)
                    });
                } else {
                    run({}, transaction);
                }
            });

            return qPromise;
        };

        _pub.update = function(table, data) {
            if (!table || !isArray(data)) {
                throw new Error('ERROR[SQL] : Table and data is required');
            }

            var qPromise = new promiseHandler();

            function run(item, tx) {
                var _cData = _setData(item._data, true);
                var executeQuery = "UPDATE " + table + " SET " + _cData.col.join(',');
                executeQuery += "  WHERE _ref='" + item._ref + "'";
                tx.executeSql(executeQuery, _cData.val, qPromise.success, qPromise.error);
            }

            sqlInstance.transaction(function(transaction) {
                qPromise.setTotal(data.length);
                data.forEach(function(item) {
                    run(item, transaction)
                });
            });

            return qPromise;
        };

        _pub.dropTable = function(table) {
            if (!table) {
                throw new Error('ERROR[SQL] : Table is required');
            }

            var qPromise = new promiseHandler(),
                executeQuery = "DROP TABLE IF EXISTS " + table;

            sqlInstance.transaction(function(transaction) {
                qPromise.setTotal(1);
                transaction.executeSql(executeQuery, [], qPromise.success, qPromise.error);
            });

            return qPromise;
        };

        _pub.alterTable = function(tbl, columnName, addRemoved) {
            var qPromise = new promiseHandler(),
                executeQuery = "ALTER TABLE " + tbl + (addRemoved ? " ADD " : " DROP ") + columnName;

            sqlInstance.transaction(function(transaction) {
                qPromise.setTotal(1);
                transaction.executeSql(executeQuery, [], qPromise.success, qPromise.error);
            });

            return qPromise;
        }

        _pub.dropTables = function(tables) {
            if (!isArray(tables)) {
                throw new Error('ERROR[SQL] : expected ArrayList<tbl>');
            }

            var qPromise = new promiseHandler();

            function run(tbl, tx) {
                executeQuery = "DROP TABLE  IF EXISTS " + tbl;
                tx.executeSql(executeQuery, [], qPromise.success, qPromise.error);
            }

            sqlInstance.transaction(function(transaction) {
                qPromise.setTotal(tables.length);
                tables.forEach(function(tbl) {
                    run(tbl, transaction);
                });
            });

            return qPromise;
        };

        _pub.query = function(query, data) {
            var qPromise = new promiseHandler();
            sqlInstance.transaction(function(tx) {
                qPromise.setTotal(1);
                tx.executeSql(query, data, qPromise.success, qPromise.error);
            });

            return qPromise;
        };

        return _pub;
    };


    function txError(tx, txError) {
        console.log(txError);
    }


    /**
     * register the storage
     */
    return dbInit;
}));