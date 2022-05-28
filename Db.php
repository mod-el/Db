<?php namespace Model\Db;

use Model\Core\Module;
use Model\DbParser\Parser;
use Model\DbParser\Table;

class Db extends Module
{
	protected Parser $parser;
	protected ?\PDO $db = null;
	/** @var Table[] */
	protected array $tables = []; // TODO: rimuovere quando le custom table e le multilang table saranno gestite dalle nuove librerie

	public string $name;
	public string $unique_id;

	public int $n_query = 0;
	public int $n_prepared = 0;
	public array $n_tables = [];

	protected int $c_transactions = 0;

	protected array $querylimit_counter = [
		'query' => [],
		'table' => [],
	];

	public array $options = [
		'db' => 'primary',
		'listCache' => [], // Deprecated
		'cache-tables' => [],
		'linked-tables' => [],
		'autoHide' => [],
		'direct-pdo' => false,
		'query-limit' => 100,
		'query-limit-table' => 10000,
		'debug' => false,
		'use_buffered_query' => true,
		'emulate_prepares' => false,
		'local_infile' => true,
	];

	protected array $tablesToCache = [];
	protected array $cachedTables = [];
	protected array $queryCache = [];

	protected array $deferedInserts = [];

	/**
	 * @param array $options
	 */
	public function init(array $options)
	{
		if ($this->module_id !== 0)
			$this->options['db'] = $this->module_id;

		$this->options = array_merge($this->options, $options);

		if ($this->options['direct-pdo']) {
			$this->unique_id = 'custom';
		} else {
			$config = $this->retrieveConfig();
			if (!$config or !isset($config['databases'][$this->options['db']]))
				throw new \Exception('Missing database configuration for ' . $this->options['db'] . ' database!');

			$configOptions = $config['databases'][$this->options['db']];

			if (isset($configOptions['listCache']))
				$this->options['listCache'] = array_unique(array_merge($configOptions['listCache'], $this->options['listCache']));
			if (isset($configOptions['cache-tables']))
				$this->options['cache-tables'] = array_unique(array_merge($configOptions['cache-tables'], $this->options['cache-tables']));
			$this->options['cache-tables'] = array_unique(array_merge($this->options['listCache'], $this->options['cache-tables']));

			if (isset($configOptions['autoHide']))
				$this->options['autoHide'] = array_unique(array_merge($configOptions['autoHide'], $this->options['autoHide']));

			if (isset($configOptions['linked-tables']))
				$this->options['linked-tables'] = array_unique(array_merge($configOptions['linked-tables'], $this->options['linked-tables']));

			$this->options = array_merge($configOptions, $this->options);

			$this->name = $this->options['database'];
			$this->unique_id = preg_replace('/[^A-Za-z0-9._-]/', '', $this->options['host'] . '-' . $this->options['database']);

			$linkedTables = [];
			foreach ($this->options['linked-tables'] as $k => $v) {
				if (is_numeric($k)) {
					$linkedTables[$v] = [
						'with' => $v . '_custom',
					];
				} else {
					if (!is_array($v))
						$v = ['with' => $v];

					$linkedTables[$k] = array_merge([
						'with' => $k . '_custom',
					], $v);
				}
			}
			$this->options['linked-tables'] = $linkedTables;
			foreach ($this->options['linked-tables'] as $table => $tableOptions)
				$this->checkLinkedTableMultilang($table);

			$this->tablesToCache = $this->options['cache-tables'];

			if (!isset($this->options['user-filter']))
				$this->options['user-filter'] = null;
			if ($this->options['user-filter'] and (!is_array($this->options['user-filter']) or count($this->options['user-filter']) < 2 or !isset($this->options['user-filter']['idx'], $this->options['user-filter']['column'])))
				$this->options['user-filter'] = null;
		}
	}

	private function initDb()
	{
		if ($this->db !== null)
			return;

		try {
			if ($this->options['direct-pdo']) {
				$this->db = $this->options['direct-pdo'];
			} else {
				$this->db = new \PDO('mysql:host=' . $this->options['host'] . ';dbname=' . $this->options['database'] . ';charset=utf8', $this->options['username'], $this->options['password'], [
					\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
					\PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
					\PDO::ATTR_EMULATE_PREPARES => $this->options['emulate_prepares'],
					\PDO::ATTR_STRINGIFY_FETCHES => false,
					\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => $this->options['use_buffered_query'],
					\PDO::MYSQL_ATTR_LOCAL_INFILE => $this->options['local_infile'],
				]);
			}
		} catch (\Exception $e) {
			$this->model->error('Error while connecting to database: ' . $e->getMessage());
		}
	}

	/**
	 * @param string $name
	 * @param array $arguments
	 * @return mixed
	 */
	function __call(string $name, array $arguments): mixed
	{
		$this->initDb();

		if (method_exists($this->db, $name))
			return call_user_func_array([$this->db, $name], $arguments);

		return null;
	}

	/**
	 * @param string $qry
	 * @param int $type
	 * @return string|false
	 */
	public function quote(string $qry, int $type = \PDO::PARAM_STR): string|false
	{
		$this->initDb();
		return $this->db->quote($qry, $type);
	}

	/**
	 * @param string $qry
	 * @param string|null $table
	 * @param string|null $type
	 * @param array $options
	 * @return \PDOStatement|int
	 */
	public function query(string $qry, string $table = null, string $type = null, array $options = []): \PDOStatement|int
	{
		$options = array_merge([
			'log' => true,
			'query-limit' => true,
		], $options);

		if ($options['query-limit'] and $this->options['query-limit'] > 0) {
			if (!isset($this->querylimit_counter['query'][$qry]))
				$this->querylimit_counter['query'][$qry] = 0;
			$this->querylimit_counter['query'][$qry]++;

			if ($this->querylimit_counter['query'][$qry] > $this->options['query-limit'])
				$this->model->error('Query limit exceeded. - ' . $qry);
		}

		if ($options['query-limit'] and $this->options['query-limit-table'] > 0 and $table !== null) {
			if (!isset($this->querylimit_counter['table'][$table]))
				$this->querylimit_counter['table'][$table] = 0;
			$this->querylimit_counter['table'][$table]++;

			if ($this->querylimit_counter['table'][$table] > $this->options['query-limit-table'])
				$this->model->error('Query limit per table ' . $table . ' exceeded. - ' . $qry);
		}

		if ($options['log']) {
			$this->trigger('query', [
				'table' => $table,
				'type' => $type,
				'options' => $options,
				'qry' => $qry,
			]);
		}

		$rowId = null;

		$this->initDb();

		$this->n_query++;
		$res = $this->db->query($qry);
		$return = $res;
		if ($res and $type === 'INSERT') {
			$return = $this->db->lastInsertId();
			$row_id = $return;
		} else {
			$row_id = null;
		}

		if ($options['log']) {
			$this->trigger('queryExecuted', [
				'id' => $row_id,
				'rows' => $res->rowCount(),
			]);
		}

		return $return;
	}

	/**
	 * @param string $qry
	 * @param array $options
	 * @return \PDOStatement
	 */
	public function prepare(string $qry, array $options = []): \PDOStatement
	{
		$this->initDb();

		$this->n_prepared++;
		return $this->db->prepare($qry, $options);
	}

	/**
	 * @return bool
	 */
	public function beginTransaction(): bool
	{
		$this->initDb();

		$res = $this->c_transactions == 0 ? $this->db->beginTransaction() : true;
		if ($res)
			$this->c_transactions++;
		return $res;
	}

	/**
	 * @return bool
	 */
	public function commit(): bool
	{
		if ($this->c_transactions <= 0)
			return false;

		$this->initDb();

		$this->c_transactions--;
		if ($this->c_transactions == 0 and $this->db->inTransaction())
			return $this->db->commit();
		else
			return true;
	}

	/**
	 * @return bool
	 */
	public function rollBack(): bool
	{
		if ($this->c_transactions > 0) {
			$this->initDb();

			$this->c_transactions = 0;
			return $this->db->inTransaction() ? $this->db->rollBack() : false;
		}
		$this->c_transactions = 0;
		return false;
	}

	/**
	 * @param int $ignore
	 * @return bool
	 */
	public function inTransaction(int $ignore = 0): bool
	{
		return ($this->c_transactions - $ignore) > 0 ? true : false;
	}

	public function terminate()
	{
		foreach ($this->deferedInserts as $table => $options) {
			if (count($options['rows']) > 0)
				$this->bulkInsert($table);
		}
		if ($this->c_transactions > 0)
			$this->rollBack();
	}

	/* CRUD methods */

	/**
	 * @param string $table
	 * @param array $data
	 * @param array $options
	 * @return int|null
	 */
	public function insert(string $table, array $data = [], array $options = []): ?int
	{
		$options = array_merge([
			'replace' => false,
			'defer' => null,
			'debug' => $this->options['debug'],
			'skip-user-filter' => false,
		], $options);

		$this->trigger('insert', [
			'table' => $table,
			'data' => $data,
			'options' => $options,
		]);

		$tableModel = $this->getTable($table);
		$this->addUserFilter($data, $tableModel, $options);
		$data = $this->filterColumns($table, $data);
		$this->checkDbData($table, $data['data'], $options);
		if ($data['multilang']) {
			$multilangTable = $this->model->_Multilang->getTableFor($table);
			$multilangOptions = $this->model->_Multilang->getTableOptionsFor($table);
			foreach ($data['multilang'] as $lang => $multilangData)
				$this->checkDbData($multilangTable, $multilangData, $options);
		}

		if ($options['defer'] !== null) {
			if (array_key_exists($table, $this->options['linked-tables']))
				$this->model->error('Cannot defer linked tables');

			if ($data['multilang'])
				$this->model->error('Cannot defer inserts with multilang fields');

			if ($options['defer'] === true)
				$options['defer'] = 0;
			if (!is_numeric($options['defer']))
				$this->model->error('Invalid defer value');
			$options['defer'] = (int)$options['defer'];

			if (!isset($this->deferedInserts[$table])) {
				$this->deferedInserts[$table] = [
					'options' => $options,
					'rows' => [],
				];
			}

			if ($this->deferedInserts[$table]['options'] !== $options)
				$this->model->error('Cannot defer inserts with different options on the same table');

			$this->deferedInserts[$table]['rows'][] = $data['data'];
			if ($options['defer'] > 0 and count($this->deferedInserts[$table]['rows']) === $options['defer'])
				$this->bulkInsert($table);

			return null;
		}

		try {
			$this->beginTransaction();

			$qry = $this->makeQueryForInsert($table, [$data['data']], $options);
			if (!$qry)
				$this->model->error('Error while generating query for insert');

			if ($options['debug'] and DEBUG_MODE)
				echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

			$id = $this->query($qry, $table, 'INSERT', $options);

			$mlRowsMap = [];
			foreach ($data['multilang'] as $lang => $multilangData) {
				$multilangData[$multilangOptions['keyfield']] = $id;
				$multilangData[$multilangOptions['lang']] = $lang;

				$qry = $this->makeQueryForInsert($multilangTable, [$multilangData], $options);
				if (!$qry)
					$this->model->error('Error while generating query for multilang insert');

				if ($options['debug'] and DEBUG_MODE)
					echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

				$mlRowsMap[$lang] = $this->query($qry, $multilangTable, 'INSERT', $options);
			}

			if (isset($this->cachedTables[$table])) {
				$dataForCache = array_merge($data['data'], count($data['multilang']) > 0 ? reset($data['multilang']) : []);
				$dataForCache[$tableModel->primary[0]] = $id;
			}

			if (array_key_exists($table, $this->options['linked-tables'])) {
				$linked_table = $this->options['linked-tables'][$table]['with'];
				$linkedTableModel = $this->getTable($linked_table);
				$data['custom-data'][$linkedTableModel->primary[0]] = $id;

				$qry = $this->makeQueryForInsert($linked_table, [$data['custom-data']], $options);
				if (!$qry)
					$this->model->error('Error while generating query for custom table insert');

				if ($options['debug'] and DEBUG_MODE)
					echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

				$id = $this->query($qry, $linked_table, 'INSERT', $options);

				if ($data['custom-multilang']) {
					$multilangTable = $linked_table . $this->model->_Multilang->tables[$table]['suffix'];
					$multilangTableModel = $this->getTable($multilangTable);
					foreach ($data['custom-multilang'] as $multilangData)
						$this->checkDbData($multilangTable, $multilangData, $options);

					foreach ($data['custom-multilang'] as $lang => $multilangData) {
						if (!isset($mlRowsMap[$lang]))
							throw new \Exception('Errore nell\'inserimento custom multilang, rows map id non trovato');

						$multilangData[$multilangTableModel->primary[0]] = $mlRowsMap[$lang];
						$qry = $this->makeQueryForInsert($multilangTable, [$multilangData], $options);
						if (!$qry)
							$this->model->error('Error while generating query for multilang insert');

						if ($options['debug'] and DEBUG_MODE)
							echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

						$this->query($qry, $multilangTable, 'INSERT', $options);
					}
				}

				if (isset($this->cachedTables[$table]))
					$dataForCache = array_merge($dataForCache, $data['custom-data'], count($data['custom-multilang']) > 0 ? reset($data['custom-multilang']) : []);
			}

			$this->trigger('inserted', [
				'table' => $table,
				'id' => $id,
			]);

			if (isset($this->cachedTables[$table]))
				$this->insert_cache($table, $dataForCache);

			$this->changedTable($table);

			$this->commit();

			return $id;
		} catch (\Exception $e) {
			$this->rollBack();
			$this->model->error('Error while inserting.', '<b>Error:</b> ' . getErr($e) . '<br /><b>Query:</b> ' . ($qry ?? 'Still undefined'));
		}
	}

	/**
	 * @param string $table
	 */
	public function bulkInsert(string $table)
	{
		if (!isset($this->deferedInserts[$table]))
			return;

		try {
			$this->beginTransaction();

			$options = $this->deferedInserts[$table]['options'];

			$qry = $this->makeQueryForInsert($table, $this->deferedInserts[$table]['rows'], $options);
			if ($qry) {
				$this->trigger('bulk-insert', [
					'table' => $table,
					'options' => $options,
				]);

				$this->query($qry, $table, 'INSERT', $options);

				$this->flushTableCache($table);
				$this->changedTable($table);
			} else {
				$this->trigger('empty-bulk-insert', [
					'table' => $table,
					'options' => $options,
				]);
			}

			unset($this->deferedInserts[$table]);

			$this->commit();
		} catch (\Exception $e) {
			$this->rollBack();
			$this->model->error('Error while bulk inserting.', ['mex' => '<b>Error:</b> ' . getErr($e), 'details' => '<b>Query:</b> ' . ($qry ?? 'Still undefined')]);
		}
	}

	/**
	 * Builds a query for the insert method
	 *
	 * @param string $table
	 * @param array $rows
	 * @param array $options
	 * @return string|null
	 */
	public function makeQueryForInsert(string $table, array $rows, array $options = []): ?string
	{
		$options = array_merge([
			'replace' => false,
		], $options);

		$qry_init = $options['replace'] ? 'REPLACE' : 'INSERT';

		$keys = [];
		$keys_set = false;
		$defaults = null;
		$qry_rows = [];

		foreach ($rows as $data) {
			if ($data === []) {
				if ($defaults === null) {
					$tableModel = $this->getTable($table);

					foreach ($tableModel->columns as $c) {
						if ($c['null']) {
							$defaults[] = 'NULL';
						} else {
							if ($c['key'] == 'PRI')
								$defaults[] = 'NULL';
							else
								$defaults[] = '\'\'';
						}
					}
				}
				$qry_rows[] = '(' . implode(',', $defaults) . ')';
			} else {
				$values = [];
				foreach ($data as $k => $v) {
					if (!$keys_set)
						$keys[] = $this->elaborateField($k);

					$values[] = $this->parseValue($v);
				}
				$keys_set = true;

				$qry_rows[] = '(' . implode(',', $values) . ')';
			}
		}

		$qry = null;
		if (count($qry_rows) > 0) {
			if ($keys_set)
				$qry = $qry_init . ' INTO ' . $this->parseField($table) . '(' . implode(',', $keys) . ') VALUES' . implode(',', $qry_rows);
			else
				$qry = $qry_init . ' INTO ' . $this->parseField($table) . ' VALUES' . implode(',', $qry_rows);
		}

		return $qry;
	}

	/**
	 * @param string $table
	 * @param mixed $where
	 * @param array $data
	 * @param array $options
	 * @return bool
	 */
	public function update(string $table, array|int|string $where, array $data, array $options = []): bool
	{
		$options = array_merge([
			'version' => null,
			'confirm' => false,
			'debug' => $this->options['debug'],
			'force' => false,
			'skip-user-filter' => false,
		], $options);

		if (isset($this->deferedInserts[$table]) and !$options['force'])
			$this->model->error('There are open bulk inserts on the table ' . $table . '; can\'t update');

		$tableModel = $this->getTable($table);
		$where = $this->preliminaryWhereProcessing($tableModel, $where);

		$this->addUserFilter($where, $tableModel, $options);

		$this->trigger('update', [
			'table' => $table,
			'where' => $where,
			'data' => $data,
			'options' => $options,
		]);

		$data = $this->filterColumns($table, $data);
		$this->checkDbData($table, $data['data'], $options);
		if ($data['multilang']) {
			$multilangTable = $this->model->_Multilang->getTableFor($table);
			$multilangOptions = $this->model->_Multilang->getTableOptionsFor($table);
			foreach ($data['multilang'] as $lang => $multilangData)
				$this->checkDbData($multilangTable, $multilangData, $options);
		}

		$this->initDb();

		try {
			$this->beginTransaction();

			if ($options['version'] !== null and array_keys($where) === [$tableModel->primary[0]]) {
				$lastVersion = $this->getVersionLock($table, $where[$tableModel->primary[0]]);

				if ($lastVersion > $options['version'])
					$this->model->error('A new version of this element has been saved in the meanwhile. Reload the page and try again.');

				$this->insert('model_version_locks', [
					'table' => $table,
					'row' => $where[$tableModel->primary[0]],
					'version' => $options['version'] + 1,
					'date' => date('Y-m-d H:i:s'),
				]);
			}

			$where_str = $this->makeSqlString($table, $where, ' AND ', ['main_alias' => 't']);
			if (empty($where_str) and !$options['confirm'])
				$this->model->error('Tried to update full table without explicit confirm');

			if ($data['data']) {
				$qry = 'UPDATE ' . $this->parseField($table) . ' AS `t` SET ' . $this->makeSqlString($table, $data['data'], ',', ['for_where' => false]) . ($where_str ? ' WHERE ' . $where_str : '');

				if ($options['debug'] and DEBUG_MODE)
					echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

				$this->query($qry, $table, 'UPDATE', $options);
			}

			foreach ($data['multilang'] as $lang => $multilangData) {
				if (!$multilangData)
					continue;

				$ml_where_str = ' WHERE `ml`.' . $this->parseField($multilangOptions['lang']) . ' = ' . $this->parseValue($lang);
				if ($where_str)
					$ml_where_str .= ' AND (' . $where_str . ')';
				$qry = 'UPDATE ' . $this->parseField($multilangTable) . ' AS `ml` INNER JOIN ' . $this->parseField($table) . ' AS `t` ON `t`.`' . $tableModel->primary[0] . '` = `ml`.' . $this->parseField($multilangOptions['keyfield']) . ' SET ' . $this->makeSqlString($table, $multilangData, ',', ['for_where' => false, 'main_alias' => 'ml']) . $ml_where_str;

				if ($options['debug'] and DEBUG_MODE)
					echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

				$result = $this->query($qry, $table, 'UPDATE', $options);
				if ($result->rowCount() === 0 and isset($where[$tableModel->primary[0]])) { // If there is no multilang row in the db, and I have the row id, I create one
					$multilangWhere = [
						$multilangOptions['keyfield'] => $where[$tableModel->primary[0]],
						$multilangOptions['lang'] => $lang,
					];
					if ($this->count($multilangTable, $multilangWhere) === 0) // I actually check that the row does not exist (rowCount can return 0 if the updated data are identical to the existing ones)
						$this->insert($multilangTable, array_merge($multilangData, $multilangWhere));
				}
			}

			if (array_key_exists($table, $this->options['linked-tables'])) {
				$linked_table = $this->options['linked-tables'][$table]['with'];
				$linkedTableModel = $this->getTable($linked_table);

				if ($data['custom-data']) {
					$qry = 'UPDATE ' . $this->parseField($linked_table) . ' AS `c` INNER JOIN ' . $this->parseField($table) . ' AS `t` ON `t`.`' . $tableModel->primary[0] . '` = `c`.`' . $linkedTableModel->primary[0] . '` SET ' . $this->makeSqlString($linked_table, $data['custom-data'], ',', ['for_where' => false, 'main_alias' => 'c']) . ($where_str ? ' WHERE ' . $where_str : '');

					if ($options['debug'] and DEBUG_MODE)
						echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

					$this->query($qry, $table, 'UPDATE', $options);
				}

				if ($data['custom-multilang']) {
					$customMultilangTable = $linked_table . $this->model->_Multilang->tables[$table]['suffix'];
					$multilangTableModel = $this->getTable($multilangTable);
					$customMultilangModel = $this->getTable($customMultilangTable);

					foreach ($data['custom-multilang'] as $lang => $multilangData) {
						if (!$multilangData)
							continue;

						$this->checkDbData($customMultilangTable, $multilangData, $options);

						$ml_where_str = ' WHERE `ml`.' . $this->parseField($multilangOptions['lang']) . ' = ' . $this->parseValue($lang);
						if ($where_str)
							$ml_where_str .= ' AND (' . $where_str . ')';
						$qry = 'UPDATE ' . $this->parseField($customMultilangTable) . ' AS `custom_ml` INNER JOIN ' . $this->parseField($multilangTable) . ' AS `ml` ON `ml`.`' . $multilangTableModel->primary[0] . '` = `custom_ml`.`' . $customMultilangModel->primary[0] . '` INNER JOIN ' . $this->parseField($table) . ' AS `t` ON `t`.`' . $tableModel->primary[0] . '` = `ml`.' . $this->parseField($multilangOptions['keyfield']) . ' SET ' . $this->makeSqlString($customMultilangTable, $multilangData, ',', ['for_where' => false, 'main_alias' => 'custom_ml']) . $ml_where_str;

						if ($options['debug'] and DEBUG_MODE)
							echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

						$this->query($qry, $table, 'UPDATE', $options);
					}
				}
			}

			if (isset($this->cachedTables[$table])) {
				if ($this->canUseCache($table, $where, $options)) {
					$this->update_cache($table, $where, array_merge(
						$data['data'],
						count($data['multilang']) > 0 ? reset($data['multilang']) : [],
						$data['custom-data'],
						count($data['custom-multilang']) > 0 ? reset($data['custom-multilang']) : []
					), $options);
				} else {
					$this->flushTableCache($table);
				}
			}

			$this->changedTable($table);

			$this->commit();
		} catch (\Exception $e) {
			$this->rollBack();
			$this->model->error('Error while updating.', '<b>Error:</b> ' . getErr($e) . (isset($qry) ? '<br /><b>Query:</b> ' . $qry : ''));
		}
		return true;
	}

	/**
	 * @param string $table
	 * @param array|int|string $where
	 * @param array $data
	 * @param array $options
	 * @return null|int
	 */
	public function updateOrInsert(string $table, array|int|string $where, array $data, array $options = []): ?int
	{
		$tableModel = $this->getTable($table);
		if (!is_array($where) and is_numeric($where))
			$where = [$tableModel->primary[0] => $where];

		$check = $this->select($table, $where, [
			'auto_ml' => false,
			'auto-join-linked-tables' => false,
		]);
		if ($check) {
			$this->update($table, $where, $data, $options);
			return $check[$tableModel->primary[0]];
		} else {
			return $this->insert($table, array_merge($where, $data), $options);
		}
	}

	/**
	 * @param string $table
	 * @param array|int|string $where
	 * @param array $options
	 * @return bool|string
	 */
	public function delete(string $table, array|int|string $where = [], array $options = [])
	{
		$options = array_merge([
			'confirm' => false,
			'joins' => [],
			'debug' => $this->options['debug'],
			'force' => false,
			'skip-user-filter' => false,
			'return_query' => false,
		], $options);

		if (isset($this->deferedInserts[$table]) and !$options['force'])
			$this->model->error('There are open bulk inserts on the table ' . $table . '; can\'t delete');

		$tableModel = $this->getTable($table);
		$where = $this->preliminaryWhereProcessing($tableModel, $where);

		$this->addUserFilter($where, $tableModel, $options);

		$this->trigger('delete', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		$joins = $this->elaborateJoins($table, $options['joins']);

		$join_str = '';
		$cj = 0;
		foreach ($joins as $join) {
			if (!isset($join['type']))
				$join['type'] = 'INNER';

			if (isset($join['alias']))
				$joinAlias = $join['alias'];
			else
				$joinAlias = 'j' . $cj;

			if (isset($join['full_on'])) {
				$join_str .= ' ' . $join['type'] . ' JOIN ' . $this->parseField($join['table']) . ' ' . $joinAlias . ' ON (' . $join['full_on'] . ')';
			} else {
				$join_where = array_merge([
					$joinAlias . '.' . $this->parseField($join['join_field']) . ' = t.' . $this->parseField($join['on']),
				], $join['where']);

				$join_str .= ' ' . $join['type'] . ' JOIN ' . $this->parseField($join['table']) . ' ' . $joinAlias . ' ON (' . $this->makeSqlString($table, $join_where, 'AND', ['joins' => $joins]) . ')';
			}
			$cj++;
		}

		$where_str = $this->makeSqlString($table, $where, ' AND ', ['joins' => $joins]);
		$where_str = empty($where_str) ? '' : ' WHERE ' . $where_str;
		if (empty($where_str) and !$options['confirm'])
			$this->model->error('Tried to delete full table without explicit confirm');

		if (in_array($table, $this->options['autoHide']))
			$qry = 'UPDATE ' . $this->parseField($table) . ' t' . $join_str . ' SET t.zk_deleted = 1' . $where_str;
		else
			$qry = 'DELETE t FROM ' . $this->parseField($table) . ' t' . $join_str . $where_str;

		if ($options['debug'] and DEBUG_MODE)
			echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

		if ($options['return_query'])
			return $qry;

		try {
			$this->beginTransaction();

			$this->query($qry, $table, 'DELETE', $options);

			if (isset($this->cachedTables[$table])) {
				if ($this->canUseCache($table, $where, $options))
					$this->delete_cache($table, $where, $options);
				else
					$this->flushTableCache($table);
			}

			$this->changedTable($table);

			$this->commit();
		} catch (\Exception $e) {
			$this->rollBack();

			$messaggio = 'Error while deleting';
			$messaggio2 = getErr($e);
			if (stripos($messaggio2, 'a foreign key constraint fails') !== false) {
				preg_match_all('/`([^`]+?)`, CONSTRAINT `(.+?)` FOREIGN KEY \(`(.+?)`\) REFERENCES `(.+?)` \(`(.+?)`\)/i', $messaggio2, $matches, PREG_SET_ORDER);

				if (count($matches[0]) == 6) {
					$fk = $matches[0];
					$messaggio = 'Impossibile: si sta tentando di eliminare un record di "<b>' . $fk[4] . '</b>" di cui &egrave; presente un riferimento nella tabella "<b>' . $fk[1] . '</b>" (sotto la colonna: "<b>' . $fk[3] . '</b>")';
				}
			}
			$this->model->error($messaggio, '<b>Error:</b> ' . $messaggio2 . '<br /><b>Query:</b> ' . $qry);
		}
		return true;
	}

	/**
	 * @param string $table
	 * @param array|int|string $where
	 * @param array $opt
	 * @return iterable|string
	 */
	public function select_all(string $table, array|int|string $where = [], array $opt = []): iterable|string
	{
		$opt['multiple'] = true;
		return $this->select($table, $where, $opt);
	}

	/**
	 * @param string $table
	 * @param array|int|string $where
	 * @param array|string $opt
	 * @return mixed
	 */
	public function select(string $table, array|int|string $where = [], array|string $opt = []): mixed
	{
		$this->initDb();

		if (isset($this->deferedInserts[$table]))
			$this->model->error('There are open bulk inserts on the table ' . $table . '; can\'t read');
		if (!is_array($opt))
			$opt = ['field' => $opt];

		$tableModel = $this->getTable($table);
		$where = $this->preliminaryWhereProcessing($tableModel, $where);

		$multilang = $this->model->isLoaded('Multilang') ? $this->model->getModule('Multilang') : false;
		$lang = $multilang ? $multilang->lang : 'it';

		$options = array_merge([
			'multiple' => false,
			'operator' => 'AND',
			'distinct' => false,
			'limit' => false,
			'order_by' => false,
			'group_by' => false,
			'having' => [],
			'auto_ml' => true,
			'lang' => $lang,
			'fallback' => true,
			'joins' => [],
			'field' => false,
			'fields' => [],
			'only-aggregates' => false,
			'min' => [],
			'max' => [],
			'sum' => [],
			'avg' => [],
			'count' => [],
			'debug' => $this->options['debug'],
			'return_query' => false,
			'stream' => true,
			'quick-cache' => true,
			'skip-user-filter' => false,
			'auto-join-linked-tables' => true,
		], $opt);
		if ($options['multiple'] === false and !$options['limit'])
			$options['limit'] = 1;

		$this->addUserFilter($where, $tableModel, $options);

		$this->trigger('select', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		if (!isset($opt['ignoreCache']) and $this->canUseCache($table, $where, $options))
			return $this->select_cache($table, $where, $options);

		$isMultilang = ($multilang and $options['auto_ml'] and array_key_exists($table, $multilang->tables));

		$sel_str = '';
		$join_str = '';
		if ($isMultilang) {
			$ml = $multilang->tables[$table];

			$mlTable = $table . $ml['suffix'];
			$mlTableModel = $this->getTable($mlTable);

			$mlFields = [];
			foreach ($ml['fields'] as $f) {
				if (isset($mlTableModel->columns[$f]) and $mlTableModel->columns[$f]['real'])
					$mlFields[] = $f;
			}

			$options['joins'][] = [
				'type' => 'LEFT',
				'table' => $mlTable,
				'alias' => 'lang',
				'full_on' => 'lang.' . $this->parseField($ml['keyfield']) . ' = t.`' . $tableModel->primary[0] . '` AND lang.' . $this->parseField($ml['lang']) . ' LIKE ' . $this->parseValue($options['lang']),
				'fields' => $mlFields,
			];
		}

		if ($options['auto-join-linked-tables']) {
			$customTable = null;
			// Join per tabelle custom normali
			if (array_key_exists($table, $this->options['linked-tables'])) {
				$customTable = $this->options['linked-tables'][$table]['with'];
				$isCustomTableMain = true;
			} elseif ($this->model->isLoaded('Multilang')) {
				// Join per tabelle multilingua di tabelle linked (ad esempio se faccio una query verso prova_texts deve joinarmi anche prova_custom_texts)
				foreach ($this->model->_Multilang->tables as $mlTable => $mlTableOptions) {
					if ($mlTable . $mlTableOptions['suffix'] === $table and array_key_exists($mlTable, $this->options['linked-tables'])) {
						$customTable = $this->options['linked-tables'][$mlTable]['with'] . $mlTableOptions['suffix'];
						$isCustomTableMain = false;
						break;
					}
				}
			}

			if ($customTable) {
				$customTableModel = $this->getTable($customTable);

				$customFields = [];
				foreach ($customTableModel->columns as $column_name => $column) {
					if ($column_name === $customTableModel->primary[0])
						continue;
					$customFields[] = $column_name;
				}

				$options['joins'][] = [
					'type' => 'LEFT',
					'table' => $customTable,
					'alias' => 'custom',
					'on' => $tableModel->primary[0],
					'join_field' => $customTableModel->primary[0],
					'fields' => $customFields,
				];

				if ($isMultilang and $isCustomTableMain) {
					$mlFields = [];
					foreach ($ml['fields'] as $f) {
						if (isset($mlTableModel->columns[$f]) and !$mlTableModel->columns[$f]['real'])
							$mlFields[] = $f;
					}

					$mlCustomTableModel = $this->getTable($customTable . $ml['suffix']);
					$options['joins'][] = [
						'type' => 'LEFT',
						'table' => $customTable . $ml['suffix'],
						'alias' => 'custom_lang',
						'full_on' => 'custom_lang.' . $mlCustomTableModel->primary[0] . ' = lang.`' . $mlTableModel->primary[0] . '`',
						'fields' => $mlFields,
					];
				}
			}
		}

		$joins = $this->elaborateJoins($table, $options['joins']);

		$cj = 0;
		foreach ($joins as $join) {
			if (!isset($join['type']))
				$join['type'] = 'INNER';

			if (isset($join['alias']))
				$joinAlias = $join['alias'];
			else
				$joinAlias = 'j' . $cj;

			if (isset($join['full_fields'])) {
				$sel_str .= ',' . $join['full_fields'];
			} else {
				foreach ($join['fields'] as $nf => $f) {
					if (!is_numeric($nf) and !is_array($f))
						$f = ['field' => $nf, 'as' => $f];

					if (is_array($f) and isset($f['field'], $f['as']))
						$join['fields'][$nf] = $joinAlias . '.' . $this->parseField($f['field']) . ' AS ' . $this->parseField($f['as']);
					else
						$join['fields'][$nf] = $joinAlias . '.' . $this->parseField($f);
				}
				if ($join['fields'])
					$sel_str .= ',' . implode(',', $join['fields']);
			}

			if (isset($join['full_on'])) {
				$join_str .= ' ' . $join['type'] . ' JOIN ' . $this->parseField($join['table']) . ' ' . $joinAlias . ' ON (' . $join['full_on'] . ')';
			} else {
				$join_where = array_merge([
					$joinAlias . '.' . $this->parseField($join['join_field']) . ' = t.' . $this->parseField($join['on']),
				], $join['where']);

				$join_str .= ' ' . $join['type'] . ' JOIN ' . $this->parseField($join['table']) . ' ' . $joinAlias . ' ON (' . $this->makeSqlString($table, $join_where, 'AND', ['joins' => $joins]) . ')';
			}

			$cj++;
		}

		$make_options = [
			'auto_ml' => $options['auto_ml'],
			'main_alias' => 't',
			'joins' => $joins,
		];

		$where_str = $this->makeSqlString($table, $where, ' ' . $options['operator'] . ' ', $make_options);
		if (in_array($table, $this->options['autoHide']))
			$where_str = empty($where_str) ? 't.zk_deleted = 0' : '(' . $where_str . ') AND t.zk_deleted = 0';
		$where_str = empty($where_str) ? '' : ' WHERE ' . $where_str;

		if ($options['distinct'])
			$qry = 'SELECT DISTINCT ' . $this->elaborateField($options['distinct'], $make_options) . ',';
		else
			$qry = 'SELECT ';

		$singleField = false;

		$aggregateFunctions = [
			'min',
			'max',
			'sum',
			'avg',
			'count',
		];

		$found = false;
		foreach ($aggregateFunctions as $f) {
			if ($options[$f]) {
				if (is_array($options[$f])) {
					if (!$found and !$options['only-aggregates'])
						$qry .= 't.*' . $sel_str;

					foreach ($options[$f] as $field => $alias) {
						if ($found or !$options['only-aggregates'])
							$qry .= ',';

						if (is_numeric($field) and is_string($alias)) {
							$field = $alias;
							$alias = $this->elaborateField('zkaggr_' . $field, array_merge($make_options, ['add-alias' => false]));
						}
						$qry .= strtoupper($f) . '(' . $this->elaborateField($field, $make_options) . ') AS ' . $alias;
					}

					$found = true;
				} else {
					$qry .= strtoupper($f) . '(' . $this->elaborateField($options[$f], $make_options) . ')';
					$singleField = true;
					$found = true;
					break;
				}
			}
		}

		$geometryColumns = [];
		if (!$found) {
			if ($options['field'] !== false) {
				$singleField = true;
				$qry .= $this->elaborateField($options['field'], $make_options);
			} elseif ($options['fields']) {
				$fields = [];
				foreach ($options['fields'] as $f)
					$fields[] = $this->elaborateField($f, $make_options);
				$qry .= implode(',', $fields);
			} else {
				$tempColumns = [];
				foreach ($tableModel->columns as $k => $c) {
					if ($c['type'] === 'point') {
						$geometryColumns[] = $k;
						$tempColumns[] = 'AsText(' . $this->elaborateField($k, $make_options) . ') AS ' . $this->elaborateField($k);
					} else {
						$tempColumns[] = $this->elaborateField($k, $make_options);
					}
				}
				if (count($geometryColumns) > 0)
					$qry .= implode(',', $tempColumns);
				else
					$qry .= 't.*';
				$qry .= $sel_str;
			}
		}

		$qry .= ' FROM ' . $this->parseField($table) . ' t' . $join_str . $where_str;
		if ($options['group_by']) {
			$qry .= ' GROUP BY ' . ($options['group_by']);
			if ($options['having'])
				$qry .= ' HAVING ' . $this->makeSqlString($table, $options['having'], ' AND ', array_merge($make_options, ['add-alias' => false, 'prefix' => 'zkaggr_']));
		}
		if ($options['order_by']) {
			if ($tableModel->primary and $options['order_by'] === $tableModel->primary[0])
				$options['order_by'] = 't.' . $options['order_by'];
			$qry .= ' ORDER BY ' . $options['order_by'];
		}
		if ($options['limit'])
			$qry .= ' LIMIT ' . $options['limit'];

		if ($options['return_query'])
			return $qry;

		if ($options['debug'] and DEBUG_MODE)
			echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

		if ($options['quick-cache'] and (!$options['multiple'] or !$options['stream'])) {
			$cacheKey = md5($qry . ((string)$options['field']) . json_encode($options['max']) . json_encode($options['sum']) . ((int)$options['multiple']));
			if (isset($this->queryCache[$table][$cacheKey])) {
				if ($this->queryCache[$table][$cacheKey]['query'] == $qry)
					return $this->queryCache[$table][$cacheKey]['res'];
				else
					unset($this->queryCache[$table][$cacheKey]);
			}
		}

		if (!isset($this->n_tables[$table]))
			$this->n_tables[$table] = 1;
		else
			$this->n_tables[$table]++;

		try {
			$q = $this->query($qry, $table, 'SELECT', $options);
		} catch (\Exception $e) {
			$this->model->error('Error while reading.', '<b>Errore:</b> ' . getErr($e) . '<br /><b>Query:</b> ' . $qry);
		}

		if ($singleField) {
			$return = [$options['field'] => $q->fetchColumn()];
			$return = $this->normalizeTypesInSelect($table, $return);
			$return = $return[$options['field']];

			$this->trigger('selectResult', [
				'type' => 'field',
				'response' => $return,
			]);
		} elseif ($options['multiple']) {
			$results = $this->streamResults($table, $options, $q, $isMultilang);
			if ($options['stream']) {
				$this->trigger('selectResult', [
					'type' => 'stream',
				]);

				return $results;
			} else {
				$return = [];
				foreach ($results as $k => $r)
					$return[$k] = $r;

				$this->trigger('selectResult', [
					'type' => 'multiple',
					'response' => count($return) . ' rows',
				]);
			}
		} else {
			$return = $q->fetch();
			if ($return !== false) {
				if ($isMultilang and $options['fallback'])
					$return = $this->multilangFallback($table, $return, $options);
				$return = $this->normalizeTypesInSelect($table, $return);
			}

			$this->trigger('selectResult', [
				'type' => 'row',
				'response' => $return,
			]);
		}

		if ($options['quick-cache'] and (!$options['multiple'] or !$options['stream'])) {
			$this->queryCache[$table][$cacheKey] = [
				'query' => $qry,
				'res' => $return,
			];
		}

		return $return;
	}

	/**
	 * Streams the results via generator, applying necessary modifiers (multilang fallback and fields normalization)
	 *
	 * @param string $table
	 * @param array $options
	 * @param \PDOStatement $q
	 * @param bool $isMultilang
	 * @return \Generator
	 */
	private function streamResults(string $table, array $options, \PDOStatement $q, bool $isMultilang): \Generator
	{
		foreach ($q as $r) {
			if ($isMultilang)
				$r = $this->multilangFallback($table, $r, $options);
			yield $this->normalizeTypesInSelect($table, $r);
		}
	}

	/**
	 * @param string $table
	 * @param array $data
	 * @param array $options
	 * @return array
	 */
	private function multilangFallback(string $table, array $data, array $options = []): array
	{
		if (!$this->model->_Multilang->options['fallback'] or !isset($this->model->_Multilang->tables[$table]))
			return $data;

		$mlTable = $this->model->_Multilang->tables[$table];

		$tableModel = $this->getTable($table);
		if (!isset($data[$tableModel->primary[0]]))
			return $data;

		if ($this->checkIfValidForFallback($data, $mlTable))
			return $data;

		$where = [
			$tableModel->primary[0] => $data[$tableModel->primary[0]],
		];

		foreach (($options['joins'] ?? []) as $idx => $join) {
			if (isset($join['alias']) and in_array($join['alias'], ['lang', 'custom', 'custom_lang']))
				unset($options['joins'][$idx]);
		}

		foreach ($this->model->_Multilang->options['fallback'] as $l) {
			if ($options['lang'] === $l)
				continue;

			$row = $this->select($table, $where, array_merge($options, [
				'lang' => $l,
				'multiple' => false,
				'fallback' => false,
				'stream' => false,
				'limit' => 1,
			]));
			if ($row and $this->checkIfValidForFallback($row, $mlTable))
				return $row;
		}

		return $data;
	}

	/**
	 * @param array $data
	 * @param array $mlTable
	 * @return bool
	 */
	private function checkIfValidForFallback(array $data, array $mlTable): bool
	{
		$atLeastOne = false;
		foreach ($mlTable['fields'] as $f) {
			if (isset($data[$f]) and !empty($data[$f])) {
				$atLeastOne = true;
				break;
			}
		}
		return $atLeastOne;
	}

	/**
	 * @param string $table
	 * @param array|int|string $where
	 * @param array $opt
	 * @return int
	 */
	public function count(string $table, array|int|string $where = [], array $opt = []): int
	{
		$tableModel = $this->getTable($table);
		$where = $this->preliminaryWhereProcessing($tableModel, $where);

		$this->initDb();

		$multilang = $this->model->isLoaded('Multilang') ? $this->model->getModule('Multilang') : false;
		$auto_ml = ($multilang and array_key_exists($table, $multilang->tables)) ? true : false;
		$lang = $multilang ? $multilang->lang : 'it';

		$options = [
			'multiple' => true,
			'operator' => 'AND',
			'distinct' => false,
			'limit' => false,
			'joins' => [],
			'order_by' => false,
			'group_by' => false,
			'auto_ml' => $auto_ml,
			'lang' => $lang,
			'field' => false,
			'debug' => $this->options['debug'],
			'return_query' => false,
			'skip-user-filter' => false,
		];
		$options = array_merge($options, $opt);

		$this->addUserFilter($where, $tableModel, $options);

		$this->trigger('count', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		if (!isset($opt['ignoreCache']) and $this->canUseCache($table, $where, $options))
			return count($this->select_cache($table, $where, $options));

		$join_str = '';

		$isMultilang = ($multilang and $options['auto_ml'] and array_key_exists($table, $multilang->tables));
		if ($isMultilang) {
			$ml = $multilang->tables[$table];

			$mlTable = $table . $ml['suffix'];
			$mlTableModel = $this->getTable($mlTable);

			$mlFields = [];
			foreach ($ml['fields'] as $f) {
				if (isset($mlTableModel->columns[$f]) and $mlTableModel->columns[$f]['real'])
					$mlFields[] = $f;
			}

			$options['joins'][] = [
				'type' => 'LEFT',
				'table' => $mlTable,
				'alias' => 'lang',
				'full_on' => 'lang.' . $this->parseField($ml['keyfield']) . ' = t.`' . $tableModel->primary[0] . '` AND lang.' . $this->parseField($ml['lang']) . ' LIKE ' . $this->parseValue($options['lang']),
				'fields' => $mlFields,
			];
		}

		if (array_key_exists($table, $this->options['linked-tables'])) {
			$customTable = $this->options['linked-tables'][$table]['with'];
			$customTableModel = $this->getTable($customTable);

			$customFields = [];
			foreach ($customTableModel->columns as $column_name => $column) {
				if ($column_name === $customTableModel->primary[0])
					continue;
				$customFields[] = $column_name;
			}

			$options['joins'][] = [
				'type' => 'LEFT',
				'table' => $customTable,
				'alias' => 'custom',
				'on' => $tableModel->primary[0],
				'join_field' => $customTableModel->primary[0],
				'fields' => $customFields,
			];

			if ($isMultilang) {
				$mlTableModel = $this->getTable($table . $ml['suffix']);
				$mlCustomTableModel = $this->getTable($customTable . $ml['suffix']);

				$mlFields = [];
				foreach ($ml['fields'] as $f) {
					if (isset($mlTableModel->columns[$f]) and !$mlTableModel->columns[$f]['real'])
						$mlFields[] = $f;
				}

				$options['joins'][] = [
					'type' => 'LEFT',
					'table' => $customTable . $ml['suffix'],
					'alias' => 'custom_lang',
					'full_on' => 'custom_lang.' . $this->parseField($mlCustomTableModel->primary[0]) . ' = lang.`' . $mlTableModel->primary[0] . '`',
					'fields' => $mlFields,
				];
			}
		}

		$joins = $this->elaborateJoins($table, $options['joins']);

		$cj = 0;
		foreach ($joins as $join) {
			if (!isset($join['type']))
				$join['type'] = 'INNER';

			if (isset($join['alias']))
				$joinAlias = $join['alias'];
			else
				$joinAlias = 'j' . $cj;

			if (isset($join['full_on'])) {
				$join_str .= ' ' . $join['type'] . ' JOIN ' . $this->parseField($join['table']) . ' ' . $joinAlias . ' ON (' . $join['full_on'] . ')';
			} else {
				$join_where = array_merge([
					$joinAlias . '.' . $this->parseField($join['join_field']) . ' = t.' . $this->parseField($join['on']),
				], $join['where']);

				$join_str .= ' ' . $join['type'] . ' JOIN ' . $this->parseField($join['table']) . ' ' . $joinAlias . ' ON (' . $this->makeSqlString($table, $join_where, 'AND', ['joins' => $joins]) . ')';
			}
			$cj++;
		}

		$make_options = [
			'auto_ml' => $options['auto_ml'],
			'main_alias' => 't',
			'joins' => $joins,
		];
		$where_str = $this->makeSqlString($table, $where, ' ' . $options['operator'] . ' ', $make_options);

		if (in_array($table, $this->options['autoHide']))
			$where_str = empty($where_str) ? 'zk_deleted = 0' : '(' . $where_str . ') AND zk_deleted = 0';
		$where_str = empty($where_str) ? '' : ' WHERE ' . $where_str;

		if ($options['distinct'])
			$qry = 'SELECT COUNT(DISTINCT ' . $this->elaborateField($options['distinct'], $make_options) . ') ';
		elseif ($options['group_by'])
			$qry = 'SELECT SQL_CALC_FOUND_ROWS * ';
		else
			$qry = 'SELECT COUNT(*) ';

		$qry .= 'FROM ' . $this->parseField($table) . ' t' . $join_str . $where_str;
		if ($options['group_by'] != false) $qry .= ' GROUP BY ' . ($options['group_by']);
		if ($options['limit'] != false) $qry .= ' LIMIT ' . ($options['limit']);

		if ($options['return_query'])
			return $qry;

		if ($options['debug'] and DEBUG_MODE)
			echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

		$cacheKey = md5($qry);
		if (isset($this->queryCache[$table][$cacheKey])) {
			if ($this->queryCache[$table][$cacheKey]['query'] == $qry)
				return $this->queryCache[$table][$cacheKey]['res'];
			else
				unset($this->queryCache[$table][$cacheKey]);
		}

		if (!isset($this->n_tables[$table . '-count']))
			$this->n_tables[$table . '-count'] = 1;
		else
			$this->n_tables[$table . '-count']++;

		try {
			$q = $this->query($qry, $table, 'COUNT');
			if ($options['group_by'])
				$q = $this->query('SELECT FOUND_ROWS()', $table, 'COUNT', ['log' => false, 'query-limit' => false]);
		} catch (\Exception $e) {
			$this->model->error('Errore durante la lettura dei dati.', '<b>Errore:</b> ' . $e->getMessage() . '<br /><b>Query:</b> ' . $qry);
		}

		$return = (int)$q->fetchColumn();

		$this->queryCache[$table][$cacheKey] = [
			'query' => $qry,
			'res' => $return,
		];

		return $return;
	}

	/**
	 * Given a multilang table and an id, returns all multilang rows for that row
	 * If no id is provided, returns a list of fields set to null
	 *
	 * @param string $table
	 * @param int|null $id
	 * @return array
	 */
	public function getMultilangTexts(string $table, int $id = null): array
	{
		if (!$this->model->isLoaded('Multilang'))
			return [];

		$languageVersions = [];
		foreach ($this->model->_Multilang->langs as $l)
			$languageVersions[$l] = [];

		$tableModel = $this->getTable($table);
		if ($tableModel) {
			$columns = $tableModel->columns;

			if (array_key_exists($table, $this->model->_Multilang->tables)) {
				$multilangTable = $table . $this->model->_Multilang->tables[$table]['suffix'];
				$multilangTableModel = $this->getTable($multilangTable);
				$multilangColumns = [];
				foreach ($this->model->_Multilang->tables[$table]['fields'] as $ml) {
					$columns[$ml] = $multilangTableModel->columns[$ml];
					$multilangColumns[] = $ml;
				}

				$langColumn = $this->model->_Multilang->tables[$table]['lang'];

				$fieldsToExtract = $multilangColumns;
				$fieldsToExtract[] = $langColumn;

				if ($id) {
					$languageVersionsQ = $this->select_all($multilangTable, [
						$this->model->_Multilang->tables[$table]['keyfield'] => $id,
					], [
						'fields' => $fieldsToExtract,
						'fallback' => false,
					]);
					foreach ($languageVersionsQ as $r) {
						$lang = $r[$langColumn];
						unset($r[$langColumn]);
						$languageVersions[$lang] = $r;
					}

					foreach ($languageVersions as &$r) {
						foreach ($multilangColumns as $k) {
							if (!array_key_exists($k, $r))
								$r[$k] = null;
						}
					}
					unset($r);
				} else {
					foreach ($languageVersions as $lang => $l_arr) {
						foreach ($this->model->_Multilang->tables[$table]['fields'] as $f)
							$languageVersions[$lang][$f] = null;
					}
				}
			}
		}

		return $languageVersions;
	}

	/* Utilites for CRUD methods */

	/**
	 * @param string $table
	 * @param array $row
	 * @return array
	 */
	private function normalizeTypesInSelect(string $table, array $row): array
	{
		try {
			$tableModel = $this->getTable($table);
		} catch (\Exception $e) {
			return $row;
		}

		$newRow = [];
		foreach ($row as $k => $v) {
			if (strpos($k, 'zkaggr_') === 0) // Remove aggregates prefix
				$k = substr($k, 7);

			if ($v !== null and $v !== false) {
				if (array_key_exists($k, $tableModel->columns)) {
					$c = $tableModel->columns[$k];
					if (in_array($c['type'], ['double', 'float', 'decimal']))
						$v = (float)$v;
					if (in_array($c['type'], ['tinyint', 'smallint', 'mediumint', 'int', 'bigint', 'year']))
						$v = (int)$v;
					if ($c['type'] === 'point') {
						$v = array_map(function ($v) {
							return (float)$v;
						}, explode(' ', substr($v, 6, -1)));
						if (count($v) !== 2 or ($v[0] == 0 and $v[1] == 0))
							$v = null;
					}
				}
			}

			$newRow[$k] = $v;
		}

		return $newRow;
	}

	/**
	 * @param array $where
	 * @param Table $tableModel
	 * @param array $options
	 */
	private function addUserFilter(array &$where, Table $tableModel, array $options)
	{
		if (
			$this->options['user-filter']
			and !$options['skip-user-filter']
			and isset($tableModel->columns[$this->options['user-filter']['column']])
			and (!isset($this->options['user-filter']['ignore']) or !in_array($tableModel->name, $this->options['user-filter']['ignore']))
		) {
			$where[$this->options['user-filter']['column']] = $this->model->getModule('User', $this->options['user-filter']['idx'])->logged();
		}
	}

	/**
	 * @param string $table
	 * @param array $joins
	 * @return array
	 */
	private function elaborateJoins(string $table, array $joins): array
	{
		/*
		Formati possibili per un join:

		//Se non si ha il modello della tabella (e relative foreign keys) è obbligatorio specificare i campi da prendere, "on" e "join_field"
		'nome tabella'
		'nome tabella'=>['campo1', 'campo2']
		['table'=>'nome tabella', 'fields'=>['campo1'=>'altra_tabella_campo', 'campo2']]
		['table'=>'nome tabella', 'on'=>'campo tabella principale', 'join_field'=>'campo tabella della join', 'fields'=>['campo1', 'campo2']]
		*/

		$return = [];
		foreach ($joins as $k => $join) {
			if (!is_array($join))
				$join = ['table' => $join];
			if (!isset($join['table']) and !isset($join['fields']) and !isset($join['on']) and !isset($join['full_on']))
				$join = ['fields' => $join];
			if (!is_numeric($k) and !isset($join['table']))
				$join['table'] = $k;
			if (!isset($join['table']))
				$this->model->error('Formato join errato');

			$tableModel = $this->getTable($table);
			$joinTableModel = $this->getTable($join['table']);

			if (!isset($join['on'], $join['join_field']) and !isset($join['full_on'])) {
				if (isset($join['on'])) {
					// Se sappiamo già quale colonna usare, andiamo a vedere se c'è una FK associata da cui prendere anche la colonna corrispondente nell'altra tabella
					if (!isset($tableModel->columns[$join['on']]))
						$this->model->error('Errore join', 'Sembra non esistere la colonna "' . $join['on'] . '" nella tabella "' . $table . '"!');

					$fk_found = false;
					foreach ($tableModel->columns[$join['on']]['foreign_keys'] as $foreign_key) {
						if ($foreign_key['ref_table'] === $join['table']) {
							$join['join_field'] = $foreign_key['ref_column'];
							$fk_found = true;
							break;
						}
					}

					if (!$fk_found)
						$this->model->error('Errore join', 'Nessuna FK sulla colonna "' . $join['on'] . '" che punti a "' . $join['table'] . '".');
				} else {
					// Altrimenti, cerchiamo di capire quale colonna usare rovistando fra le FK
					$foreign_key = null;
					foreach ($tableModel->columns as $column) {
						foreach ($column['foreign_keys'] as $fk) {
							if ($fk['ref_table'] === $join['table']) {
								if ($foreign_key === null) {
									$foreign_key = $fk;
								} else { // Ambiguo: due foreign key per la stessa tabella, non posso capire quale sia quella giusta
									$this->model->error('Errore join', 'Ci sono due foreign key nella tabella "' . $table . '" che puntano a "' . $join['table'] . '", usare la clausola "on" per specificare quale colonna utilizzare.');
								}
							}
						}
					}

					if ($foreign_key !== null) {
						$join['on'] = $foreign_key['column'];
						$join['join_field'] = $foreign_key['ref_column'];
					} else {
						foreach ($joinTableModel->columns as $column) {
							foreach ($column['foreign_keys'] as $fk) {
								if ($fk['ref_table'] === $table) {
									if ($foreign_key === null) {
										$foreign_key = $fk;
									} else { // Ambiguo: due foreign key per la stessa tabella, non posso capire quale sia quella giusta
										$this->model->error('Errore join', 'Ci sono due foreign key nella tabella "' . $join['table'] . '" che puntano a "' . $table . '", usare le clausole "on"/"join_field" per specificare quali colonne utilizzare.');
									}
								}
							}
						}

						if ($foreign_key) {
							$join['on'] = $foreign_key['ref_column'];
							$join['join_field'] = $foreign_key['column'];
						}
					}

					if ($foreign_key === null)
						$this->model->error('Errore join', 'Non trovo nessuna foreign key che leghi le tabelle "' . $table . '" e "' . $join['table'] . '". Specificare i parametri a mano.');
				}
			}

			if (!isset($join['full_fields'])) {
				if (!isset($join['fields'])) {
					$join['fields'] = [];
					foreach ($joinTableModel->columns as $k_c => $c) {
						if (isset($tableModel->columns[$k_c]))
							$join['fields'][] = ['field' => $k_c, 'as' => $join['table'] . '_' . $k_c];
						else
							$join['fields'][] = $k_c;
					}
				}

				if (!is_array($join['fields']))
					$join['fields'] = [$join['fields']];
			}

			$return[] = $join;
		}

		return $return;
	}

	/**
	 * @param string $table
	 * @param array $data
	 * @param array $options
	 * @return bool
	 */
	private function checkDbData(string $table, array $data, array $options = []): bool
	{
		$options = array_merge([
			'check' => true,
			'checkTypes' => true,
			'checkLengths' => false,
		], $options);

		if ($options['check'] === false) // Se è stata disabilitata la verifica dalle opzioni, salto la verifica
			return true;

		$tableModel = $this->getTable($table);
		foreach ($data as $k => $v) {
			if (!array_key_exists($k, $tableModel->columns))
				$this->model->error('Error while writing data.', 'Database column "' . $table . '.' . $k . '" does not exist! (either that or cache needs to be regenerated)');

			if ($options['checkTypes']) {
				if (!$this->checkType($tableModel->columns[$k], $v, $options))
					$this->model->error('Error while writing data.', 'Data type for column "' . $table . '.' . $k . '" does not match!<br />' . zkdump($v, true, true));
			}
		}

		return true;
	}

	private function checkType(array $column, mixed $v, array $options): bool
	{
		if ($v === null)
			return (bool)$column['null'];

		switch ($column['type']) {
			case 'int':
			case 'tinyint':
			case 'smallint':
			case 'mediumint':
			case 'bigint':
			case 'float':
			case 'decimal':
			case 'double':
			case 'year':
				if (!empty($v) and !is_numeric($v))
					return false;
				return true;

			case 'char':
			case 'varchar':
				if ($options['checkLengths']) {
					if (strlen($v) > $column['length'])
						return false;
				}
				return true;

			case 'date':
			case 'datetime':
				if (is_object($v) and get_class($v) == 'DateTime')
					$checkData = $v;
				else
					$checkData = date_create($v);
				if (!$checkData)
					return false;
				return true;

			default:
				return true;
		}
	}

	/**
	 * @param string $table
	 * @param array $options
	 */
	public function linkTable(string $table, array $options = [])
	{
		$options = array_merge([
			'with' => $table . '_custom',
		], $options);

		$this->options['linked-tables'][$table] = $options;
		$this->checkLinkedTableMultilang($table);
	}

	public function getLinkedTables(string $table, array $options = []): array
	{
		$options = array_merge([
			'linked' => true,
			'multilang' => true,
		], $options);

		$ret = [
			$table,
		];

		if ($options['linked']) {
			foreach ($this->options['linked-tables'] as $linkedTable => $linkedTableOptions) {
				if ($linkedTable === $table)
					$ret[] = $linkedTableOptions['with'];
				if ($linkedTableOptions['with'] === $table)
					$ret[] = $linkedTable;
			}
		}

		if ($options['multilang'] and $this->model->isLoaded('Multilang')) {
			foreach ($this->model->_Multilang->tables as $mlTable => $mlOptions) {
				if ($mlTable === $table)
					$ret[] = $mlTable . $mlOptions['suffix'];
				if ($mlTable . $mlOptions['suffix'] === $table)
					$ret[] = $mlTable;
			}
		}

		return array_unique($ret);
	}

	/**
	 * @param string $table
	 */
	private function checkLinkedTableMultilang(string $table)
	{
		if (!isset($this->options['linked-tables'][$table]))
			return;

		$customTable = $this->options['linked-tables'][$table]['with'];
		if ($this->model->isLoaded('Multilang') and array_key_exists($table, $this->model->_Multilang->tables) and !array_key_exists($customTable, $this->model->_Multilang->tables)) {
			$this->model->_Multilang->tables[$customTable] = [
				'keyfield' => 'parent',
				'lang' => 'lang',
				'suffix' => '_texts',
				'fields' => [],
			];

			$customTableModel = $this->getTable($customTable . '_texts');
			foreach ($customTableModel->columns as $k => $column) {
				if ($k === $customTableModel->primary[0] or $k === 'parent' or $k === 'lang')
					continue;

				$this->model->_Multilang->tables[$customTable]['fields'][] = $k;
			}
		}
	}

	/**
	 * @param string $table
	 */
	public function cacheTable(string $table)
	{
		if (!in_array($table, $this->tablesToCache))
			$this->tablesToCache[] = $table;
	}

	/**
	 * @param string $table
	 */
	public function uncacheTable(string $table)
	{
		$this->flushTableCache($table);

		$idx = array_search($table, $this->tablesToCache);
		if ($idx !== false)
			unset($this->tablesToCache[$idx]);
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $options
	 * @return bool
	 */
	private function canUseCache(string $table, array $where = [], array $options = []): bool
	{
		if (!in_array($table, $this->tablesToCache))
			return false;

		foreach ($where as $k => $v) {
			if (is_numeric($k) and is_string($v))
				return false;
			if (is_array($v))
				return false;
		}

		if (!in_array($options['operator'] ?? 'AND', ['AND', 'OR']))
			return false;

		if (($options['order_by'] ?? false) !== false) {
			$orderBy = str_word_count($options['order_by'], 1, '0123456789_');
			if (count($orderBy) > 2)
				return false;
			if (count($orderBy) == 2 and !in_array(strtolower($orderBy[1]), ['asc', 'desc']))
				return false;
		}

		if (isset($options['lang']) and $this->model->isLoaded('Multilang')) {
			$lang = $this->model->getModule('Multilang')->lang;
			if ($options['lang'] !== $lang)
				return false;
		}

		if (!empty($options['joins']))
			return false;

		if ($options['limit'] ?? null) {
			if (!preg_match('/^[0-9]+(,[0-9+])?$/', $options['limit']))
				return false;
		}
		return true;
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $options
	 * @return array|\Generator|bool
	 */
	private function select_cache(string $table, array $where = [], array $options = [])
	{
		if (!isset($this->n_tables[$table . '-cache']))
			$this->n_tables[$table . '-cache'] = 1;
		else
			$this->n_tables[$table . '-cache']++;

		if (!isset($this->cachedTables[$table]))
			$this->cachedTables[$table] = $this->select_all($table, [], ['stream' => false, 'ignoreCache' => true]);

		$cache = $this->cachedTables[$table];

		if ($options['order_by']) {
			if (strtoupper($options['order_by']) === 'RAND()') {
				shuffle($cache);
			} else {
				$orderBy = str_word_count($options['order_by'], 1, '0123456789_');
				if (count($orderBy) === 1) $orderBy = [$orderBy[0], 'ASC'];
				$orderBy0 = $orderBy[0];
				$orderBy1 = strtoupper($orderBy[1]);

				usort($cache, function ($a, $b) use ($orderBy0, $orderBy1) {
					if ($a[$orderBy0] == $b[$orderBy0]) return 0;
					if (is_numeric($a[$orderBy0]) and is_numeric($b[$orderBy0])) {
						switch ($orderBy1) {
							case 'DESC':
								return $a[$orderBy0] < $b[$orderBy0] ? 1 : -1;
								break;
							default:
								return $a[$orderBy0] > $b[$orderBy0] ? 1 : -1;
								break;
						}
					} else {
						$cmp = strcasecmp($a[$orderBy0], $b[$orderBy0]);
						if ($orderBy1 == 'DESC') $cmp *= -1;
						return $cmp;
					}
				});
			}
		}

		$results = [];
		foreach ($cache as $row) {
			if (!$this->isCachedRowMatching($row, $where, $options['operator'] ?? 'AND'))
				continue;

			if ($options['multiple']) {
				$results[] = $row;
			} else {
				if ($options['field']) return $row[$options['field']];
				else return $row;
			}
		}

		if ($options['multiple']) {
			if ($options['limit']) {
				if (is_numeric($options['limit'])) {
					$return = array_slice($results, 0, $options['limit']);
				} else {
					$limit = explode(',', $options['limit']);
					$return = array_slice($results, $limit[0], $limit[1]);
				}
			} else {
				$return = $results;
			}

			if (isset($options['stream']) and $options['stream']) {
				return $this->streamCacheResults($return);
			} else {
				return $return;
			}
		} else {
			return false;
		}
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $options
	 */
	private function delete_cache(string $table, array $where = [], array $options = [])
	{
		foreach ($this->cachedTables[$table] as $idx => $row) {
			if ($this->isCachedRowMatching($row, $where, $options['operator'] ?? 'AND'))
				unset($this->cachedTables[$table][$idx]);
		}
	}

	/**
	 * @param string $table
	 * @param array $data
	 */
	private function insert_cache(string $table, array $data = [])
	{
		$this->cachedTables[$table][] = $data;
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $data
	 * @param array $options
	 */
	private function update_cache(string $table, array $where = [], array $data = [], array $options = [])
	{
		foreach ($this->cachedTables[$table] as $idx => $row) {
			if (!$this->isCachedRowMatching($row, $where, $options['operator'] ?? 'AND'))
				continue;

			$this->cachedTables[$table][$idx] = array_merge($row, $data);
		}
	}

	/**
	 * @param array $row
	 * @param array $where
	 * @param string $operator
	 * @return bool
	 */
	private function isCachedRowMatching(array $row, array $where, string $operator = 'AND'): bool
	{
		if (empty($where)) {
			return true;
		} else {
			switch ($operator) {
				case 'AND':
					foreach ($where as $k => $v) {
						if ((string)$row[$k] != (string)$v)
							return false;
					}
					return true;
					break;
				case 'OR':
					foreach ($where as $k => $v) {
						if ((string)$row[$k] == (string)$v)
							return true;
					}
					return false;
					break;
				default:
					$this->model->error('Unknown operator');
					break;
			}
		}
	}

	/**
	 * @param array $results
	 * @return \Generator
	 */
	private function streamCacheResults(array $results): \Generator
	{
		foreach ($results as $r)
			yield $r;
	}

	/**
	 * @param string $table
	 */
	private function changedTable(string $table)
	{
		if (isset($this->queryCache[$table]))
			$this->queryCache[$table] = [];

		$this->trigger('changedTable', [
			'table' => $table,
		]);
	}

	/**
	 * @param string $table
	 */
	private function flushTableCache(string $table)
	{
		if (isset($this->cachedTables[$table]))
			unset($this->cachedTables[$table]);
	}

	/**
	 * @param string $t
	 * @return string
	 * @deprecated Use parseField instead
	 */
	public function makeSafe(string $t): string
	{
		return $this->parseField($t);
	}

	/**
	 * @param string $k
	 * @param array $opt
	 * @return string
	 */
	private function elaborateField(string $k, array $opt = []): string
	{
		$options = array_merge([
			'auto_ml' => false,
			'main_alias' => false,
			'joins' => [],
			'add-alias' => true,
		], $opt);
		$kr = $this->parseField($k);

		$changed = false;
		$cj = 0;
		foreach ($options['joins'] as $join) {
			if (!isset($join['full_fields'])) {
				if (isset($join['alias']))
					$joinAlias = $join['alias'];
				else
					$joinAlias = 'j' . $cj;

				if (!is_array($join['fields']))
					$join['fields'] = [$join['fields']];

				foreach ($join['fields'] as $nf => $f) {
					if (is_array($f) and isset($f['as']))
						$ff = $f['as'];
					else
						$ff = $f;

					if ($ff == $k) {
						if (!is_array($f) and is_numeric($nf)) {
							$kr = $joinAlias . '.' . $kr;
						} else {
							if (is_array($f)) $kr = 'j' . $cj . '.' . $f['field'];
							else $kr = $joinAlias . '.' . $nf;
						}
						$changed = true;
					}
				}
			}
			$cj++;
		}

		if (!$changed and $options['main_alias'])
			$kr = $options['main_alias'] . '.' . $kr;

		if (!$options['add-alias']) {
			$kr = explode('.', $kr);
			if (count($kr) > 1) {
				unset($kr[0]);
				$kr = implode(',', $kr);
			}
		}

		return $kr;
	}

	/**
	 * @param string $t
	 * @return string
	 */
	public function parseField(string $t): string
	{
		return '`' . preg_replace('/[^a-zA-Z0-9_.,()!=<> -]+/', '', $t) . '`';
	}

	/**
	 * @param mixed $v
	 * @return string
	 */
	public function parseValue(mixed $v): string
	{
		if ($v === null)
			return 'NULL';

		if (is_object($v)) {
			if (get_class($v) == 'DateTime')
				$v = $v->format('Y-m-d H:i:s');
			else
				$this->model->error('Only DateTime objects can be passed as Db values.');
		}

		if (is_array($v)) {
			if (count($v) === 2 and is_numeric($v[0]) and is_numeric($v[1]))
				return 'POINT(' . $v[0] . ',' . $v[1] . ')';
			else
				$this->model->error('Unknown value type');
		}

		$this->initDb();

		return $this->db->quote($v);
	}

	/**
	 * @param Table $tableModel
	 * @param array|int|string $where
	 * @return array
	 */
	private function preliminaryWhereProcessing(Table $tableModel, array|int|string $where): array
	{
		if (is_array($where)) {
			return $where;
		} else {
			if (is_numeric($where)) {
				if (count($tableModel->primary) !== 1)
					throw new \Exception('In order to select by id, table must have only one primary key');

				$where = [
					$tableModel->primary[0] => $where,
				];
			} elseif (is_string($where)) {
				$where = [$where];
			}

			return $where;
		}
	}

	/**
	 * @param string $table
	 * @param array $array
	 * @param string $glue
	 * @param array $options
	 * @return string
	 */
	public function makeSqlString(string $table, array $array, string $glue, array $options = []): string
	{
		$options = array_merge([
			'for_where' => true,
			'auto_ml' => false,
			'main_alias' => false,
			'joins' => [],
			'prefix' => null,
		], $options);

		$tableModel = $this->getTable($table);

		$str = [];
		foreach ($array as $k => $v) {
			$alreadyParsed = false;

			if (is_array($v)) {
				if (!is_numeric($k) and (strtoupper($k) === 'OR' or strtoupper($k) === 'AND')) {
					$sub_str = $this->makeSqlString($table, $v, $k, $options);
					if (!empty($sub_str))
						$str[] = '(' . $sub_str . ')';
					continue;
				} elseif (!is_numeric($k) and isset($tableModel->columns[$k]) and strtoupper($tableModel->columns[$k]['type']) === 'POINT') {
					if (count($v) !== 2 or !is_numeric($v[0]) or !is_numeric($v[1]))
						$this->model->error('Wrong point format');
					$v1 = 'POINT(' . $v[0] . ',' . $v[1] . ')';
					$operator = '=';
					$alreadyParsed = true;
				} elseif (isset($v['operator'], $v['sub'])) {
					$sub_str = $this->makeSqlString($table, $v['sub'], $v['operator'], $options);
					if (!empty($sub_str))
						$str[] = '(' . $sub_str . ')';
					continue;
				} else {
					$n_elementi = count($v);
					if ($n_elementi < 2 or $n_elementi > 4 or count(array_filter(array_keys($v), 'is_numeric')) < $n_elementi)
						continue;

					switch ($n_elementi) {
						case 2:
							if (is_numeric($k)) {
								$k = $v[0];
								$operator = '=';
							} else {
								$operator = $v[0];
							}

							$v1 = $v[1];
							break;
						case 3:
							if (is_string($v[0]) and strtoupper($v[0]) === 'BETWEEN') {
								$operator = $v[0];
								$v1 = $v[1];
								$v2 = $v[2];
							} else {
								$k = $v[0];
								$operator = $v[1];
								$v1 = $v[2];
							}
							break;
						case 4:
							if (strtoupper($v[1]) !== 'BETWEEN')
								continue 2;

							$k = $v[0];
							$operator = $v[1];
							$v1 = $v[2];
							$v2 = $v[3];
							break;
					}

					if (in_array(trim(strtoupper($operator)), ['IN', 'NOT IN'])) {
						if (!is_array($v1))
							$this->model->error('Expected array after a "' . trim($operator) . '" operator');

						$alreadyParsed = true;
						$v1 = '(' . implode(',', array_map(function ($el) {
								return $this->parseValue($el);
							}, $v1)) . ')';
					} else {
						if (is_array($v1))
							$this->model->error('Arrays can only be used with IN or NOT IN operators');
					}
				}
			} else {
				if (is_numeric($k)) {
					$str[] = '(' . $v . ')';
					continue;
				} else {
					$v1 = $v;
					$operator = '=';
				}
			}

			$operator = strtoupper($operator);

			if (is_array($k)) {
				if ($operator !== 'MATCH')
					throw new \Exception('You can use multiple fields only with MATCH operator');
			} else {
				$k = [$k];
			}
			foreach ($k as &$k_field) {
				if ($options['prefix'])
					$k_field = $options['prefix'] . $k_field;
				$k_field = $this->elaborateField($k_field, $options);
			}
			unset($k_field);
			$k = implode(',', $k);

			if (!$alreadyParsed) {
				if ($v1 === null and $options['for_where']) {
					if ($operator === '=')
						$operator = 'IS';
					elseif ($operator === '!=')
						$operator = 'IS NOT';
				}

				$v1 = $this->parseValue($v1);
			}

			switch ($operator) {
				case 'BETWEEN':
					$v2 = $this->parseValue($v2);
					$str[] = $k . ' BETWEEN ' . $v1 . ' AND ' . $v2;
					break;
				case 'MATCH':
					$str[] = 'MATCH(' . $k . ') AGAINST(' . $v1 . ')';
					break;
				default:
					$str[] = $k . ' ' . $operator . ' ' . $v1;
					break;
			}
		}

		return implode(' ' . $glue . ' ', $str);
	}

	/**
	 * @param string $type
	 * @param int $n
	 * @return bool
	 */
	public function setQueryLimit(string $type, int $n): bool
	{
		switch ($type) {
			case 'query':
				$this->options['query-limit'] = $n;
				break;
			case 'table':
				$this->options['query-limit-table'] = $n;
				break;
			default:
				return false;
				break;
		}

		return true;
	}

	/* Dealing with table models */

	/**
	 * @param string $table
	 * @param array $data
	 * @return array
	 */
	private function filterColumns(string $table, array $data): array
	{
		$tableModel = $this->getTable($table);

		$mainData = [];
		$multilangData = [];
		$customData = [];
		$customMultilang = [];

		foreach ($data as $k => $v) {
			if (array_key_exists($k, $tableModel->columns)) {
				$c = $tableModel->columns[$k];
				if (isset($c['extra']) and stripos($c['extra'], 'GENERATED') !== false)
					continue;

				if ($c['real'])
					$mainData[$k] = $v;
				elseif (array_key_exists($table, $this->options['linked-tables']))
					$customData[$k] = $v;
			}
		}

		if ($this->model->isLoaded('Multilang') and array_key_exists($table, $this->model->_Multilang->tables)) {
			foreach ($this->model->_Multilang->langs as $lang) {
				$multilangData[$lang] = [];
				$customMultilang[$lang] = [];
			}

			$mlTableModel = $this->getTable($table . $this->model->_Multilang->tables[$table]['suffix']);

			foreach ($data as $k => $v) {
				if (!isset($mlTableModel->columns[$k]))
					continue;

				if (in_array($k, $this->model->_Multilang->tables[$table]['fields']) and $mlTableModel->columns[$k]['real']) {
					if (array_key_exists($k, $mainData)) // A field cannot exist both in the main table and in the multilang table
						unset($mainData[$k]);

					if (!is_array($v)) {
						$v = [
							$this->model->_Multilang->lang => $v,
						];
					}

					foreach ($v as $lang => $subValue) {
						if (!in_array($lang, $this->model->_Multilang->langs))
							continue;
						$multilangData[$lang][$k] = $subValue;
					}
				}

				if (array_key_exists($table, $this->options['linked-tables']) and !$mlTableModel->columns[$k]['real']) {
					if (array_key_exists($k, $customData)) // A field cannot exist both in the main table and in the multilang table
						unset($customData[$k]);

					if (!is_array($v)) {
						$v = [
							$this->model->_Multilang->lang => $v,
						];
					}

					foreach ($v as $lang => $subValue) {
						if (!in_array($lang, $this->model->_Multilang->langs))
							continue;
						$customMultilang[$lang][$k] = $subValue;
					}
				}
			}
		}

		return [
			'data' => $mainData,
			'multilang' => $multilangData,
			'custom-data' => $customData,
			'custom-multilang' => $customMultilang,
		];
	}

	/**
	 * @param string $name
	 * @return Table
	 */
	public function getTable(string $name): Table
	{
		if (isset($this->tables[$name]))
			return $this->tables[$name];

		if (!isset($this->parser)) {
			$this->initDb();
			$this->parser = new Parser($this->db);
		}

		$tableModel = $this->parser->getTable($name);

		$customTableModel = null;
		if (array_key_exists($name, $this->options['linked-tables'])) {
			$customTableModel = $this->parser->getTable($this->options['linked-tables'][$name]['with']);
		} elseif ($this->model->isLoaded('Multilang')) {
			foreach ($this->model->_Multilang->tables as $mlTable => $mlTableOptions) {
				if ($mlTable . $mlTableOptions['suffix'] === $name and array_key_exists($mlTable, $this->options['linked-tables'])) {
					$customTableModel = $this->parser->getTable($this->options['linked-tables'][$mlTable]['with'] . $mlTableOptions['suffix']);
					break;
				}
			}
		}

		if ($customTableModel)
			$tableModel->loadColumns($customTableModel->columns, false);

		return $tableModel;
	}

	/**
	 * @param string $table
	 * @param int $id
	 * @return int
	 */
	public function getVersionLock(string $table, int $id): int
	{
		$version = $this->select('model_version_locks', [
			'table' => $table,
			'row' => $id,
		], [
			'order_by' => 'id DESC',
		]);

		return $version ? $version['version'] : 1;
	}
}
