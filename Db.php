<?php namespace Model\Db;

use Model\Core\Module;

class Db extends Module
{
	/** @var string */
	public $name;
	/** @var string */
	public $unique_id;
	/** @var \PDO */
	protected $db;
	/** @var Table[] */
	protected $tables = [];

	/** @var int */
	public $n_query = 0;
	/** @var int */
	public $n_prepared = 0;
	/** @var array */
	public $n_tables = [];

	/** @var int */
	protected $c_transactions = 0;

	/** @var array */
	protected $querylimit_counter = [
		'query' => [],
		'table' => [],
	];

	/** @var array */
	public $options = [
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

	/** @var array */
	protected $tablesToCache = [];
	/** @var array */
	protected $cachedTables = [];
	/** @var array */
	protected $queryCache = [];

	/** @var array */
	protected $deferedInserts = [];

	/**
	 * @param array $options
	 */
	public function init(array $options)
	{
		if ($this->module_id !== 0)
			$this->options['db'] = $this->module_id;

		$this->options = array_merge($this->options, $options);

		try {
			if ($this->options['direct-pdo']) {
				$this->db = $this->options['direct-pdo'];
				$this->unique_id = 'custom';
			} else {
				$config = $this->retrieveConfig();
				if (!$config or !isset($config['databases'][$this->options['db']]))
					throw new \Exception('Missing database configuration for ' . $options['db'] . ' database!');

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

				$this->db = new \PDO('mysql:host=' . $this->options['host'] . ';dbname=' . $this->options['database'] . ';charset=utf8', $this->options['username'], $this->options['password'], [
					\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
					\PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC,
					\PDO::ATTR_EMULATE_PREPARES => $this->options['emulate_prepares'],
					\PDO::ATTR_STRINGIFY_FETCHES => false,
					\PDO::MYSQL_ATTR_USE_BUFFERED_QUERY => $this->options['use_buffered_query'],
					\PDO::MYSQL_ATTR_LOCAL_INFILE => $this->options['local_infile'],
				]);
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
			}

			$this->tablesToCache = $this->options['cache-tables'];

			if (!isset($this->options['user-filter']))
				$this->options['user-filter'] = null;
			if ($this->options['user-filter'] and (!is_array($this->options['user-filter']) or count($this->options['user-filter']) < 2 or !isset($this->options['user-filter']['idx'], $this->options['user-filter']['column'])))
				$this->options['user-filter'] = null;
		} catch (\Exception $e) {
			$this->model->error('Error while connecting to database: ' . $e->getMessage());
		}
	}

	/**
	 * @param string $name
	 * @param array $arguments
	 * @return mixed
	 */
	function __call(string $name, array $arguments)
	{
		if (method_exists($this->db, $name)) {
			return call_user_func_array([$this->db, $name], $arguments);
		}
		return null;
	}

	/**
	 * @param string $qry
	 * @param string $table
	 * @param string $type
	 * @param array $options
	 * @return \PDOStatement|int
	 */
	public function query(string $qry, string $table = null, string $type = null, array $options = [])
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
		$this->n_prepared++;
		return $this->db->prepare($qry, $options);
	}

	/**
	 * @return bool
	 */
	public function beginTransaction(): bool
	{
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

		$this->c_transactions--;
		if ($this->c_transactions == 0)
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
			$this->c_transactions = 0;
			return $this->db->rollBack();
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

	/**
	 *
	 */
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

			foreach ($data['multilang'] as $lang => $multilangData) {
				$multilangData[$multilangOptions['keyfield']] = $id;
				$multilangData[$multilangOptions['lang']] = $lang;

				$qry = $this->makeQueryForInsert($multilangTable, [$multilangData], $options);
				if (!$qry)
					$this->model->error('Error while generating query for multilang insert');

				if ($options['debug'] and DEBUG_MODE)
					echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

				$this->query($qry, $multilangTable, 'INSERT', $options);
			}

			if (isset($this->cachedTables[$table])) {
				$dataForCache = array_merge($data['data'], count($data['multilang']) > 0 ? reset($data['multilang']) : []);
				$dataForCache[$tableModel->primary] = $id;
			}

			if (array_key_exists($table, $this->options['linked-tables'])) {
				$linked_table = $this->options['linked-tables'][$table]['with'];
				$linkedTableModel = $this->getTable($linked_table);
				$data['custom-data'][$linkedTableModel->primary] = $id;

				$qry = $this->makeQueryForInsert($linked_table, [$data['custom-data']], $options);
				if (!$qry)
					$this->model->error('Error while generating query for custom table insert');

				if ($options['debug'] and DEBUG_MODE)
					echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

				$id = $this->query($qry, $linked_table, 'INSERT', $options);

				if ($data['custom-multilang']) {
					$multilangTable = $this->model->_Multilang->getTableFor($linked_table);
					$multilangOptions = $this->model->_Multilang->getTableOptionsFor($linked_table);
					foreach ($data['custom-multilang'] as $lang => $multilangData)
						$this->checkDbData($multilangTable, $multilangData, $options);

					foreach ($data['custom-multilang'] as $lang => $multilangData) {
						$multilangData[$multilangOptions['keyfield']] = $id;
						$multilangData[$multilangOptions['lang']] = $lang;

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
			$this->model->error('Error while bulk inserting.', '<b>Error:</b> ' . getErr($e), ['details' => '<b>Query:</b> ' . ($qry ?? 'Still undefined')]);
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
	private function makeQueryForInsert(string $table, array $rows, array $options): ?string
	{
		$qry_init = $options['replace'] ? 'REPLACE' : 'INSERT';

		$keys = [];
		$keys_set = false;
		$defaults = null;
		$qry_rows = [];

		$tableModel = $this->getTable($table);

		foreach ($rows as $data) {
			if ($data === []) {
				if ($defaults === null) {
					foreach ($tableModel->columns as $k => $c) {
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

					if ($v === null)
						$values[] = 'NULL';
					else
						$values[] = $this->elaborateValue($v);
				}
				$keys_set = true;

				$qry_rows[] = '(' . implode(',', $values) . ')';
			}
		}

		$qry = null;
		if (count($qry_rows) > 0) {
			if ($keys_set) {
				$qry = $qry_init . ' INTO `' . $this->makeSafe($table) . '`(' . implode(',', $keys) . ') VALUES' . implode(',', $qry_rows);
			} else {
				$qry = $qry_init . ' INTO `' . $this->makeSafe($table) . '` VALUES' . implode(',', $qry_rows);
			}
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
	public function update(string $table, $where, array $data = null, array $options = []): bool
	{
		if (!is_array($data))
			$this->model->error('Error while updating.', '<b>Error:</b> No data array was given!');

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
		if ($where === null)
			return false;

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

		try {
			$this->beginTransaction();

			if ($options['version'] !== null and array_keys($where) === [$tableModel->primary]) {
				$lastVersion = $this->getVersionLock($table, $where[$tableModel->primary]);

				if ($lastVersion > $options['version'])
					$this->model->error('A new version of this element has been saved in the meanwhile. Reload the page and try again.');

				$this->insert('model_version_locks', [
					'table' => $table,
					'row' => $where[$tableModel->primary],
					'version' => $options['version'] + 1,
					'date' => date('Y-m-d H:i:s'),
				]);
			}

			$where_str = $this->makeSqlString($table, $where, ' AND ', ['main_alias' => 't']);
			if (empty($where_str) and !$options['confirm'])
				$this->model->error('Tried to update full table without explicit confirm');

			if ($data['data']) {
				$qry = 'UPDATE `' . $this->makeSafe($table) . '` AS `t` SET ' . $this->makeSqlString($table, $data['data'], ',', ['for_where' => false]) . ($where_str ? ' WHERE ' . $where_str : '');

				if ($options['debug'] and DEBUG_MODE)
					echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

				$this->query($qry, $table, 'UPDATE', $options);
			}

			foreach ($data['multilang'] as $lang => $multilangData) {
				if (!$multilangData)
					continue;

				$ml_where_str = ' WHERE `ml`.`' . $this->makeSafe($multilangOptions['lang']) . '` = ' . $this->db->quote($lang);
				if ($where_str)
					$ml_where_str .= ' AND (' . $where_str . ')';
				$qry = 'UPDATE `' . $this->makeSafe($multilangTable) . '` AS `ml` INNER JOIN `' . $this->makeSafe($table) . '` AS `t` ON `t`.`' . $tableModel->primary . '` = `ml`.`' . $this->makeSafe($multilangOptions['keyfield']) . '` SET ' . $this->makeSqlString($table, $multilangData, ',', ['for_where' => false, 'main_alias' => 'ml']) . $ml_where_str;

				if ($options['debug'] and DEBUG_MODE)
					echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

				$result = $this->query($qry, $table, 'UPDATE', $options);
				if ($result->rowCount() === 0 and isset($where[$tableModel->primary])) { // If there is no multilang row in the db, and I have the row id, I create one
					$multilangWhere = [
						$multilangOptions['keyfield'] => $where[$tableModel->primary],
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
					$qry = 'UPDATE `' . $this->makeSafe($linked_table) . '` AS `c` INNER JOIN `' . $this->makeSafe($table) . '` AS `t` ON `t`.`' . $tableModel->primary . '` = `c`.`' . $linkedTableModel->primary . '` SET ' . $this->makeSqlString($linked_table, $data['custom-data'], ',', ['for_where' => false, 'main_alias' => 'c']) . ($where_str ? ' WHERE ' . $where_str : '');

					if ($options['debug'] and DEBUG_MODE)
						echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

					$this->query($qry, $table, 'UPDATE', $options);
				}

				if ($data['custom-multilang']) {
					$multilangTable = $this->model->_Multilang->getTableFor($linked_table);
					$multilangOptions = $this->model->_Multilang->getTableOptionsFor($linked_table);

					foreach ($data['custom-multilang'] as $lang => $multilangData) {
						if (!$multilangData)
							continue;

						$this->checkDbData($multilangTable, $multilangData, $options);

						$ml_where_str = ' WHERE `ml`.`' . $this->makeSafe($multilangOptions['lang']) . '` = ' . $this->db->quote($lang);
						if ($where_str)
							$ml_where_str .= ' AND (' . $where_str . ')';
						$qry = 'UPDATE `' . $this->makeSafe($multilangTable) . '` AS `ml` INNER JOIN `' . $this->makeSafe($table) . '` AS `t` ON `t`.`' . $tableModel->primary . '` = `ml`.`' . $this->makeSafe($multilangOptions['keyfield']) . '` SET ' . $this->makeSqlString($table, $multilangData, ',', ['for_where' => false, 'main_alias' => 'ml']) . $ml_where_str;

						if ($options['debug'] and DEBUG_MODE)
							echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

						$result = $this->query($qry, $table, 'UPDATE', $options);
						if ($result->rowCount() === 0 and isset($where[$tableModel->primary])) { // If there is no multilang row in the db, and I have the row id, I create one
							$multilangWhere = [
								$multilangOptions['keyfield'] => $where[$tableModel->primary],
								$multilangOptions['lang'] => $lang,
							];
							if ($this->count($multilangTable, $multilangWhere) === 0) // I actually check that the row does not exist (rowCount can return 0 if the updated data are identical to the existing ones)
								$this->insert($multilangTable, array_merge($multilangData, $multilangWhere));
						}
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
	 * @param array|int $where
	 * @param array $data
	 * @param array $options
	 * @return null|int
	 */
	public function updateOrInsert(string $table, $where, array $data = null, array $options = []): ?int
	{
		if (!is_array($data))
			$this->model->error('Error while updating.', '<b>Error:</b> No data array was given!');

		$tableModel = $this->getTable($table);
		if (!is_array($where) and is_numeric($where))
			$where = [$tableModel->primary => $where];

		$check = $this->select($table, $where, [
			'auto_ml' => false,
			'auto-join-linked-tables' => false,
		]);
		if ($check) {
			$this->update($table, $where, $data, $options);
			return $check[$tableModel->primary];
		} else {
			return $this->insert($table, array_merge($where, $data), $options);
		}
	}

	/**
	 * @param string $table
	 * @param mixed $where
	 * @param array $options
	 * @return bool
	 */
	public function delete(string $table, $where = [], array $options = []): bool
	{
		$options = array_merge([
			'confirm' => false,
			'debug' => $this->options['debug'],
			'force' => false,
			'skip-user-filter' => false,
		], $options);

		if (isset($this->deferedInserts[$table]) and !$options['force'])
			$this->model->error('There are open bulk inserts on the table ' . $table . '; can\'t delete');

		$tableModel = $this->getTable($table);
		$where = $this->preliminaryWhereProcessing($tableModel, $where);
		if ($where === null)
			return false;

		$this->addUserFilter($where, $tableModel, $options);

		$this->trigger('delete', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		$where_str = $this->makeSqlString($table, $where, ' AND ');
		$where_str = empty($where_str) ? '' : ' WHERE ' . $where_str;
		if (empty($where_str) and !$options['confirm'])
			$this->model->error('Tried to delete full table without explicit confirm');

		if (in_array($table, $this->options['autoHide'])) $qry = 'UPDATE ' . $this->makeSafe($table) . ' SET zk_deleted = 1' . $where_str;
		else $qry = 'DELETE FROM `' . $this->makeSafe($table) . '`' . $where_str;

		if ($options['debug'] and DEBUG_MODE)
			echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

		try {
			$this->beginTransaction();

			$this->query($qry, $table, 'DELETE', $options);

			if (!in_array($table, $this->options['autoHide']) and array_key_exists($table, $this->options['linked-tables'])) {
				$linked_table = $this->options['linked-tables'][$table]['with'];

				$qry = 'DELETE FROM `' . $this->makeSafe($linked_table) . '` ' . $where_str;
				$this->query($qry, $linked_table, 'DELETE', $options);
			}

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
	 * @param mixed $where
	 * @param array $opt
	 * @return mixed
	 */
	public function select_all(string $table, $where = [], array $opt = [])
	{
		$opt['multiple'] = true;
		return $this->select($table, $where, $opt);
	}

	/**
	 * @param string $table
	 * @param mixed $where
	 * @param array|string $opt
	 * @return mixed
	 */
	public function select(string $table, $where = [], $opt = [])
	{
		if (isset($this->deferedInserts[$table]))
			$this->model->error('There are open bulk inserts on the table ' . $table . '; can\'t read');
		if (!is_array($opt))
			$opt = ['field' => $opt];

		$tableModel = $this->getTable($table);
		$where = $this->preliminaryWhereProcessing($tableModel, $where);
		if ($where === null)
			return false;

		$multilang = $this->model->isLoaded('Multilang') ? $this->model->getModule('Multilang') : false;
		$auto_ml = ($multilang and array_key_exists($table, $multilang->tables)) ? true : false;
		$lang = $multilang ? $multilang->lang : 'it';

		$options = array_merge([
			'multiple' => false,
			'operator' => 'AND',
			'distinct' => false,
			'limit' => false,
			'order_by' => false,
			'group_by' => false,
			'having' => [],
			'auto_ml' => $auto_ml,
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
			$options['joins'][] = [
				'type' => 'LEFT',
				'table' => $table . $ml['suffix'],
				'alias' => 'lang',
				'full_on' => 'lang.`' . $this->makeSafe($ml['keyfield']) . '` = t.`' . $tableModel->primary . '` AND lang.`' . $this->makeSafe($ml['lang']) . '` LIKE ' . $this->db->quote($options['lang']),
				'fields' => $ml['fields'],
			];
		}

		if ($options['auto-join-linked-tables'] and array_key_exists($table, $this->options['linked-tables'])) {
			$customTable = $this->options['linked-tables'][$table]['with'];
			$customTableModel = $this->getTable($this->options['linked-tables'][$table]['with']);

			$customFields = [];
			foreach ($customTableModel->columns as $column_name => $column) {
				if ($column_name === $customTableModel->primary)
					continue;
				$customFields[] = $column_name;
			}

			$options['joins'][] = [
				'type' => 'LEFT',
				'table' => $customTable,
				'alias' => 'custom',
				'on' => $tableModel->primary,
				'join_field' => $customTableModel->primary,
				'fields' => $customFields,
			];

			if ($isMultilang) {
				$ml = $multilang->tables[$customTable];
				$options['joins'][] = [
					'type' => 'LEFT',
					'table' => $customTable . $ml['suffix'],
					'alias' => 'custom_lang',
					'full_on' => 'custom_lang.`' . $this->makeSafe($ml['keyfield']) . '` = t.`' . $customTableModel->primary . '` AND custom_lang.`' . $this->makeSafe($ml['lang']) . '` LIKE ' . $this->db->quote($options['lang']),
					'fields' => $ml['fields'],
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

			if (isset($join['full_fields'])) {
				$sel_str .= ',' . $join['full_fields'];
			} else {
				foreach ($join['fields'] as $nf => $f) {
					if (!is_numeric($nf) and !is_array($f))
						$f = ['field' => $nf, 'as' => $f];

					if (is_array($f) and isset($f['field'], $f['as']))
						$join['fields'][$nf] = $joinAlias . '.' . $this->makeSafe($f['field']) . ' AS ' . $this->makeSafe($f['as']);
					else
						$join['fields'][$nf] = $joinAlias . '.' . $this->makeSafe($f);
				}
				if ($join['fields'])
					$sel_str .= ',' . implode(',', $join['fields']);
			}

			if (isset($join['full_on'])) {
				$join_str .= ' ' . $join['type'] . ' JOIN `' . $this->makeSafe($join['table']) . '` ' . $joinAlias . ' ON (' . $join['full_on'] . ')';
			} else {
				$join_where = array_merge([
					$joinAlias . '.`' . $this->makeSafe($join['join_field']) . '` = t.`' . $this->makeSafe($join['on']) . '`',
				], $join['where']);

				$join_str .= ' ' . $join['type'] . ' JOIN `' . $this->makeSafe($join['table']) . '` ' . $joinAlias . ' ON (' . $this->makeSqlString($table, $join_where, 'AND', ['joins' => $joins]) . ')';
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

		$qry .= ' FROM `' . $this->makeSafe($table) . '` t' . $join_str . $where_str;
		if ($options['group_by']) {
			$qry .= ' GROUP BY ' . ($options['group_by']);
			if ($options['having'])
				$qry .= ' HAVING ' . $this->makeSqlString($table, $options['having'], ' AND ', array_merge($make_options, ['add-alias' => false, 'prefix' => 'zkaggr_']));
		}
		if ($options['order_by'])
			$qry .= ' ORDER BY ' . ($options['order_by']);
		if ($options['limit'])
			$qry .= ' LIMIT ' . ($options['limit']);

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
		} elseif ($options['multiple']) {
			$results = $this->streamResults($table, $options, $q, $isMultilang);
			if ($options['stream'])
				return $results;

			$return = [];
			foreach ($results as $k => $r)
				$return[$k] = $r;
		} else {
			$return = $q->fetch();
			if ($return !== false) {
				if ($isMultilang and $options['fallback'])
					$return = $this->multilangFallback($table, $return, $options);
				$return = $this->normalizeTypesInSelect($table, $return);
			}
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
			$data = $this->normalizeTypesInSelect($table, $r);
			yield $data;
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
		if (!isset($data[$tableModel->primary]))
			return $data;

		if ($this->checkIfValidForFallback($data, $mlTable))
			return $data;

		$where = [
			$tableModel->primary => $data[$tableModel->primary],
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
	 * @param mixed $where
	 * @param array $opt
	 * @return int
	 */
	public function count(string $table, $where = [], array $opt = []): int
	{
		$tableModel = $this->getTable($table);
		$where = $this->preliminaryWhereProcessing($tableModel, $where);
		if ($where === null)
			return false;

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
			$options['joins'][] = [
				'type' => 'LEFT',
				'table' => $table . $ml['suffix'],
				'alias' => 'lang',
				'full_on' => 'lang.`' . $this->makeSafe($ml['keyfield']) . '` = t.`' . $tableModel->primary . '` AND lang.`' . $this->makeSafe($ml['lang']) . '` LIKE ' . $this->db->quote($options['lang']),
				'fields' => $ml['fields'],
			];
		}

		if (array_key_exists($table, $this->options['linked-tables'])) {
			$customTable = $this->options['linked-tables'][$table]['with'];
			$customTableModel = $this->getTable($this->options['linked-tables'][$table]['with']);

			$customFields = [];
			foreach ($customTableModel->columns as $column_name => $column) {
				if ($column_name === $customTableModel->primary)
					continue;
				$customFields[] = $column_name;
			}

			$options['joins'][] = [
				'type' => 'LEFT',
				'table' => $customTable,
				'alias' => 'custom',
				'on' => $tableModel->primary,
				'join_field' => $customTableModel->primary,
				'fields' => $customFields,
			];

			if ($isMultilang) {
				$ml = $multilang->tables[$customTable];
				$options['joins'][] = [
					'type' => 'LEFT',
					'table' => $customTable . $ml['suffix'],
					'alias' => 'custom_lang',
					'full_on' => 'custom_lang.`' . $this->makeSafe($ml['keyfield']) . '` = t.`' . $customTableModel->primary . '` AND custom_lang.`' . $this->makeSafe($ml['lang']) . '` LIKE ' . $this->db->quote($options['lang']),
					'fields' => $ml['fields'],
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
				$join_str .= ' ' . $join['type'] . ' JOIN `' . $this->makeSafe($join['table']) . '` ' . $joinAlias . ' ON (' . $join['full_on'] . ')';
			} else {
				$join_where = array_merge([
					$joinAlias . '.`' . $this->makeSafe($join['join_field']) . '` = t.`' . $this->makeSafe($join['on']) . '`',
				], $join['where']);

				$join_str .= ' ' . $join['type'] . ' JOIN `' . $this->makeSafe($join['table']) . '` ' . $joinAlias . ' ON (' . $this->makeSqlString($table, $join_where, 'AND', ['joins' => $joins]) . ')';
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
		else
			$qry = 'SELECT COUNT(*) ';

		$qry .= 'FROM `' . $this->makeSafe($table) . '` t' . $join_str . $where_str;
		if ($options['group_by'] != false) $qry .= ' GROUP BY ' . ($options['group_by']);
		if ($options['order_by'] != false) $qry .= ' ORDER BY ' . ($options['order_by']);
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

					foreach ($languageVersions as $l => &$r) {
						foreach ($multilangColumns as $k) {
							if (!array_key_exists($k, $r))
								$r[$k] = null;
						}
					}
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
			if (!isset($join['table']) and !isset($join['fields']) and !isset($join['on']) and !isset($join['main_field']) and !isset($join['full_on']))
				$join = ['fields' => $join];
			if (!is_numeric($k) and !isset($join['table']))
				$join['table'] = $k;
			if (!isset($join['where']))
				$join['where'] = [];
			if (!isset($join['table']))
				$this->model->error('Formato join errato');

			$tableModel = $this->getTable($table);
			$joinTableModel = $this->getTable($join['table']);

			if (!isset($join['on'], $join['join_field']) and !isset($join['full_on'])) {
				if ($tableModel === false)
					$this->model->error('Errore durante la lettura dei dati.', 'Durante la lettura da <b>' . $table . '</b> e la join con la tabella <b>' . $join['table'] . '</b>, non sono stati fornite le colonne di aggancio (e non esiste modello per la tabella).');

				if (isset($join['on'])) { // Se sappiamo già quale colonna usare, andiamo a vedere se c'è una FK associata da cui prendere anche la colonna corrispondente nell'altra tabella
					if (!isset($tableModel->columns[$join['on']]))
						$this->model->error('Errore join', 'Sembra non esistere la colonna "' . $join['on'] . '" nella tabella "' . $table . '"!');
					if (!array_key_exists('foreign_key', $tableModel->columns[$join['on']]))
						$this->model->error('Errore join', 'Tipo di modello tabella obsoleto, non esiste anche lettura FK.');
					if (!$tableModel->columns[$join['on']]['foreign_key'])
						$this->model->error('Errore join', 'Nessuna FK sulla colonna "' . $join['on'] . '" della tabella "' . $table . '".');

					$foreign_key = $tableModel->foreign_keys[$tableModel->columns[$join['on']]['foreign_key']];
					if ($foreign_key['ref_table'] != $join['table'])
						$this->model->error('Errore join', 'La colonna "' . $join['on'] . '" della tabella "' . $table . '" punta a una tabella diversa da "' . $join['table'] . '" ("' . $foreign_key['ref_table'] . '").');

					$join['join_field'] = $foreign_key['ref_column'];
				} else { // Altrimenti, cerchiamo di capire quale colonna usare rovistando fra le FK
					$foreign_key = false;
					foreach ($tableModel->foreign_keys as $fk) {
						if ($fk['ref_table'] == $join['table']) {
							if ($foreign_key === false) {
								$foreign_key = $fk;
							} else { // Ambiguo: due foreign key per la stessa tabella, non posso capire quale sia quella giusta
								$this->model->error('Errore join', 'Ci sono due foreign key nella tabella "' . $table . '" che puntano a "' . $join['table'] . '", usare la clausola "on" per specificare quale colonna utilizzare.');
							}
						}
					}

					if ($foreign_key !== false) {
						$join['on'] = $foreign_key['column'];
						$join['join_field'] = $foreign_key['ref_column'];
					} else {
						foreach ($joinTableModel->foreign_keys as $fk) {
							if ($fk['ref_table'] == $table) {
								if ($foreign_key === false) {
									$foreign_key = $fk;
								} else { // Ambiguo: due foreign key per la stessa tabella, non posso capire quale sia quella giusta
									$this->model->error('Errore join', 'Ci sono due foreign key nella tabella "' . $join['table'] . '" che puntano a "' . $table . '", usare le clausole "on"/"join_field" per specificare quali colonne utilizzare.');
								}
							}
						}

						if ($foreign_key) {
							$join['on'] = $foreign_key['ref_column'];
							$join['join_field'] = $foreign_key['column'];
						}
					}

					if ($foreign_key === false)
						$this->model->error('Errore join', 'Non trovo nessuna foreign key che leghi le tabelle "' . $table . '" e "' . $join['table'] . '". Specificare i parametri a mano.');
				}
			}

			if (!isset($join['full_fields'])) {
				if (!isset($join['fields'])) {
					if ($joinTableModel === false)
						$this->model->error('Errore durante la lettura dei dati.', 'Durante la lettura da <b>' . $table . '</b> e la join con la tabella <b>' . $join['table'] . '</b>, non sono stati forniti i campi da prendere da quest\'ultima (e non esiste modello per la tabella).');

					$join['fields'] = [];
					foreach ($joinTableModel->columns as $k_c => $c) {
						if (isset($tableModel->columns[$k_c])) {
							$join['fields'][] = ['field' => $k_c, 'as' => $join['table'] . '_' . $k_c];
						} else {
							$join['fields'][] = $k_c;
						}
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
		if ($options['check'] === false or !$this->getTable($table, false)) // Se è stata disabilitata la verifica dalle opzioni, oppure non esiste file di configurazione per questa tabella, salto la verifica
			return true;

		foreach ($data as $k => $v) {
			if (!array_key_exists($k, $this->getTable($table)->columns)) {
				$this->model->error('Error while writing data.', 'Database column "' . $table . '.' . $k . '" does not exist! (either that or cache needs to be generated)');
			}
			if ($options['checkTypes']) {
				if (!$this->getTable($table)->checkType($k, $v, $options)) {
					$this->model->error('Error while writing data.', 'Data type for column "' . $table . '.' . $k . '" does not match!<br />' . zkdump($v, true, true));
				}
			}
		}

		return true;
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
		if ($this->model->isLoaded('Multilang') and !array_key_exists($customTable, $this->model->_Multilang->tables)) {
			$this->model->_Multilang->tables[$customTable] = [
				'keyfield' => 'parent',
				'lang' => 'lang',
				'suffix' => '_texts',
				'fields' => [],
			];

			$customTableModel = $this->getTable($customTable . '_texts');
			foreach ($customTableModel->columns as $k => $column) {
				if ($k === $customTableModel->primary or $k === 'parent' or $k === 'lang')
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
	 */
	public function makeSafe(string $t): string
	{
		return preg_replace('/[^a-zA-Z0-9_.,()!=<> -]+/', '', $t);
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
		$kr = '`' . $this->makeSafe($k) . '`';

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
	 * @param mixed $v
	 * @return string
	 */
	private function elaborateValue($v): string
	{
		if (is_object($v)) {
			if (get_class($v) == 'DateTime')
				$v = $v->format('Y-m-d H:i:s');
			else
				$this->model->error('Only DateTime objects can be passed as Db values.');
		}
		if (is_array($v)) {
			if (count($v) === 2 and is_numeric($v[0]) and is_numeric($v[1])) {
				return 'POINT(' . $v[0] . ',' . $v[1] . ')';
			} else {
				$this->model->error('Unknown value type');
			}
		}

		return $this->db->quote($v);
	}

	/**
	 * @param Table $tableModel
	 * @param $where
	 * @return array|null
	 */
	private function preliminaryWhereProcessing(Table $tableModel, $where): ?array
	{
		if (is_array($where)) {
			return $where;
		} else {
			if (is_numeric($where))
				$where = [$tableModel->primary => $where];
			elseif (is_string($where))
				$where = [$where];
			else
				return null;

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
					if ($n_elementi < 2 or $n_elementi > 4 or count(array_filter(array_keys($v), 'is_numeric')) < $n_elementi) continue;

					switch ($n_elementi) {
						case 2:
							if (is_numeric($k)) {
								$k = $v[0];
								$operator = '=';
							} else {
								$operator = $v[0];

								if (in_array(trim(strtoupper($operator)), ['IN', 'NOT IN'])) {
									if (!is_array($v[1]))
										$this->model->error('Expected array after a "' . trim($operator) . '" clause');

									$alreadyParsed = true;
									$v[1] = '(' . implode(',', array_map(function ($el) {
											return $this->elaborateValue($el);
										}, $v[1])) . ')';
								}
							}
							$v1 = $v[1];
							break;
						case 3:
							if ($v[0] == 'BETWEEN') {
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
							if ($v[1] != 'BETWEEN')
								continue 2;
							$k = $v[0];
							$operator = $v[1];
							$v1 = $v[2];
							$v2 = $v[3];
							break;
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

			if ($options['prefix'])
				$k = $options['prefix'] . $k;
			$k = $this->elaborateField($k, $options);

			if (!$alreadyParsed) {
				if ($v1 === null) {
					$v1 = 'NULL';
					if ($options['for_where']) {
						if ($operator == '=') $operator = 'IS';
						elseif ($operator == '!=') $operator = 'IS NOT';
					}
				} else {
					$v1 = $this->elaborateValue($v1);
				}
			}

			if ($operator == 'BETWEEN') {
				if ($v2 === null) $v2 = 'NULL';
				else $v2 = $this->elaborateValue($v2);

				$str[] = $k . ' BETWEEN ' . $v1 . ' AND ' . $v2;
			} else
				$str[] = $k . ' ' . $operator . ' ' . $v1;
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

			foreach ($data as $k => $v) {
				if (in_array($k, $this->model->_Multilang->tables[$table]['fields'])) {
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

				if (array_key_exists($table, $this->options['linked-tables'])) {
					$linkedTable = $this->options['linked-tables'][$table]['with'];

					if (in_array($k, $this->model->_Multilang->tables[$linkedTable]['fields'])) {
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
		}

		return [
			'data' => $mainData,
			'multilang' => $multilangData,
			'custom-data' => $customData,
			'custom-multilang' => $customMultilang,
		];
	}

	/**
	 * @param string $table
	 * @return Table|null
	 */
	public function getTable(string $table): ?Table
	{
		if (!isset($this->tables[$table])) {
			if (file_exists(__DIR__ . '/data/' . $this->unique_id . '/' . $table . '.php')) {
				include(__DIR__ . '/data/' . $this->unique_id . '/' . $table . '.php');
				if (!isset($foreign_keys))
					$foreign_keys = [];
				$this->tables[$table] = new Table($table, $table_columns, $foreign_keys);

				if (array_key_exists($table, $this->options['linked-tables'])) {
					$customTableModel = $this->getTable($this->options['linked-tables'][$table]['with']);
					foreach ($customTableModel->columns as $k => $column) {
						if ($k === $customTableModel->primary or isset($this->tables[$table]->columns[$k]))
							continue;
						$column['real'] = false;
						$this->tables[$table]->columns[$k] = $column;
					}

					$this->tables[$table]->foreign_keys = array_merge($this->tables[$table]->foreign_keys, $customTableModel->foreign_keys);
				}
			} else {
				$this->model->error('Can\'t find table model for "' . entities($table) . '" in cache.');
			}
		}
		return $this->tables[$table];
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
