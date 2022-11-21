<?php namespace Model\Db;

use Model\Core\Module;
use Model\DbParser\Parser;
use Model\DbParser\Table;

class DbOld extends Module
{
	protected Parser $parser;

	public string $name;
	public string $unique_id;

	public int $n_query = 0;
	public int $n_prepared = 0;
	public array $n_tables = [];

	private int $forcedTenantId;

	public array $options = [
		'db' => 'primary',
		'linked_tables' => [],
		'direct-pdo' => false,
		'debug' => false,
		'use_buffered_query' => true,
		'emulate_prepares' => false,
		'local_infile' => true,
	];

	/**
	 * @param array $options
	 */
	public function init(array $options)
	{
		if ($this->module_id !== '0')
			$this->options['db'] = $this->module_id;

		$this->options = array_merge($this->options, $options);

		if ($this->options['direct-pdo']) {
			$this->unique_id = 'custom';
		} else {
			$config = \Model\Config\Config::get('db');
			if (!$config or !isset($config['databases'][$this->options['db']]))
				throw new \Exception('Missing database configuration for ' . $this->options['db'] . ' database!');

			$configOptions = $config['databases'][$this->options['db']];

			if (class_exists('\\Model\\LinkedTables\\LinkedTables'))
				$this->options['linked_tables'] = \Model\LinkedTables\LinkedTables::getTables($this->getConnection());

			$this->options = array_merge($configOptions, $this->options);

			$this->name = $this->options['name'];
			$this->unique_id = preg_replace('/[^A-Za-z0-9._-]/', '', $this->options['host'] . '-' . $this->options['name']);

			if (!isset($this->options['tenant-filter']))
				$this->options['tenant-filter'] = null;
			if ($this->options['tenant-filter'] and (!is_array($this->options['tenant-filter']) or count($this->options['tenant-filter']) < 2 or !isset($this->options['tenant-filter']['idx'], $this->options['tenant-filter']['column'])))
				$this->options['tenant-filter'] = null;
		}
	}

	/**
	 * @return DbConnection
	 */
	public function getConnection(): DbConnection
	{
		return Db::getConnection($this->options['db']);
	}

	/**
	 * @param string $qry
	 * @return string
	 */
	public function quote(string $qry): string
	{
		return $this->getConnection()->parseValue($qry);
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
		], $options);

		if ($options['log']) {
			$this->trigger('query', [
				'table' => $table,
				'type' => $type,
				'options' => $options,
				'qry' => $qry,
			]);
		}

		$this->n_query++;
		$res = $this->getConnection()->query($qry, $table, $type, $options);
		$return = $res;
		if ($type === 'INSERT') {
			$return = $this->getConnection()->getDb()->lastInsertId();
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
	 * @return bool
	 */
	public function beginTransaction(): bool
	{
		return $this->getConnection()->beginTransaction();
	}

	/**
	 * @return bool
	 */
	public function commit(): bool
	{
		return $this->getConnection()->commit();
	}

	/**
	 * @return bool
	 */
	public function rollBack(): bool
	{
		return $this->getConnection()->rollBack();
	}

	/**
	 * @param int $ignore
	 * @return bool
	 */
	public function inTransaction(int $ignore = 0): bool
	{
		return $this->getConnection()->inTransaction();
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
		$this->trigger('insert', [
			'table' => $table,
			'data' => $data,
			'options' => $options,
		]);

		$id = $this->getConnection()->insert($table, $data, $options);

		$this->trigger('inserted', [
			'table' => $table,
			'id' => $id,
		]);

		return $id;
	}

	/**
	 * @param string $table
	 */
	public function bulkInsert(string $table): void
	{
		$this->getConnection()->bulkInsert($table);
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
			'skip_tenancy' => false,
		], $options);

		$tableModel = $this->getConnection()->getTable($table);
		$where = $this->preliminaryWhereProcessing($tableModel, $where);

		$where = $this->addTenantFilter($where, $tableModel, $options);

		$this->trigger('update', [
			'table' => $table,
			'where' => $where,
			'data' => $data,
			'options' => $options,
		]);

		$data = $this->filterColumns($table, $data);
		$this->checkDbData($table, $data['data'], $options);
		if ($data['multilang']) {
			$multilangTable = \Model\Multilang\Ml::getTableFor($this->getConnection(), $table);
			$multilangOptions = \Model\Multilang\Ml::getTableOptionsFor($this->getConnection(), $table);
			foreach ($data['multilang'] as $multilangData)
				$this->checkDbData($multilangTable, $multilangData, $options);
		}

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

				$ml_where_str = ' WHERE `ml`.' . $this->parseField($multilangOptions['lang_field']) . ' = ' . $this->parseValue($lang);
				if ($where_str)
					$ml_where_str .= ' AND (' . $where_str . ')';
				$qry = 'UPDATE ' . $this->parseField($multilangTable) . ' AS `ml` INNER JOIN ' . $this->parseField($table) . ' AS `t` ON `t`.`' . $tableModel->primary[0] . '` = `ml`.' . $this->parseField($multilangOptions['parent_field']) . ' SET ' . $this->makeSqlString($table, $multilangData, ',', ['for_where' => false, 'main_alias' => 'ml']) . $ml_where_str;

				if ($options['debug'] and DEBUG_MODE)
					echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

				$result = $this->query($qry, $table, 'UPDATE', $options);
				if ($result->rowCount() === 0 and isset($where[$tableModel->primary[0]])) { // If there is no multilang row in the db, and I have the row id, I create one
					$multilangWhere = [
						$multilangOptions['parent_field'] => $where[$tableModel->primary[0]],
						$multilangOptions['lang_field'] => $lang,
					];
					if ($this->count($multilangTable, $multilangWhere) === 0) // I actually check that the row does not exist (rowCount can return 0 if the updated data are identical to the existing ones)
						$this->insert($multilangTable, array_merge($multilangData, $multilangWhere));
				}
			}

			if (array_key_exists($table, $this->options['linked_tables'])) {
				$linked_table = $this->options['linked_tables'][$table];
				$linkedTableModel = $this->getConnection()->getTable($linked_table);

				if ($data['custom-data']) {
					$qry = 'UPDATE ' . $this->parseField($linked_table) . ' AS `c` INNER JOIN ' . $this->parseField($table) . ' AS `t` ON `t`.`' . $tableModel->primary[0] . '` = `c`.`' . $linkedTableModel->primary[0] . '` SET ' . $this->makeSqlString($linked_table, $data['custom-data'], ',', ['for_where' => false, 'main_alias' => 'c']) . ($where_str ? ' WHERE ' . $where_str : '');

					if ($options['debug'] and DEBUG_MODE)
						echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

					$this->query($qry, $table, 'UPDATE', $options);
				}

				if ($data['custom-multilang']) {
					$multilangTableModel = $this->getConnection()->getTable($multilangTable);

					$customMultilangTable = $linked_table . $multilangOptions['table_suffix'];
					$customMultilangModel = $this->getConnection()->getTable($customMultilangTable);

					foreach ($data['custom-multilang'] as $lang => $multilangData) {
						if (!$multilangData)
							continue;

						$this->checkDbData($customMultilangTable, $multilangData, $options);

						$ml_where_str = ' WHERE `ml`.' . $this->parseField($multilangOptions['lang_field']) . ' = ' . $this->parseValue($lang);
						if ($where_str)
							$ml_where_str .= ' AND (' . $where_str . ')';
						$qry = 'UPDATE ' . $this->parseField($customMultilangTable) . ' AS `custom_ml` INNER JOIN ' . $this->parseField($multilangTable) . ' AS `ml` ON `ml`.`' . $multilangTableModel->primary[0] . '` = `custom_ml`.`' . $customMultilangModel->primary[0] . '` INNER JOIN ' . $this->parseField($table) . ' AS `t` ON `t`.`' . $tableModel->primary[0] . '` = `ml`.' . $this->parseField($multilangOptions['parent_field']) . ' SET ' . $this->makeSqlString($customMultilangTable, $multilangData, ',', ['for_where' => false, 'main_alias' => 'custom_ml']) . $ml_where_str;

						if ($options['debug'] and DEBUG_MODE)
							echo '<b>QUERY DEBUG:</b> ' . $qry . '<br />';

						$this->query($qry, $table, 'UPDATE', $options);
					}
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
		$tableModel = $this->getConnection()->getTable($table);
		if (!is_array($where) and is_numeric($where))
			$where = [$tableModel->primary[0] => $where];

		$check = $this->select($table, $where, [
			'auto_ml' => false,
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
	 * @param array|int $where
	 * @param array $options
	 * @return \PDOStatement|null
	 */
	public function delete(string $table, array|int $where = [], array $options = []): ?\PDOStatement
	{
		$this->trigger('delete', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		return $this->getConnection()->delete($table, $where, $options);
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return iterable
	 */
	public function select_all(string $table, array|int $where = [], array $options = []): iterable
	{
		$this->trigger('select', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		$response = $this->getConnection()->selectAll($table, $where, $options);

		if (!isset($this->n_tables[$table]))
			$this->n_tables[$table] = 1;
		else
			$this->n_tables[$table]++;

		$this->trigger('selectResult', ($options['stream'] ?? true) ? [
			'type' => 'stream',
		] : [
			'type' => 'multiple',
			'response' => count($response) . ' rows',
		]);

		return $response;
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return mixed
	 */
	public function select(string $table, array|int $where = [], array $options = []): mixed
	{
		$this->trigger('select', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		$response = $this->getConnection()->select($table, $where, $options);

		if (!isset($this->n_tables[$table]))
			$this->n_tables[$table] = 1;
		else
			$this->n_tables[$table]++;

		$this->trigger('selectResult', [
			'type' => 'row',
			'response' => $response,
		]);

		return $response;
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return int
	 */
	public function count(string $table, array|int $where = [], array $options = []): int
	{
		$this->trigger('count', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		$response = $this->getConnection()->count($table, $where, $options);

		if (!isset($this->n_tables[$table . '-count']))
			$this->n_tables[$table . '-count'] = 1;
		else
			$this->n_tables[$table . '-count']++;

		$this->trigger('countResult', [
			'response' => $response,
		]);

		return $response;
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
		if (!class_exists('\\Model\\Multilang\\Ml'))
			return [];

		$mlTables = \Model\Multilang\Ml::getTables($this->getConnection());

		$languageVersions = [];
		foreach (\Model\Multilang\Ml::getLangs() as $l)
			$languageVersions[$l] = [];

		$tableModel = $this->getConnection()->getTable($table);
		if ($tableModel) {
			$columns = $tableModel->columns;

			if (array_key_exists($table, $mlTables)) {
				$multilangTable = $table . $mlTables[$table]['table_suffix'];
				$multilangTableModel = $this->getConnection()->getTable($multilangTable);
				$multilangColumns = [];
				foreach ($mlTables[$table]['fields'] as $ml) {
					$columns[$ml] = $multilangTableModel->columns[$ml];
					$multilangColumns[] = $ml;
				}

				$langColumn = $mlTables[$table]['lang_field'];

				$fieldsToExtract = $multilangColumns;
				$fieldsToExtract[] = $langColumn;

				if ($id) {
					$languageVersionsQ = $this->select_all($multilangTable, [
						$mlTables[$table]['parent_field'] => $id,
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
						foreach ($mlTables[$table]['fields'] as $f)
							$languageVersions[$lang][$f] = null;
					}
				}
			}
		}

		return $languageVersions;
	}

	/* Utilites for CRUD methods */

	/**
	 * @param array $where
	 * @param Table $tableModel
	 * @param array $options
	 * @return array
	 */
	private function addTenantFilter(array $where, Table $tableModel, array $options): array
	{
		if (
			$this->options['tenant-filter']
			and !$options['skip_tenancy']
			and isset($tableModel->columns[$this->options['tenant-filter']['column']])
			and (!isset($this->options['tenant-filter']['ignore']) or !in_array($tableModel->name, $this->options['tenant-filter']['ignore']))
		) {
			$where[$this->options['tenant-filter']['column']] = $this->getTenantId();
		}

		return $where;
	}

	/**
	 * @return int|null
	 */
	public function getTenantId(): ?int
	{
		if ($this->options['tenant-filter'])
			return $this->forcedTenantId ?? ($this->model->getModule('User', $this->options['tenant-filter']['idx'])->logged() ?: null);
		else
			return null;
	}

	/**
	 * @param int $id
	 * @return void
	 */
	public function setTenantId(int $id): void
	{
		$this->forcedTenantId = $id;
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

		$tableModel = $this->getConnection()->getTable($table);
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
			foreach ($this->options['linked_tables'] as $linkedTable => $linkedWith) {
				if ($linkedTable === $table)
					$ret[] = $linkedWith;
				if ($linkedWith === $table)
					$ret[] = $linkedTable;
			}
		}

		if ($options['multilang'] and class_exists('\\Model\\Multilang\\Ml')) {
			foreach (\Model\Multilang\Ml::getTables($this->getConnection()) as $mlTable => $mlOptions) {
				if ($mlTable === $table)
					$ret[] = $mlTable . $mlOptions['table_suffix'];
				if ($mlTable . $mlOptions['table_suffix'] === $table)
					$ret[] = $mlTable;
			}
		}

		return array_unique($ret);
	}

	/**
	 * @param string $table
	 */
	private function changedTable(string $table)
	{
		$this->getConnection()->changedTable($table);

		$this->trigger('changedTable', [
			'table' => $table,
		]);
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
	 * @param string $f
	 * @return string
	 */
	public function parseField(string $f): string
	{
		return $this->getConnection()->parseColumn($f);
	}

	/**
	 * @param mixed $v
	 * @return string
	 */
	public function parseValue(mixed $v): string
	{
		return $this->getConnection()->parseValue($v);
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

		$tableModel = $this->getConnection()->getTable($table);

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
	 * @return void
	 */
	public function setQueryLimit(string $type, int $n): void
	{
		$this->getConnection()->setQueryLimit($type, $n);
	}

	/* Dealing with table models */

	/**
	 * @param string $table
	 * @param array $data
	 * @return array
	 */
	private function filterColumns(string $table, array $data): array
	{
		$tableModel = $this->getConnection()->getTable($table);

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
				elseif (array_key_exists($table, $this->options['linked_tables']))
					$customData[$k] = $v;
			}
		}

		if (class_exists('\\Model\\Multilang\\Ml')) {
			$mlTables = \Model\Multilang\Ml::getTables($this->getConnection());

			if (array_key_exists($table, $mlTables)) {
				foreach (\Model\Multilang\Ml::getLangs() as $lang) {
					$multilangData[$lang] = [];
					$customMultilang[$lang] = [];
				}

				$mlTableModel = $this->getConnection()->getTable($table . $mlTables[$table]['table_suffix']);

				foreach ($data as $k => $v) {
					if (!isset($mlTableModel->columns[$k]))
						continue;

					if (in_array($k, $mlTables[$table]['fields']) and $mlTableModel->columns[$k]['real']) {
						if (array_key_exists($k, $mainData)) // A field cannot exist both in the main table and in the multilang table
							unset($mainData[$k]);

						if (!is_array($v)) {
							$v = [
								\Model\Multilang\Ml::getLang() => $v,
							];
						}

						foreach ($v as $lang => $subValue) {
							if (!in_array($lang, \Model\Multilang\Ml::getLangs()))
								continue;
							$multilangData[$lang][$k] = $subValue;
						}
					}

					if (array_key_exists($table, $this->options['linked_tables']) and !$mlTableModel->columns[$k]['real']) {
						if (array_key_exists($k, $customData)) // A field cannot exist both in the main table and in the multilang table
							unset($customData[$k]);

						if (!is_array($v)) {
							$v = [
								\Model\Multilang\Ml::getLang() => $v,
							];
						}

						foreach ($v as $lang => $subValue) {
							if (!in_array($lang, \Model\Multilang\Ml::getLangs()))
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
	 * @param string $name
	 * @return Table
	 * @deprecated
	 */
	public function getTable(string $name): Table
	{
		return $this->getConnection()->getTable($name);
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
