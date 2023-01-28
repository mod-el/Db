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

	public array $options = [
		'db' => 'primary',
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

			$this->options = array_merge($configOptions, $this->options);

			$this->name = $this->options['name'];
			$this->unique_id = preg_replace('/[^A-Za-z0-9._-]/', '', $this->options['host'] . '-' . $this->options['name']);
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
		$this->n_query++;
		return $this->getConnection()->query($qry, $table, $type, $options);
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
		return $this->getConnection()->insert($table, $data, $options);
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
		$this->getConnection()->update($table, $where, $data, $options);
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
		return $this->getConnection()->updateOrInsert($table, $where, $data, $options);
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return \PDOStatement|null
	 */
	public function delete(string $table, array|int $where = [], array $options = []): ?\PDOStatement
	{
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
		$response = $this->getConnection()->selectAll($table, $where, $options);

		if (!isset($this->n_tables[$table]))
			$this->n_tables[$table] = 1;
		else
			$this->n_tables[$table]++;

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
		$response = $this->getConnection()->select($table, $where, $options);

		if (!isset($this->n_tables[$table]))
			$this->n_tables[$table] = 1;
		else
			$this->n_tables[$table]++;

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
		$response = $this->getConnection()->count($table, $where, $options);

		if (!isset($this->n_tables[$table . '-count']))
			$this->n_tables[$table . '-count'] = 1;
		else
			$this->n_tables[$table . '-count']++;

		return $response;
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

		if ($options['linked'] and class_exists('\\Model\\LinkedTables\\LinkedTables')) {
			$linked_tables = \Model\LinkedTables\LinkedTables::getTables($this->getConnection());
			foreach ($linked_tables as $linkedTable => $linkedWith) {
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

	/* Dealing with table models */

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
