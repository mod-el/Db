<?php namespace Model\Db;

use Model\Core\Core;

abstract class Migration
{
	/** @var Db */
	private $db;
	/** @var Core */
	protected $model;
	/** @var string */
	public $module;
	/** @var string */
	public $migration_name;
	/** @var string */
	public $name;
	/** @var string */
	public $version;
	/** @var array */
	protected $queue = [];
	/** @var bool */
	public $disabled = false;

	/**
	 * @param Db $db
	 */
	function __construct(Db $db)
	{
		$this->db = $db;
		$this->model = $db->model;

		$class = explode('\\', get_class($this));
		$this->module = count($class) > 3 ? $class[count($class) - 3] : null;
		$this->migration_name = end($class);

		$class = explode('_', end($class));
		$this->version = $class[1];

		preg_match_all('/((?:^|[A-Z])[a-z]+)/', $class[2], $words);
		$this->name = implode(' ', $words[1]);

		$this->exec();
	}

	/**
	 * @return mixed
	 */
	abstract public function exec();

	/**
	 * Brute-checks if the migration was already executed
	 *
	 * @return bool
	 */
	public function check(): bool
	{
		return false;
	}

	/**
	 *
	 */
	public function up()
	{
		$tmp_history = [];
		foreach ($this->queue as $action) {
			try {
				$qry = $this->getQuery($action['action'], $action['options']);
				$this->db->query($qry);
				$tmp_history[] = $action;
			} catch (\Exception $e) {
				try {
					$this->reverse($tmp_history);
				} catch (\Exception $r_e) {
					throw new \Exception("Error while performing a migration, and it was not possible to reverse it either!\nError #1: " . getErr($e) . "\nError #2: " . getErr($r_e));
				}
				throw $e;
			}
		}
	}

	/**
	 *
	 */
	public function down()
	{
		$this->reverse($this->queue);
	}

	/**
	 * @return array
	 */
	public function getSqlQueries(): array
	{
		$queries = [];
		foreach ($this->queue as $action)
			$queries[] = $this->getQuery($action['action'], $action['options']);
		return $queries;
	}

	/**
	 * @param array $list
	 */
	protected function reverse(array $list)
	{
		$list = array_reverse($list);
		foreach ($list as $action) {
			$action = $this->getReversedAction($action);
			$qry = $this->getQuery($action['action'], $action['options']);
			$this->db->query($qry);
		}
	}

	/**
	 * @param string $action
	 * @param array $options
	 */
	protected function getQuery(string $action, array $options = []): string
	{
		switch ($action) {
			case 'query':
				return $options['query'];
				break;
			case 'createTable':
				return 'CREATE TABLE `' . $options['table'] . '` (`' . $options['primary'] . '` int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (`' . $options['primary'] . '`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci';
				break;
			case 'dropTable':
				return 'DROP TABLE `' . $options['table'] . '`';
				break;
			case 'addColumn':
				$qry = 'ALTER TABLE `' . $options['table'] . '` ADD COLUMN `' . $options['name'] . '` ' . $options['type'];
				if ($options['unsigned'])
					$qry .= ' unsigned';
				$qry .= $options['null'] ? ' NULL' : ' NOT NULL';
				if ($options['default'] !== null)
					$qry .= ' DEFAULT ' . $this->db->quote($options['default']);
				if ($options['after'])
					$qry .= ' AFTER `' . $options['after'] . '`';
				return $qry;
				break;
			case 'dropColumn':
				return 'ALTER TABLE `' . $options['table'] . '` DROP COLUMN `' . $options['name'] . '`';
				break;
			case 'changeColumn':
				$qry = 'ALTER TABLE `' . $options['table'] . '` CHANGE COLUMN `' . $options['column'] . '` `' . $options['name'] . '` ' . $options['type'];
				if ($options['unsigned'])
					$qry .= ' unsigned';
				$qry .= $options['null'] ? ' NULL' : ' NOT NULL';
				if ($options['default'] !== null)
					$qry .= ' DEFAULT ' . $this->db->quote($options['default']);
				if ($options['after'])
					$qry .= ' AFTER `' . $options['after'] . '`';
				return $qry;
				break;
			case 'addIndex':
				$qry = 'ALTER TABLE `' . $options['table'] . '` ADD INDEX `' . $options['name'] . '` ';
				$fields = array_map(function ($field) {
					return '`' . $field . '`';
				}, $options['fields']);
				$qry .= '(' . implode(',', $fields) . ')';
				return $qry;
				break;
			case 'dropIndex':
				return 'ALTER TABLE `' . $options['table'] . '` DROP INDEX `' . $options['name'] . '`';
				break;
			case 'addForeignKey':
				return 'ALTER TABLE `' . $options['table'] . '` ADD CONSTRAINT `' . $options['name'] . '` FOREIGN KEY (`' . $options['field'] . '`) REFERENCES `' . $options['ref-table'] . '` (`' . $options['ref-column'] . '`) ON DELETE ' . $options['on-delete'] . ' ON UPDATE ' . $options['on-update'];
				break;
			case 'dropForeignKey':
				return 'ALTER TABLE `' . $options['table'] . '` DROP FOREIGN KEY `' . $options['name'] . '`';
				break;
			default:
				throw new \Exception('Unknown action');
				break;
		}
	}

	/**
	 * @param array $action
	 * @return array
	 */
	protected function getReversedAction(array $action): array
	{
		switch ($action['action']) {
			case 'createTable':
				return [
					'action' => 'dropTable',
					'options' => [
						'table' => $action['options']['table'],
					],
				];
				break;
			case 'addColumn':
				return [
					'action' => 'dropColumn',
					'options' => [
						'table' => $action['options']['table'],
						'name' => $action['options']['name'],
					],
				];
				break;
			case 'addIndex':
				return [
					'action' => 'dropIndex',
					'options' => [
						'table' => $action['options']['table'],
						'name' => $action['options']['name'],
					],
				];
				break;
			case 'addForeignKey':
				return [
					'action' => 'dropForeignKey',
					'options' => [
						'table' => $action['options']['table'],
						'name' => $action['options']['name'],
					],
				];
				break;
			default:
				throw new \Exception('Irreversible action');
				break;
		}
	}

	/* Table planning methods */

	/**
	 * @param string $qry
	 */
	protected function query(string $qry)
	{
		$this->queue[] = [
			'action' => 'query',
			'options' => [
				'query' => $qry,
			],
		];
	}

	/**
	 * @param string $table
	 * @param string $primary
	 */
	protected function createTable(string $table, string $primary = 'id')
	{
		$this->queue[] = [
			'action' => 'createTable',
			'options' => [
				'table' => $table,
				'primary' => $primary,
			],
		];
	}

	/**
	 * @param string $table
	 */
	protected function dropTable(string $table)
	{
		$this->queue[] = [
			'action' => 'dropTable',
			'options' => [
				'table' => $table,
			],
		];
	}

	/**
	 * @param string $table
	 * @param string $name
	 * @param array $options
	 */
	protected function addColumn(string $table, string $name, array $options = [])
	{
		$this->queue[] = [
			'action' => 'addColumn',
			'options' => array_merge([
				'table' => $table,
				'name' => $name,
				'type' => 'VARCHAR(255)',
				'null' => true,
				'unsigned' => false,
				'after' => null,
				'default' => null,
			], $options),
		];
	}

	/**
	 * @param string $table
	 * @param string $column
	 */
	protected function dropColumn(string $table, string $column)
	{
		$this->queue[] = [
			'action' => 'dropColumn',
			'options' => [
				'table' => $table,
				'name' => $column,
			],
		];
	}

	/**
	 * @param string $table
	 * @param string $name
	 * @param array $options
	 */
	protected function changeColumn(string $table, string $name, array $options = [])
	{
		$this->queue[] = [
			'action' => 'changeColumn',
			'options' => array_merge([
				'table' => $table,
				'column' => $name,
				'name' => $name,
				'type' => 'VARCHAR(255)',
				'null' => true,
				'unsigned' => false,
				'after' => null,
				'default' => null,
			], $options),
		];
	}

	/**
	 * @param string $table
	 * @param string $name
	 * @param array $fields
	 * @param array $options
	 */
	protected function addIndex(string $table, string $name, array $fields, array $options = [])
	{
		$this->queue[] = [
			'action' => 'addIndex',
			'options' => array_merge([
				'table' => $table,
				'name' => $name,
				'fields' => $fields,
			], $options),
		];
	}

	/**
	 * @param string $table
	 * @param string $column
	 */
	protected function dropIndex(string $table, string $column)
	{
		$this->queue[] = [
			'action' => 'dropIndex',
			'options' => [
				'table' => $table,
				'name' => $column,
			],
		];
	}

	/**
	 * @param string $table
	 * @param string $name
	 * @param string $field
	 * @param string $refTable
	 * @param string $refColumn
	 * @param array $options
	 */
	protected function addForeignKey(string $table, string $name, string $field, string $refTable, string $refColumn = 'id', array $options = [])
	{
		$options = array_merge([
			'on-update' => 'CASCADE',
			'on-delete' => 'RESTRICT',
		], $options);

		$this->queue[] = [
			'action' => 'addForeignKey',
			'options' => array_merge([
				'table' => $table,
				'name' => $name,
				'field' => $field,
				'ref-table' => $refTable,
				'ref-column' => $refColumn,
			], $options),
		];
	}

	/**
	 * @param string $table
	 * @param string $name
	 */
	protected function dropForeignKey(string $table, string $name)
	{
		$this->queue[] = [
			'action' => 'dropForeignKey',
			'options' => [
				'table' => $table,
				'name' => $name,
			],
		];
	}

	/* Utility methods */

	/**
	 * @param string $table
	 * @return bool
	 */
	protected function tableExists(string $table): bool
	{
		$tables = $this->db->query('SHOW TABLES')->fetchAll();
		$tables = array_map(function ($table) {
			return $table['Tables_in_' . $this->db->name];
		}, $tables);

		return in_array($table, $tables);
	}
}
