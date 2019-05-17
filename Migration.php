<?php namespace Model\Db;

abstract class Migration
{
	/** @var Db */
	private $db;
	/** @var string */
	public $migration_name;
	/** @var string */
	public $name;
	/** @var int */
	public $version;
	/** @var array */
	protected $queue = [];

	/**
	 * @param Db $db
	 */
	function __construct(Db $db)
	{
		$this->db = $db;

		$class = explode('\\', get_class($this));
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
	 *
	 */
	public function up()
	{
		$tmp_history = [];
		foreach ($this->queue as $action) {
			try {
				$this->performAction($action['action'], $action['options']);
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
	 * @param array $list
	 */
	protected function reverse(array $list)
	{
		$list = array_reverse($list);
		foreach ($list as $action) {
			$action = $this->getReversedAction($action);
			$this->performAction($action['action'], $action['options']);
		}
	}

	/**
	 * @param string $action
	 * @param array $options
	 */
	protected function performAction(string $action, array $options = [])
	{
		switch ($action) {
			case 'createTable':
				$qry = 'CREATE TABLE `' . $options['table'] . '` (`' . $options['primary'] . '` int(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (`' . $options['primary'] . '`)) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci';
				$this->db->query($qry);
				break;
			case 'dropTable':
				$qry = 'DROP TABLE `' . $options['table'] . '`';
				$this->db->query($qry);
				break;
			case 'addColumn':
				$qry = 'ALTER TABLE `' . $options['table'] . '` ADD COLUMN `' . $options['name'] . '` ' . $options['type'];
				if ($options['unsigned'])
					$qry .= ' unsigned';
				$qry .= $options['null'] ? ' NULL' : ' NOT NULL';
				if ($options['after'])
					$qry .= ' AFTER `' . $options['after'] . '`';
				$this->db->query($qry);
				break;
			case 'dropColumn':
				$qry = 'ALTER TABLE `' . $options['table'] . '` DROP COLUMN `' . $options['name'] . '`';
				$this->db->query($qry);
				break;
			case 'addIndex':
				$qry = 'ALTER TABLE `' . $options['table'] . '` ADD INDEX `' . $options['name'] . '` ';
				$fields = array_map(function ($field) {
					return $this->db->quote($field);
				}, $options['fields']);
				$qry .= '(' . implode(',', $fields) . ')';
				$this->db->query($qry);
				break;
			case 'dropIndex':
				$qry = 'ALTER TABLE `' . $options['table'] . '` DROP INDEX `' . $options['name'] . '`';
				$this->db->query($qry);
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
}
