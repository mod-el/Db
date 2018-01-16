<?php namespace Model\Db;

class Table
{
	/** @var string */
	public $primary;
	/** @var array */
	public $columns = array();
	/** @var array */
	public $foreign_keys = array();

	/**
	 * Table constructor.
	 *
	 * @param array $columns
	 * @param array $foreign_keys
	 */
	function __construct(array $columns, array $foreign_keys = [])
	{
		foreach ($columns as $k => $c) {
			if ($c['key'] == 'PRI')
				$this->primary = $k;
			$this->columns[$k] = $c;
		}

		$this->foreign_keys = $foreign_keys;
	}

	/**
	 * Given a column key, checks if it matches a given value
	 *
	 * @param string $k
	 * @param mixed $v
	 * @param array $options
	 * @return bool
	 */
	public function checkType(string $k, $v, array $options): bool
	{
		if (!array_key_exists($k, $this->columns))
			return false;

		if ($v === null) {
			if ($this->columns[$k]['null']) return true;
			else return false;
		}

		switch ($this->columns[$k]['type']) {
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
				break;
			case 'char':
			case 'varchar':
				if ($options['checkLengths']) {
					if (strlen($v) > $this->columns[$k]['length'])
						return false;
				}
				return true;
				break;
			case 'date':
			case 'datetime':
				if (is_object($v) and get_class($v) == 'DateTime')
					$checkData = $v;
				else
					$checkData = date_create($v);
				if (!$checkData)
					return false;
				return true;
				break;
			default:
				return true;
				break;
		}
	}
}
