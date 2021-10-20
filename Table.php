<?php namespace Model\Db;

class Table
{
	public string $name;
	public string $primary;
	public array $columns = [];
	public array $foreign_keys = [];

	/**
	 * Table constructor.
	 *
	 * @param string $name
	 * @param array $columns
	 * @param array $foreign_keys
	 */
	function __construct(string $name, array $columns, array $foreign_keys = [])
	{
		$this->name = $name;

		foreach ($columns as $k => $c) {
			if ($c['key'] == 'PRI')
				$this->primary = $k;

			$c['real'] = true; // For linked tables
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

		if ($v === null)
			return (bool)$this->columns[$k]['null'];

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

			case 'char':
			case 'varchar':
				if ($options['checkLengths']) {
					if (strlen($v) > $this->columns[$k]['length'])
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
}
