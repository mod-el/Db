<?php namespace Model\Db;

class Migrate
{
	/** @var Db */
	private $db;

	/**
	 * @param Db $db
	 */
	function __construct(Db $db)
	{
		$this->db = $db;
	}

	/**
	 *
	 */
	public function exec()
	{
		$status = $this->getMigrationsStatus();
		$migrations = $this->getMigrations();

		foreach ($migrations as $module => $module_migrations) {
			$tmp_history = [];
			foreach ($module_migrations as $migration) {
				if ($migration->version > ($status[$module] ?? 0)) {
					try {
						$migration->up();
						$tmp_history[] = $migration;
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

			foreach ($tmp_history as $migration) {
				$this->db->query('INSERT INTO `model_migrations`(`module`,`name`,`version`,`date`) VALUES(
	' . $this->db->quote($module) . ',
	' . $this->db->quote($migration->name) . ',
	' . $this->db->quote($migration->version) . ',
	' . $this->db->quote(date('Y-m-d H:i:s')) . '
)');
			}
		}
	}

	private function reverse(array $history)
	{
		$history = array_reverse($history);
		foreach ($history as $migration)
			$migration->down();
	}

	/**
	 * @return array
	 */
	private function getMigrationsStatus(): array
	{
		$tables = $this->db->query('SHOW TABLES')->fetchAll();
		$tables = array_map(function ($table) {
			return $table['Tables_in_' . $this->db->name];
		}, $tables);

		if (!in_array('model_migrations', $tables))
			return [];

		$status = [];
		$list = $this->db->query('SELECT * FROM model_migrations ORDER BY `module`, `version`');
		foreach ($list as $migration)
			$status[$migration['module']] = $migration['version'];

		return $status;
	}

	/**
	 * @return Migration[][]
	 */
	private function getMigrations(): array
	{
		$migrations = [];

		$dirs = glob(INCLUDE_PATH . 'model' . DIRECTORY_SEPARATOR . '*');
		foreach ($dirs as $d) {
			if (!is_dir($d . DIRECTORY_SEPARATOR . 'Migrations'))
				continue;

			$module = pathinfo($d, PATHINFO_FILENAME);

			$migrations_files = glob($d . DIRECTORY_SEPARATOR . 'Migrations' . DIRECTORY_SEPARATOR . '*');
			foreach ($migrations_files as $f) {
				$ext = pathinfo($f, PATHINFO_EXTENSION);
				if ($ext !== 'php')
					continue;

				if (!isset($migrations[$module]))
					$migrations[$module] = [];

				$className = 'Model\\Db\\Migrations\\' . pathinfo($f, PATHINFO_FILENAME);
				$migrations[$module][] = new $className($this->db);
			}
		}

		uasort($migrations, function ($a, $b) {
			if ($a === 'Db')
				return -1;
			if ($b === 'Db')
				return 1;
			return 0;
		});

		return $migrations;
	}
}
