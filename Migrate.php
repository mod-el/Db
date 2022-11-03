<?php namespace Model\Db;

use Model\Core\Autoloader;

class Migrate
{
	private string $db_name;
	/** @var DbOld */
	private DbOld $db;

	/**
	 * @param string $name
	 * @param DbOld $db
	 */
	function __construct(string $name, DbOld $db)
	{
		$this->db_name = $name;
		$this->db = $db;
	}

	/**
	 *
	 */
	public function exec()
	{
		$migrations = $this->getMigrations();
		$status = $this->getMigrationsStatus();

		foreach ($migrations as $migration) {
			if (!in_array($migration->module . '-' . $migration->version, $status)) {
				if (!$migration->check()) {
					try {
						$migration->up();
					} catch (\Exception $e) {
						if (!$migration->ignoreErrors)
							throw $e;
					}
				}

				$this->db->query('INSERT INTO `model_migrations`(`db`,`module`,`name`,`version`,`date`) VALUES(
	' . $this->db->parseValue($this->db_name) . ',
	' . $this->db->parseValue($migration->module) . ',
	' . $this->db->parseValue($migration->name) . ',
	' . $this->db->parseValue($migration->version) . ',
	' . $this->db->parseValue(date('Y-m-d H:i:s')) . '
)');
			}
		}
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

		$migrations = [];
		try {
			$list = $this->db->query('SELECT * FROM model_migrations WHERE `db` = \'' . $this->db_name . '\' ORDER BY `module`, `version`');
		} catch (\Exception $e) { // Fallback in caso di non esistenza della colonna db - TODO: rimuovere in futuro
			$list = $this->db->query('SELECT * FROM model_migrations ORDER BY `module`, `version`');
		}
		foreach ($list as $migration)
			$migrations[] = $migration['module'] . '-' . str_pad($migration['version'], 14, '0', STR_PAD_LEFT);

		return $migrations;
	}

	/**
	 * @return Migration[]
	 */
	private function getMigrations(): array
	{
		$migrations = [];

		$migration_files = Autoloader::getFilesByType('Migration');
		foreach ($migration_files as $module => $moduleMigrations) {
			if (empty($moduleMigrations))
				continue;

			foreach ($moduleMigrations as $baseClassName => $className) {
				/** @var Migration $migration */
				$migration = new $className($this->db);
				if (
					!$migration->disabled
					and (!$migration->target or $migration->target === $this->db_name)
					and (!$migration->exclude or $migration->exclude !== $this->db_name)
				) {
					$migrations[$module][$baseClassName] = $migration;
				}
			}

			if (isset($migrations[$module]))
				ksort($migrations[$module]);
		}

		uasort($migrations, function ($a, $b) {
			if ($a === 'Db')
				return -1;
			if ($b === 'Db')
				return 1;
			return 0;
		});

		$migrationsDb = [];
		foreach ($migrations['Db'] as $migration)
			$migrationsDb[] = $migration;
		unset($migrations['Db']);

		$othersMigrations = [];
		foreach ($migrations as $moduleMigrations) {
			foreach ($moduleMigrations as $migrationIdx => $migration)
				$othersMigrations[$migrationIdx] = $migration;
		}
		ksort($othersMigrations);
		$othersMigrations = array_values($othersMigrations);

		return array_merge($migrationsDb, $othersMigrations);
	}
}
