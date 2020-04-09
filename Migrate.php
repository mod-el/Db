<?php namespace Model\Db;

use Model\Core\Autoloader;

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
		$migrations = $this->getMigrations();
		$status = $this->getMigrationsStatus();

		foreach ($migrations as $migration) {
			if (!in_array($migration->module . '-' . $migration->version, $status)) {
				try {
					$this->db->beginTransaction();

					if (!$migration->check())
						$migration->up();

					$this->db->query('INSERT INTO `model_migrations`(`module`,`name`,`version`,`date`) VALUES(
	' . $this->db->quote($migration->module) . ',
	' . $this->db->quote($migration->name) . ',
	' . $this->db->quote($migration->version) . ',
	' . $this->db->quote(date('Y-m-d H:i:s')) . '
)');

					$this->db->commit();
				} catch (\Exception $e) {
					$this->db->rollBack();
					throw $e;
				}
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
		$list = $this->db->query('SELECT * FROM model_migrations ORDER BY `module`, `version`');
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
				$migration = new $className($this->db);
				if (!$migration->disabled)
					$migrations[$module][$baseClassName] = new $className($this->db);
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
		foreach ($migrations as $module => $moduleMigrations) {
			foreach ($moduleMigrations as $migrationIdx => $migration)
				$othersMigrations[$migrationIdx] = $migration;
		}
		ksort($othersMigrations);
		$othersMigrations = array_values($othersMigrations);

		return array_merge($migrationsDb, $othersMigrations);
	}
}
