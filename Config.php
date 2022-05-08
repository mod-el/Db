<?php namespace Model\Db;

use Model\Cache\Cache;
use Model\Core\Autoloader;
use Model\Core\Module_Config;

class Config extends Module_Config
{
	public $configurable = true;
	public $hasCleanUp = true;

	/**
	 *
	 */
	protected function assetsList()
	{
		$this->addAsset('config');
	}

	/**
	 * Executes migrations
	 *
	 * @return bool
	 */
	public function makeCache(): bool
	{
		$config = $this->retrieveConfig();
		if (!$config or !isset($config['databases']))
			return true;

		foreach ($config['databases'] as $db_name => $db_options) {
			$db = $this->model->load('Db', [
				'db' => $db_name,
			], $db_name);

			if (!isset($db_options['migrate']))
				$db_options['migrate'] = $db_name === 'primary';

			if ($db_options['migrate']) {
				$migrate = new Migrate($db_name, $db);
				$migrate->exec();
			}
		}

		return true;
	}

	/**
	 * Returns the config template
	 *
	 * @param string $type
	 * @return string
	 */
	public function getTemplate(string $type): ?string
	{
		return 'config';
	}

	/**
	 * Save the configuration
	 *
	 * @param string $type
	 * @param array $data
	 * @return bool
	 */
	public function saveConfig(string $type, array $data): bool
	{
		$config = $this->retrieveConfig();

		if (isset($config['databases'])) {
			foreach ($config['databases'] as $idx => $db) {
				if (isset($data['delete-' . $idx]) and $data['delete-' . $idx] == 'yes') { // Config row for this database was deleted
					unset($config['databases'][$idx]);
				} elseif (isset($data[$idx . '-host'])) {
					$config['databases'][$idx]['host'] = $data[$idx . '-host'];
					$config['databases'][$idx]['username'] = $data[$idx . '-username'];
					$config['databases'][$idx]['database'] = $data[$idx . '-database'];

					if ($data[$idx . '-password'])
						$config['databases'][$idx]['password'] = $data[$idx . '-password'];

					if (isset($data[$idx . '-idx']) and $data[$idx . '-idx'] != $idx) { // Idx for this database was changed
						$config['databases'][$data[$idx . '-idx']] = $config['databases'][$idx];
						unset($config['databases'][$idx]);
					}
				}
			}
		}

		if ($data['new-idx'] ?? null) {
			if (!$data['new-host'] or !$data['new-username'] or !$data['new-database'])
				return false;

			$config['databases'][$data['new-idx']] = [
				'host' => $data['new-host'],
				'username' => $data['new-username'],
				'password' => $data['new-password'],
				'database' => $data['new-database'],
			];
		}

		$configFileDir = INCLUDE_PATH . 'app' . DIRECTORY_SEPARATOR . 'config' . DIRECTORY_SEPARATOR . 'Db';
		if (!is_dir($configFileDir))
			mkdir($configFileDir, 0777, true);

		$w = file_put_contents($configFileDir . DIRECTORY_SEPARATOR . 'config.php', '<?php
$config = ' . var_export($config, true) . ';
');
		return (bool)$w;
	}

	/**
	 * First initialization of module
	 *
	 * @param array $data
	 * @return bool
	 */
	public function init(?array $data = null): bool
	{
		if ($data === null or !$this->saveConfig('init', $data))
			return false;

		$this->model->markModuleAsInitialized('Db');

		return true;
	}

	/**
	 * Gets the data for CLI configuration
	 *
	 * @return array
	 */
	public function getConfigData(): ?array
	{
		$keys = [];

		$config = $this->retrieveConfig();

		if (!isset($config['databases']))
			$config['databases'] = [];

		foreach ($config['databases'] as $idx => $db) {
			$keys['delete-' . $idx] = ['label' => 'Delete database ' . $idx . '? Type yes to delete', 'default' => null];
			$keys[$idx . '-host'] = ['label' => 'Host for database ' . $idx, 'default' => $db['host']];
			$keys[$idx . '-username'] = ['label' => 'Username for database ' . $idx, 'default' => $db['username']];
			$keys[$idx . '-password'] = ['label' => 'Password for database ' . $idx, 'default' => $db['password']];
			$keys[$idx . '-database'] = ['label' => 'Database name for database ' . $idx, 'default' => $db['database']];
		}

		$keys['new-idx'] = ['label' => 'Index for a new database (empty to not create one)', 'default' => count($config['databases']) == 0 ? 'primary' : ''];
		$keys['new-host'] = ['label' => 'Host for a new database (if creating one)', 'default' => count($config['databases']) == 0 ? '127.0.0.1' : reset($config['databases'])['host']];
		$keys['new-username'] = ['label' => 'Username for a new database (if creating one)', 'default' => 'root'];
		$keys['new-password'] = ['label' => 'Password for a new database (if creating one)', 'default' => null];
		$keys['new-database'] = ['label' => 'Database name for a new database (if creating one)', 'default' => null];

		return $keys;
	}

	/**
	 * Deletes all the rows in model_version_locks older than 24 hours
	 */
	public function cleanUp()
	{
		$threshold = date_create();
		$threshold->modify('-24 hours');
		$this->model->_Db->delete('model_version_locks', [
			'date' => ['<=', $threshold->format('Y-m-d H:i:s')],
		]);
	}

	public function getFileInstance(string $type, string $file): ?object
	{
		switch ($type) {
			case 'Migration':
				$className = Autoloader::searchFile($type, $file);
				return new $className($this->model->_Db);
				break;
		}

		return null;
	}
}
