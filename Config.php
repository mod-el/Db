<?php namespace Model\Db;

use Model\Core\Autoloader;
use Model\Core\Module_Config;

class Config extends Module_Config
{
	/**
	 * Executes migrations
	 *
	 * @return bool
	 */
	public function makeCache(): bool
	{
		$config = \Model\Config\Config::get('db');
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
	 * Gets the data for CLI configuration
	 *
	 * @return array
	 */
	public function getConfigData(): ?array
	{
		return [];
	}

	public function getFileInstance(string $type, string $file): ?object
	{
		switch ($type) {
			case 'Migration':
				$className = Autoloader::searchFile($type, $file);
				return new $className($this->model->_Db);
		}

		return null;
	}
}
