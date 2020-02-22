<?php namespace Model\Db\Migrations;

use Model\Db\Migration;

class Migration_20200222234000_ChangeVersionColumn extends Migration
{
	public function exec()
	{
		$this->changeColumn('model_migrations', 'version', [
			'type' => 'char(14)',
			'null' => false,
		]);

		$this->query('UPDATE model_migrations SET version = LPAD(version, 14, \'0\')');

		$this->addIndex('model_migrations', 'module_version', [
			'module',
			'version',
		]);
	}
}
