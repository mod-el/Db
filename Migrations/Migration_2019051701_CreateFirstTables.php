<?php namespace Model\Db\Migrations;

use Model\Db\Migration;

class Migration_2019051701_CreateFirstTables extends Migration
{
	public function exec()
	{
		$this->createTable('main_settings');
		$this->addColumn('model_migrations', 'k', ['null' => false]);
		$this->addColumn('model_migrations', 'v', ['type' => 'text']);

		$this->createTable('model_version_locks');
		$this->addColumn('model_migrations', 'table', ['null' => false]);
		$this->addColumn('model_migrations', 'row', ['type' => 'int', 'null' => false]);
		$this->addColumn('model_migrations', 'version', ['type' => 'int', 'null' => false, 'unsigned' => true]);
		$this->addColumn('model_migrations', 'date', ['type' => 'datetime', 'null' => false]);

		$this->addIndex('model_migrations', 'admin_version_locks_idx', [
			'table',
			'row',
		]);

		$this->addIndex('model_migrations', 'model_version_locks_date', [
			'date',
		]);
	}
}
