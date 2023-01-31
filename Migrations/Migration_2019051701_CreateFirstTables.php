<?php namespace Model\Db\Migrations;

use Model\Db\Migration;

class Migration_2019051701_CreateFirstTables extends Migration
{
	public function exec()
	{
		$this->createTable('model_version_locks');
		$this->addColumn('model_version_locks', 'table', ['null' => false]);
		$this->addColumn('model_version_locks', 'row', ['type' => 'int', 'null' => false]);
		$this->addColumn('model_version_locks', 'version', ['type' => 'int', 'null' => false, 'unsigned' => true]);
		$this->addColumn('model_version_locks', 'date', ['type' => 'datetime', 'null' => false]);

		$this->addIndex('model_version_locks', 'admin_version_locks_idx', [
			'table',
			'row',
		]);

		$this->addIndex('model_version_locks', 'model_version_locks_date', [
			'date',
		]);
	}

	public function check(): bool
	{
		return $this->tableExists('model_version_locks');
	}
}
