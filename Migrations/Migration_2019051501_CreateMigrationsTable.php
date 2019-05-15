<?php namespace Model\Db\Migrations;

use Model\Db\Migration;

class Migration_2019051501_CreateMigrationsTable extends Migration
{
	public function exec()
	{
		$this->createTable('model_migrations');
		$this->addColumn('model_migrations', 'module', ['null' => false]);
		$this->addColumn('model_migrations', 'name', ['null' => false]);
		$this->addColumn('model_migrations', 'version', [
			'type' => 'int',
			'null' => false,
		]);
		$this->addColumn('model_migrations', 'date', [
			'type' => 'datetime',
			'null' => false,
		]);
	}
}
