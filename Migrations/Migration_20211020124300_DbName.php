<?php namespace Model\Db\Migrations;

use Model\Db\Migration;

class Migration_20211020124300_DbName extends Migration
{
	public function exec()
	{
		$this->addColumn('model_migrations', 'db', [
			'after' => 'db',
			'null' => false,
			'default' => 'primary',
		]);
	}
}
