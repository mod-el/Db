<?php namespace Model\Db\Migrations;

use Model\Db\Migration;

class Migration_20211020124300_DbName extends Migration
{
	public ?bool $ignoreErrors = true;

	public function exec()
	{
		$this->addColumn('model_migrations', 'db', [
			'after' => 'id',
			'null' => false,
			'default' => 'primary',
		]);
	}
}
