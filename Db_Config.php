<?php
namespace Model;

class Db_Config extends Module_Config {
	public $configurable = true;

	/**
	 * Caches all the tables structure
	 *
	 * @return bool
	 */
	public function makeCache(){
		$path = INCLUDE_PATH.'model'.DIRECTORY_SEPARATOR.'Db'.DIRECTORY_SEPARATOR;

		if(!file_exists($path))
			$this->model->error('DB module path does not exists!');
		if(!is_writable($path))
			$this->model->error('DB module path is not writable!');

		if(!is_dir($path.'data'))
			mkdir($path.'data');

		$config = $this->retrieveConfig();
		if(!$config or !isset($config['databases']))
			return true;

		foreach($config['databases'] as $db_name=>$db_options) {
			$db = $this->model->load('Db', [
				'db'=>$db_name,
			], $db_name);

			if(!is_dir($path.'data'.DIRECTORY_SEPARATOR.$db->unique_id))
				mkdir($path.'data'.DIRECTORY_SEPARATOR.$db->unique_id);

			$references = array(); // References of each table (via foreign keys) in other tables
			$tables = $db->query('SHOW TABLES');
			foreach($tables as $t){
				$table = $t['Tables_in_'.$db->name];

				/*** COLONNE ***/
				$tq = $db->query('EXPLAIN `'.$table.'`');
				$columns = array();
				foreach($tq as $c){
					if(preg_match('/^enum\(.+\).*$/i', $c['Type'])){
						$type = 'enum';
						$values = explode(',', preg_replace('/^enum\((.+)\).*$/i', '$1', $c['Type']));
						foreach($values as &$v)
							$v = preg_replace('/^\'(.+)\'$/i', '$1', $v);
						unset($v);
						$length = $values;
					}elseif(preg_match('/^.+\([0-9,]+\).*$/i', $c['Type'])){
						$type = strtolower(preg_replace('/^(.+)\([0-9,]+\).*$/i', '\\1', $c['Type']));
						$length = preg_replace('/^.+\(([0-9,]+)\).*$/i', '\\1', $c['Type']);
					}else{
						$type = $c['Type'];
						$length = false;
					}

					$null = $c['Null']=='YES' ? true : false;

					$col = array(
						'type'=>$type,
						'length'=>$length,
						'null'=>$null,
						'key'=>$c['Key'],
						'default'=>$c['Default'],
						'extra'=>$c['Extra'],
						'foreign_key'=>null
					);
					$columns[$c['Field']] = $col;
				}

				/*** FOREIGN KEYS ***/
				$create = $db->query('SHOW CREATE TABLE `'.$table.'`')->fetch();
				$create = $create['Create Table'];
				$righe_query = explode("\n", str_replace("\r", '', $create));
				$foreign_keys = array();
				foreach($righe_query as $r){
					$r = trim($r);
					if(substr($r, 0, 10)=='CONSTRAINT'){
						preg_match_all('/CONSTRAINT `(.+?)` FOREIGN KEY \(`(.+?)`\) REFERENCES `(.+?)` \(`(.+?)`\)/i', $r, $matches, PREG_SET_ORDER);

						if(count($matches[0])!=5)
							continue;
						$fk = $matches[0];

						if(!isset($columns[$fk[2]]))
							echo '<b>Warning:</b> something is wrong, column '.$fk[2].', declared in foreign key '.$fk[1].' doesn\'t seem to exist!<br />';
						$columns[$fk[2]]['foreign_key'] = $fk[1];

						$on_update = 'RESTRICT';
						if(preg_match('/ON UPDATE (NOT NULL|DELETE|CASCADE|NO ACTION)/i', $r, $upd_match))
							$on_update = $upd_match[1];

						$on_delete = 'RESTRICT';
						if(preg_match('/ON DELETE (NOT NULL|DELETE|CASCADE|NO ACTION)/i', $r, $del_match))
							$on_delete = $del_match[1];

						$foreign_keys[$fk[1]] = array(
							'column'=>$fk[2],
							'ref_table'=>$fk[3],
							'ref_column'=>$fk[4],
							'update'=>$on_update,
							'delete'=>$on_delete,
						);

						if(!isset($references[$fk[3]]))
							$references[$fk[3]] = array();
						$references[$fk[3]][] = array(
							'table'=>$table,
							'fk_column'=>$fk[2],
							'column'=>$fk[4],
							'fk'=>$fk[1],
						);
					}
				}

				$scrittura = file_put_contents($path.'data'.DIRECTORY_SEPARATOR.$db->unique_id.'/'.$table.'.php', '<?php
$table_columns = '.var_export($columns, true).';
$foreign_keys = '.var_export($foreign_keys, true).';
');
				if(!$scrittura)
					return false;
			}

			foreach($references as $table=>$ref){
				file_put_contents($path.'data'.DIRECTORY_SEPARATOR.$db->unique_id.'/'.$table.'.php', '$references = '.var_export($ref, true).';', FILE_APPEND);
			}
		}

		return true;
	}

	/**
	 * Returns the config template
	 *
	 * @param array $request
	 * @return string
	 */
	public function getTemplate(array $request){
		return INCLUDE_PATH.'model'.DIRECTORY_SEPARATOR.'Db'.DIRECTORY_SEPARATOR.'templates'.DIRECTORY_SEPARATOR.'config';
	}

	/**
	 * Save the configuration
	 *
	 * @param string $type
	 * @param array $data
	 * @return bool
	 */
	public function saveConfig($type, array $data){
		$config = $this->retrieveConfig();

		if(isset($config['databases'])){
			foreach($config['databases'] as $idx=>$db){
				if(isset($data['delete-'.$idx]) and $data['delete-'.$idx]=='yes'){ // Config row for this database was deleted
					unset($config['databases'][$idx]);
				}else{
					$config['databases'][$idx]['host'] = $data[$idx.'-host'];
					$config['databases'][$idx]['username'] = $data[$idx.'-username'];
					$config['databases'][$idx]['database'] = $data[$idx.'-database'];

					if($data[$idx.'-password'])
						$config['databases'][$idx]['password'] = $data[$idx.'-password'];

					if(isset($data[$idx.'-idx']) and $data[$idx.'-idx']!=$idx){ // Idx for this database was changed
						$config['databases'][$data[$idx.'-idx']] = $config['databases'][$idx];
						unset($config['databases'][$idx]);
					}
				}
			}
		}

		if($data['new-idx'] and !isset($config['databases'][$data['new-idx']])){
			if(!$data['new-host'] or !$data['new-username'] or !$data['new-database'])
				return false;

			$config['databases'][$data['new-idx']] = [
				'host'=>$data['new-host'],
				'username'=>$data['new-username'],
				'password'=>$data['new-password'],
				'database'=>$data['new-database'],
			];
		}

		$configFileDir = INCLUDE_PATH.'app'.DIRECTORY_SEPARATOR.'config'.DIRECTORY_SEPARATOR.'Db';
		if(!is_dir($configFileDir))
			mkdir($configFileDir, 0777, true);

		$w = file_put_contents($configFileDir.DIRECTORY_SEPARATOR.'config.php', '<?php
$config = '.var_export($config, true).';
');
		return (bool) $w;
	}

	/**
	 * First initialization of module
	 *
	 * @param array $data
	 * @return bool
	 */
	public function install(array $data=[]){
		if(!$this->saveConfig('install', $data))
			return false;

		return $this->model->_Db->query('CREATE TABLE IF NOT EXISTS `main_settings` (
		  `id` int(11) NOT NULL AUTO_INCREMENT,
		  `k` varchar(255) COLLATE utf8_unicode_ci NOT NULL,
		  `v` text COLLATE utf8_unicode_ci NULL,
		  PRIMARY KEY (`id`)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci;');
	}

	/**
	 * Gets the data for CLI configuration
	 *
	 * @return array
	 */
	public function getConfigData(){
		$keys = [];

		$config = $this->retrieveConfig();

		if(!isset($config['databases']))
			$config['databases'] = [];

		foreach($config['databases'] as $idx=>$db){
			$keys['delete-'.$idx] = ['label'=>'Delete database '.$idx.'? Type yes to delete', 'default'=>null];
			$keys[$idx.'-host'] = ['label'=>'Host for database '.$idx, 'default'=>$db['host']];
			$keys[$idx.'-username'] = ['label'=>'Username for database '.$idx, 'default'=>$db['username']];
			$keys[$idx.'-password'] = ['label'=>'Password for database '.$idx, 'default'=>$db['password']];
			$keys[$idx.'-database'] = ['label'=>'Database name for database '.$idx, 'default'=>$db['database']];
		}

		$keys['new-idx'] = ['label'=>'Index for a new database (empty to not create one)', 'default'=>count($config['databases'])==0 ? 'primary' : ''];
		$keys['new-host'] = ['label'=>'Host for a new database (if creating one)', 'default'=>count($config['databases'])==0 ? '127.0.0.1' : reset($config['databases'])['host']];
		$keys['new-username'] = ['label'=>'Username for a new database (if creating one)', 'default'=>'root'];
		$keys['new-password'] = ['label'=>'Password for a new database (if creating one)', 'default'=>null];
		$keys['new-database'] = ['label'=>'Database name for a new database (if creating one)', 'default'=>null];

		return $keys;
	}
}