<?php
namespace Model;

class Db extends Module{
	/** @var string */
	public $name;
	/** @var string */
	public $unique_id;
	/** @var \PDO */
	protected $db;
	/** @var array */
	protected $tables = array();

	/** @var int */
	public $n_query = 0;
	/** @var int */
	public $n_prepared = 0;
	/** @var array  */
	public $n_tables = array();

	/** @var int  */
	protected $c_transactions = 0;

	/** @var array  */
	protected $querylimit_counter = array(
		'query'=>array(),
		'table'=>array()
	);

	/** @var array  */
	protected $options = array(
		'db'=>'primary',
		'listCache'=>[],
		'autoHide'=>[],
		'direct-pdo'=>false,
		'query-limit'=>100,
		'query-limit-table'=>10000,
		'debug'=>false,
	);

	/** @var array  */
	protected $cachedLists = array();
	/** @var array  */
	protected $queryCache = array();

	/**
	 * @param array $options
	 */
	public function init($options){
		$this->options = array_merge($this->options, $options);

		try{
			if($this->options['direct-pdo']){
				$this->db = $this->options['direct-pdo'];
				$this->unique_id = 'custom';
			}else{
				$config = $this->retrieveConfig();
				if(!$config or !isset($config['databases'][$this->options['db']]))
					throw new \Exception('Missing database configuration for '.$options['db'].' database!');

				$this->options = array_merge($config['databases'][$this->options['db']], $this->options);

				$this->db = new \PDO('mysql:host='.$this->options['host'].';dbname='.$this->options['database'].';charset=utf8', $this->options['username'], $this->options['password'], array(\PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION, \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC));
				$this->name = $this->options['database'];
				$this->unique_id = preg_replace('/[^A-Za-z0-9._-]/', '', $this->options['host'].'-'.$this->options['database']);
			}
		}catch(\Exception $e){
			$this->model->error('Error while connecting to database: '.$e->getMessage());
		}

		$this->methods = array(
			'query',
			'insert',
			'update',
			'updateOrInsert',
			'delete',
			'read',
			'select',
			'read_all',
			'select_all',
			'count',
		);
	}

	/**
	 * @param string $name
	 * @param array $arguments
	 * @return mixed
	 */
	function __call($name, $arguments){
		if(method_exists($this->db, $name)){
			return call_user_func_array(array($this->db, $name), $arguments);
		}
		return null;
	}

	/**
	 * @param string $qry
	 * @param string|bool $table
	 * @param string|bool $type
	 * @param array $options
	 * @return \PDOStatement|int
	 */
	public function query($qry, $table=false, $type=false, $options=[]){
		$options = array_merge([
			'log' => true,
			'query-limit' => true,
		], $options);

		if($options['query-limit'] and $this->options['query-limit']>0){
			if(!isset($this->querylimit_counter['query'][$qry]))
				$this->querylimit_counter['query'][$qry] = 0;
			$this->querylimit_counter['query'][$qry]++;

			if($this->querylimit_counter['query'][$qry]>$this->options['query-limit'])
				$this->model->error('Query limit exceeded. - '.$qry);
		}

		if($options['query-limit'] and $this->options['query-limit-table']>0 and $table!==false){
			if(!isset($this->querylimit_counter['table'][$table]))
				$this->querylimit_counter['table'][$table] = 0;
			$this->querylimit_counter['table'][$table]++;

			if($this->querylimit_counter['table'][$table]>$this->options['query-limit-table'])
				$this->model->error('Query limit per table '.$table.' exceeded. - '.$qry);
		}

		if($options['log']){
			$this->trigger('query', [
				'table' => $table,
				'type' => $type,
				'options' => $options,
				'qry' => $qry,
			]);
		}

		$rowId = null;

		$this->n_query++;
		$res = $this->db->query($qry);
		$return = $res;
		if($res and $type=='INSERT'){
			$return = $this->db->lastInsertId();
			$row_id = $return;
		}else{
			$row_id = null;
		}

		if($options['log']) {
			$this->trigger('queryExecuted', [
				'id' => $row_id,
				'rows' => $res->rowCount(),
			]);
		}

		return $return;
	}

	/**
	 * @param string $qry
	 * @param array $options
	 * @return \PDOStatement
	 */
	public function prepare($qry, $options=array()){
		$this->n_prepared++;
		return $this->db->prepare($qry, $options);
	}

	/**
	 * @return bool
	 */
	public function beginTransaction(){
		$res = $this->c_transactions==0 ? $this->db->beginTransaction() : true;
		if($res)
			$this->c_transactions++;
		return $res;
	}

	/**
	 * @return bool
	 */
	public function commit(){
		if($this->c_transactions<=0)
			return false;

		$this->c_transactions--;
		if($this->c_transactions==0)
			return $this->db->commit();
		else
			return true;
	}

	/**
	 * @return bool
	 */
	public function rollBack(){
		if($this->c_transactions>0){
			$this->c_transactions = 0;
			return $this->db->rollBack();
		}
		$this->c_transactions = 0;
		return false;
	}

	/**
	 * @param int $ignore
	 * @return bool
	 */
	public function inTransaction($ignore=0){
		return ($this->c_transactions-$ignore)>0 ? true : false;
	}

	/**
	 *
	 */
	public function terminate(){
		if($this->c_transactions>0)
			$this->rollBack();
	}

	/* CRUD methods */

	/**
	 * @param string $table
	 * @param array|bool $data
	 * @param array $options
	 * @return int
	 */
	public function insert($table, $data=false, $options=array()){
		if(!is_array($data)){
			$this->model->error('Error while inserting.', '<b>Error:</b> No data array was given!');
		}

		$options = array_merge(array(
			'replace'=>false,
			'debug'=>$this->options['debug'],
		), $options);

		$this->trigger('insert', [
			'table' => $table,
			'data' => $data,
			'options' => $options,
		]);

		$this->loadTable($table);
		$data = $this->filterColumns($table, $data);
		$this->checkDbData($table, $data, $options);

		try{
			$qry_init = $options['replace'] ? 'REPLACE' : 'INSERT';

			if($data===array()){
				$tableModel = $this->getTable($table);
				$arrIns = array();
				foreach($tableModel->columns as $k=>$c){
					if($c['null']){
						$arrIns[] = 'NULL';
					}else{
						if($c['key']=='PRI')
							$arrIns[] = 'NULL';
						else
							$arrIns[] = '\'\'';
					}
				}
				$qry = $qry_init.' INTO `'.$this->makeSafe($table).'` VALUES('.implode(',', $arrIns).')';
			}else{
				$keys = array(); $values = array();
				foreach($data as $k => $v){
					$keys[] = $this->elaborateField($table, $k);
					if($v===null) $values[] = 'NULL';
					else $values[] = $this->elaborateValue($v);
				}

				$qry = $qry_init.' INTO `'.$this->makeSafe($table).'`('.implode(',', $keys).') VALUES('.implode(',', $values).')';
			}

			if($options['debug'] and DEBUG_MODE)
				echo '<b>QUERY DEBUG:</b> '.$qry.'<br />';

			$id = $this->query($qry, $table, 'INSERT', $options);

			$this->trigger('inserted', [
				'table' => $table,
				'id' => $id,
			]);

			$this->changedTable($table);

			return $id;
		}catch(\Exception $e){
			$this->model->error('Error while inserting.', '<b>Error:</b> '.getErr($e).'<br /><b>Query:</b> '.$qry);
		}
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array|bool $data
	 * @param array $options
	 * @return bool
	 */
	public function update($table, $where, $data=false, $options=array()){
		if(!is_array($data)){
			$this->model->error('Error while updating.', '<b>Error:</b> No data array was given!');
		}
		if(!is_array($where) and is_numeric($where))
			$where = ['id'=>$where];

		$options = array_merge(array(
			'confirm'=>false,
			'debug'=>$this->options['debug'],
		), $options);

		$this->trigger('update', [
			'table' => $table,
			'where' => $where,
			'data' => $data,
			'options' => $options,
		]);

		$this->loadTable($table);
		$data = $this->filterColumns($table, $data);
		$check = $this->checkDbData($table, $data, $options);
		if(isErr($check))
			return $check;

		if($this->tables[$table]!==false and isset($this->tables[$table]->columns['zkversion'], $data['zkversion'])){
			$prev_versions = $this->model->select_all($table, $where, ['stream'=>true]);
			foreach($prev_versions as $r){
				if($r['zkversion']>$data['zkversion'])
					$this->model->error('A new version of this element has been saved.', ['code'=>'zkversion-mismatch']);
			}
			$data['zkversion']++;
		}

		if(array_keys($data)==array('zkversion')) // Only version number? There is no useful data then
			return true;
		if($data===array())
			return true;

		$where_str = $this->makeSqlString($table, $where, ' AND ');
		$where_str = empty($where_str) ? '' : ' WHERE '.$where_str;
		if(empty($where_str) and !$options['confirm'])
			$this->model->error('Tried to update full table without explicit confirm');

		$qry = 'UPDATE `'.$this->makeSafe($table).'` SET '.$this->makeSqlString($table, $data, ',', array('for_where'=>false)).$where_str;

		if($options['debug'] and DEBUG_MODE)
			echo '<b>QUERY DEBUG:</b> '.$qry.'<br />';

		try{
			$this->query($qry, $table, 'UPDATE', $options);

			$this->changedTable($table);
		}catch(\Exception $e){
			$this->model->error('Error while updating.', '<b>Error:</b> '.getErr($e).'<br /><b>Query:</b> '.$qry);
		}
		return true;
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array|bool $data
	 * @param array $options
	 * @return bool|int
	 */
	public function updateOrInsert($table, $where, $data=false, $options=array()){
		if(!is_array($data)){
			$this->model->error('Error while updating.', '<b>Error:</b> No data array was given!');
		}
		if(!is_array($where) and is_numeric($where))
			$where = ['id'=>$where];

		if($this->count($table, $where)>0){
			return $this->update($table, $where, $data, $options);
		}else{
			return $this->insert($table, array_merge($where, $data), $options);
		}
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $options
	 * @return bool
	 */
	public function delete($table, $where=array(), $options=array()){
		if(!is_array($where) and is_numeric($where)){
			$where = array('id'=>$where);
		}
		$options = array_merge(array(
			'confirm'=>false,
			'debug'=>$this->options['debug'],
		), $options);

		$this->trigger('delete', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		$this->loadTable($table);

		$where_str = $this->makeSqlString($table, $where, ' AND ');
		$where_str = empty($where_str) ? '' : ' WHERE '.$where_str;
		if(empty($where_str) and !$options['confirm'])
			$this->model->error('Tried to delete full table without explicit confirm');

		if(in_array($table, $this->options['autoHide'])) $qry = 'UPDATE '.$this->makeSafe($table).' SET zk_deleted = 1'.$where_str;
		else $qry = 'DELETE FROM `'.$this->makeSafe($table).'`'.$where_str;

		if($options['debug'] and DEBUG_MODE)
			echo '<b>QUERY DEBUG:</b> '.$qry.'<br />';

		try{
			$this->query($qry, $table, 'DELETE', $options);

			$this->changedTable($table);
		}catch(\Exception $e){
			$messaggio = 'Error while deleting';
			$messaggio2 = getErr($e);
			if(stripos($messaggio2, 'a foreign key constraint fails')!==false){
				preg_match_all('/`([^`]+?)`, CONSTRAINT `(.+?)` FOREIGN KEY \(`(.+?)`\) REFERENCES `(.+?)` \(`(.+?)`\)/i', $messaggio2, $matches, PREG_SET_ORDER);

				if(count($matches[0])==6){
					$fk = $matches[0];
					$messaggio = 'Impossibile: si sta tentando di eliminare un record di "<b>'.$fk[4].'</b>" di cui &egrave; presente un riferimento nella tabella "<b>'.$fk[1].'</b>" (sotto la colonna: "<b>'.$fk[3].'</b>")';
				}
			}
			$this->model->error($messaggio, '<b>Error:</b> '.$messaggio2.'<br /><b>Query:</b> '.$qry);
		}
		return true;
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $opt
	 * @return mixed
	 */
	public function select_all($table, $where=array(), $opt=array()){
		$opt['multiple'] = true;
		return $this->select($table, $where, $opt);
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $opt
	 * @return mixed
	 */
	public function select($table, $where=array(), $opt=array()){
		if($where===false or $where===null)
			return false;
		if(!is_array($where) and is_numeric($where))
			$where = array('id'=>$where);
		if(!is_array($opt))
			$opt = array('field'=>$opt);

		$multilang = $this->model->isLoaded('Multilang') ? $this->model->getModule('Multilang') : false;
		$auto_ml = ($multilang and array_key_exists($table, $multilang->tables)) ? true : false;
		$lang = $multilang ? $multilang->lang : 'it';

		$options = array_merge(array(
			'multiple'=>false,
			'operator'=>'AND',
			'distinct'=>false,
			'limit'=>false,
			'order_by'=>false,
			'group_by'=>false,
			'auto_ml'=>$auto_ml,
			'lang'=>$lang,
			'joins'=>array(),
			'field'=>false,
			'max'=>false,
			'sum'=>false,
			'debug'=>$this->options['debug'],
			'return_query'=>false,
			'stream'=>false
		), $opt);
		if($options['multiple']===false and $options['limit']===false)
			$options['limit'] = 1;

		$this->trigger('select', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		$this->loadTable($table);

		if(in_array($table, $this->options['listCache']) and !isset($opt['ignoreCache'])){
			if($this->canUseCache($table, $where, $options)){
				if(!isset($this->cachedLists[$table]))
					$this->cachedLists[$table] = $this->select_all($table, array(), array('ignoreCache'=>true));
				return $this->select_cache($table, $where, $options);
			}
		}

		$sel_str = ''; $join_str = '';
		if($multilang and $options['auto_ml'] and array_key_exists($table, $multilang->tables)){
			$ml = $multilang->tables[$table];
			foreach($ml['fields'] as $nf => $f)
				$ml['fields'][$nf] = 'lang.'.$f;
			if($ml['fields'])
				$sel_str .= ','.implode(',', $ml['fields']);

			if($multilang->options['fallback']){
				$join_str .= ' LEFT OUTER JOIN `'.$table.$ml['suffix'].'` AS lang ON lang.`'.$this->makeSafe($ml['keyfield']).'` = t.`id` AND lang.`'.$this->makeSafe($ml['lang']).'` LIKE (CASE WHEN '.$this->db->quote($options['lang']).' IN (SELECT `'.$this->makeSafe($ml['lang']).'` FROM `'.$table.$ml['suffix'].'` WHERE `'.$this->makeSafe($ml['keyfield']).'` = t.`id`) THEN '.$this->db->quote($options['lang']).' ELSE '.$this->db->quote($multilang->options['fallback']).' END)';
			}else{
				$join_str .= ' LEFT OUTER JOIN `'.$table.$ml['suffix'].'` AS lang ON lang.`'.$this->makeSafe($ml['keyfield']).'` = t.`id` AND lang.`'.$this->makeSafe($ml['lang']).'` LIKE '.$this->db->quote($options['lang']);
			}
		}

		$joins = $this->elaborateJoins($table, $options['joins']);

		$cj = 0;
		foreach($joins as $join){
			if(isset($join['full_fields'])){
				$sel_str .= ','.$join['full_fields'];
			}else {
				foreach ($join['fields'] as $nf => $f) {
					if (!is_numeric($nf) and !is_array($f))
						$f = array('field' => $nf, 'as' => $f);

					if (is_array($f) and isset($f['field'], $f['as']))
						$join['fields'][$nf] = 'j' . $cj . '.' . $this->makeSafe($f['field']) . ' AS ' . $this->makeSafe($f['as']);
					else
						$join['fields'][$nf] = 'j' . $cj . '.' . $this->makeSafe($f);
				}
				if($join['fields'])
					$sel_str .= ',' . implode(',', $join['fields']);
			}

			if(!isset($join['type']))
				$join['type'] = 'INNER';

			if(isset($join['full_on'])){
				$join_str .= ' '.$join['type'].' JOIN `'.$this->makeSafe($join['table']).'` j'.$cj.' ON '.$join['full_on'];
			}else{
				$join_where = array_merge([
					'j'.$cj.'.`'.$this->makeSafe($join['join_field']).'` = t.`'.$this->makeSafe($join['on']).'`',
				], $join['where']);

				$join_str .= ' '.$join['type'].' JOIN `'.$this->makeSafe($join['table']).'` j'.$cj.' ON ('.$this->makeSqlString($table, $join_where, 'AND', ['joins'=>$joins]).')';
			}

			$cj++;
		}

		$make_options = array('auto_ml'=>$options['auto_ml'], 'main_alias'=>'t', 'joins'=>$joins);
		$where_str = $this->makeSqlString($table, $where, ' '.$options['operator'].' ', $make_options);
		if(in_array($table, $this->options['autoHide']))
			$where_str = empty($where_str) ? 't.zk_deleted = 0' : '('.$where_str.') AND t.zk_deleted = 0';
		$where_str = empty($where_str) ? '' : ' WHERE '.$where_str;

		if($options['distinct'])
			$qry = 'SELECT DISTINCT '.$this->elaborateField($table, $options['distinct'], $make_options).',';
		else
			$qry = 'SELECT ';

		if($options['max']!==false){
			$qry .= 'MAX('.$this->elaborateField($table, $options['max'], $make_options).')';
		}elseif($options['sum']!==false){
			$qry .= 'SUM('.$this->elaborateField($table, $options['sum'], $make_options).')';
		}elseif($options['field']!==false){
			$qry .= $this->elaborateField($table, $options['field'], $make_options);
		}else{
			$qry .= 't.*'.$sel_str;
		}

		$qry .= ' FROM `'.$this->makeSafe($table).'` t'.$join_str.$where_str;
		if($options['group_by']!=false) $qry .= ' GROUP BY '.($options['group_by']);
		if($options['order_by']!=false) $qry .= ' ORDER BY '.($options['order_by']);
		if($options['limit']!=false) $qry .= ' LIMIT '.($options['limit']);

		if($options['return_query'])
			return $qry;

		if($options['debug'] and DEBUG_MODE)
			echo '<b>QUERY DEBUG:</b> '.$qry.'<br />';

		$cacheKey = md5($qry.((string) $options['field']).((string) $options['max']).((string) $options['sum']).((int) $options['multiple']));
		if(isset($this->queryCache[$table][$cacheKey])){
			if($this->queryCache[$table][$cacheKey]['query']==$qry)
				return $this->queryCache[$table][$cacheKey]['res'];
			else
				unset($this->queryCache[$table][$cacheKey]);
		}

		if(!isset($this->n_tables[$table]))
			$this->n_tables[$table] = 1;
		else
			$this->n_tables[$table]++;

		try{
			$q = $this->query($qry, $table, 'SELECT', $options);
		}catch(\Exception $e){
			$this->model->error('Error while reading.', '<b>Errore:</b> '.getErr($e).'<br /><b>Query:</b> '.$qry);
		}

		if($options['field']!==false or $options['max']!==false or $options['sum']!==false){
			$return = array($options['field']=>$q->fetchColumn());
			$return = $this->normalizeTypesInSelect($table, $return);
			$return = $return[$options['field']];
		}elseif($options['multiple']){
			if($options['stream'])
				return $q;

			$return = $q->fetchAll();
			foreach($return as $k=>$riga)
				$return[$k] = $this->normalizeTypesInSelect($table, $riga);
		}else{
			if($options['stream'])
				return $q;

			$return = $this->normalizeTypesInSelect($table, $q->fetch());
		}

		$this->queryCache[$table][$cacheKey] = [
			'query'=>$qry,
			'res'=>$return,
		];

		return $return;
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $opt
	 * @return int
	 */
	public function count($table, $where=array(), $opt=array()){
		if($where===false or $where===null)
			return false;
		if(!is_array($where) and is_numeric($where))
			$where = ['id'=>$where];
		$multilang = $this->model->isLoaded('Multilang') ? $this->model->getModule('Multilang') : false;
		$auto_ml = ($multilang and array_key_exists($table, $multilang->tables)) ? true : false;
		$lang = $multilang ? $multilang->lang : 'it';

		$options = array(
			'multiple'=>true,
			'operator'=>'AND',
			'distinct'=>false,
			'limit'=>false,
			'joins'=>array(),
			'order_by'=>false,
			'group_by'=>false,
			'auto_ml'=>$auto_ml,
			'lang'=>$lang,
			'field'=>false,
			'debug'=>$this->options['debug'],
			'return_query'=>false,
		);
		$options = array_merge($options, $opt);

		$this->trigger('count', [
			'table' => $table,
			'where' => $where,
			'options' => $options,
		]);

		if(in_array($table, $this->options['listCache']) and !isset($opt['ignoreCache'])){
			if($this->canUseCache($table, $where, $options)){
				if(!isset($this->cachedLists[$table]))
					$this->cachedLists[$table] = $this->select_all($table, array(), array('ignoreCache'=>true));
				return count($this->select_cache($table, $where, $options));
			}
		}

		$join_str = '';

		if($multilang and $options['auto_ml'] and array_key_exists($table, $multilang->tables)){
			$ml = $multilang->tables[$table];
			$join_str .= ' LEFT OUTER JOIN `'.$table.$ml['suffix'].'` lang ON lang.`'.$this->makeSafe($ml['keyfield']).'` = t.id AND lang.`'.$this->makeSafe($ml['lang']).'` LIKE '.$this->db->quote($options['lang']);
		}

		$joins = $this->elaborateJoins($table, $options['joins']);

		$cj = 0;
		foreach($joins as $join){
			if(!isset($join['type'])) $join['type'] = 'INNER';

			if(isset($join['full_on'])){
				$join_str .= ' '.$join['type'].' JOIN `'.$this->makeSafe($join['table']).'` j'.$cj.' ON '.$join['full_on'];
			}else {
				$join_where = array_merge([
					'j'.$cj.'.`'.$this->makeSafe($join['join_field']).'` = t.`'.$this->makeSafe($join['on']).'`',
				], $join['where']);

				$join_str .= ' '.$join['type'].' JOIN `'.$this->makeSafe($join['table']).'` j'.$cj.' ON ('.$this->makeSqlString($table, $join_where, 'AND', ['joins'=>$joins]).')';
			}
			$cj++;
		}

		$make_options = array('main_alias'=>'t', 'joins'=>$joins, 'auto_ml'=>$options['auto_ml']);
		$where_str = $this->makeSqlString($table, $where, ' '.$options['operator'].' ', $make_options);

		if(in_array($table, $this->options['autoHide']))
			$where_str = empty($where_str) ? 'zk_deleted = 0' : '('.$where_str.') AND zk_deleted = 0';
		$where_str = empty($where_str) ? '' : ' WHERE '.$where_str;

		if($options['distinct'])
			$qry = 'SELECT COUNT(DISTINCT '.$this->elaborateField($table, $options['distinct'], $make_options).') ';
		else
			$qry = 'SELECT COUNT(*) ';

		$qry .= 'FROM `'.$this->makeSafe($table).'` t'.$join_str.$where_str;
		if($options['group_by']!=false) $qry .= ' GROUP BY '.($options['group_by']);
		if($options['order_by']!=false) $qry .= ' ORDER BY '.($options['order_by']);
		if($options['limit']!=false) $qry .= ' LIMIT '.($options['limit']);

		if($options['return_query'])
			return $qry;

		if($options['debug'] and DEBUG_MODE)
			echo '<b>QUERY DEBUG:</b> '.$qry.'<br />';

		$cacheKey = md5($qry);
		if(isset($this->queryCache[$table][$cacheKey])){
			if($this->queryCache[$table][$cacheKey]['query']==$qry)
				return $this->queryCache[$table][$cacheKey]['res'];
			else
				unset($this->queryCache[$table][$cacheKey]);
		}

		if(!isset($this->n_tables[$table.'-count']))
			$this->n_tables[$table.'-count'] = 1;
		else
			$this->n_tables[$table.'-count']++;

		try{
			$q = $this->query($qry, $table, 'COUNT');
		}catch(\Exception $e){
			$this->model->error('Errore durante la lettura dei dati.', '<b>Errore:</b> '.$e->getMessage().'<br /><b>Query:</b> '.$qry);
		}

		$return = (int) $q->fetchColumn();

		$this->queryCache[$table][$cacheKey] = [
			'query'=>$qry,
			'res'=>$return,
		];

		return $return;
	}

	/* Utilites for CRUD methods */

	/**
	 * @param string $table
	 * @param mixed $riga
	 * @return array
	 */
	private function normalizeTypesInSelect($table, $riga){  // Al momento agisce solo sui campi decimal, per trasformarli in float effettivi, ma può essere ampliata in futuro
		if(!is_array($riga))
			return $riga;
		if(!isset($this->tables[$table]) or !$this->tables[$table])
			return $riga;

		foreach($riga as $k=>$v){
			if($v===null or $v===false)
				continue;
			if(array_key_exists($k, $this->tables[$table]->columns)){
				if(in_array($this->tables[$table]->columns[$k]['type'], ['double', 'float', 'decimal']))
					$riga[$k] = (float) $v;
				if(in_array($this->tables[$table]->columns[$k]['type'], ['tinyint', 'smallint', 'mediumint', 'int', 'bigint', 'year']))
					$riga[$k] = (int) $v;
			}
		}
		return $riga;
	}

	/**
	 * @param string $table
	 * @param array $joins
	 * @return array
	 */
	private function elaborateJoins($table, $joins){
		/*
		Formati possibili per un join:

		//Se non si ha il modello della tabella (e relative foreign keys) è obbligatorio specificare i campi da prendere, "on" e "join_field"
		'nome tabella'
		'nome tabella'=>['campo1', 'campo2']
		['table'=>'nome tabella', 'fields'=>['campo1'=>'altra_tabella_campo', 'campo2']]
		['table'=>'nome tabella', 'on'=>'campo tabella principale', 'join_field'=>'campo tabella della join', 'fields'=>['campo1', 'campo2']]
		*/

		$return = array();
		foreach($joins as $k => $join){
			if(!is_array($join))
				$join = array('table'=>$join);
			if(!isset($join['table']) and !isset($join['fields']) and !isset($join['on']) and !isset($join['main_field']) and !isset($join['full_on']))
				$join = array('fields'=>$join);
			if(!is_numeric($k) and !isset($join['table']))
				$join['table'] = $k;
			if(!isset($join['where']))
				$join['where'] = array();

			$tableModel = $this->getTable($table);
			if(!isset($join['on'], $join['join_field']) and !isset($join['full_on'])){
				if($tableModel===false)
					$this->model->error('Errore durante la lettura dei dati.', 'Durante la lettura da <b>'.$table.'</b> e la join con la tabella <b>'.$join['table'].'</b>, non sono stati fornite le colonne di aggancio (e non esiste modello per la tabella).');

				if(isset($join['on'])){ // Se sappiamo già quale colonna usare, andiamo a vedere se c'è una FK associata da cui prendere anche la colonna corrispondente nell'altra tabella
					if(!isset($tableModel->columns[$join['on']]))
						$this->model->error('Errore join', 'Sembra non esistere la colonna "'.$join['on'].'" nella tabella "'.$table.'"!');
					if(!array_key_exists('foreign_key', $tableModel->columns[$join['on']]))
						$this->model->error('Errore join', 'Tipo di modello tabella obsoleto, non esiste anche lettura FK.');
					if(!$tableModel->columns[$join['on']]['foreign_key'])
						$this->model->error('Errore join', 'Nessuna FK sulla colonna "'.$join['on'].'" della tabella "'.$table.'".');

					$foreign_key = $tableModel->foreign_keys[$tableModel->columns[$join['on']]['foreign_key']];
					if($foreign_key['ref_table']!=$join['table'])
						$this->model->error('Errore join', 'La colonna "'.$join['on'].'" della tabella "'.$table.'" punta a una tabella diversa da "'.$join['table'].'" ("'.$foreign_key['ref_table'].'").');

					$join['join_field'] = $foreign_key['ref_column'];
				}else{ // Altrimenti, cerchiamo di capire quale colonna usare rovistando fra le FK
					$foreign_key = false;
					foreach($tableModel->foreign_keys as $k=>$fk){
						if($fk['ref_table']==$join['table']){
							if($foreign_key===false){
								$foreign_key = $fk;
							}else{ // Ambiguo: due foreign key per la stessa tabella, non posso capire quale sia quella giusta
								$this->model->error('Errore join', 'Ci sono due foreign key nella tabella "'.$table.'" che puntano a "'.$join['table'].'", usare la clausola "on" per specificare quale colonna utilizzare.');
							}
						}
					}

					if($foreign_key===false)
						$this->model->error('Errore join', 'Non trovo nessuna foreign key nella tabella "'.$table.'" che punti a "'.$join['table'].'". Specificare i parametri a mano.');

					$join['on'] = $foreign_key['column'];
					$join['join_field'] = $foreign_key['ref_column'];
				}
			}

			if(!isset($join['full_fields'])) {
				if (!isset($join['fields'])) {
					$joinTableModel = $this->getTable($join['table']);
					if ($joinTableModel === false)
						$this->model->error('Errore durante la lettura dei dati.', 'Durante la lettura da <b>' . $table . '</b> e la join con la tabella <b>' . $join['table'] . '</b>, non sono stati forniti i campi da prendere da quest\'ultima (e non esiste modello per la tabella).');

					$join['fields'] = array();
					foreach ($joinTableModel->columns as $k => $c) {
						if (isset($tableModel->columns[$k])) {
							$join['fields'][] = array('field' => $k, 'as' => $join['table'] . '_' . $k);
						} else {
							$join['fields'][] = $k;
						}
					}
				}

				if (!is_array($join['fields']))
					$join['fields'] = array($join['fields']);
			}

			$return[] = $join;
		}

		return $return;
	}

	/**
	 * @param string $table
	 * @param array $data
	 * @param array $options
	 * @return bool
	 */
	private function checkDbData($table, $data, $options=array()){
		$options = array_merge(array('check'=>true, 'checkTypes'=>true, 'checkLengths'=>false), $options);
		if($options['check']===false or $this->tables[$table]===false) // Se è stata disabilitata la verifica dalle opzioni, oppure non esiste file di configurazione per questa tabella, salto la verifica
			return true;

		foreach($data as $k=>$v){
			if(!array_key_exists($k, $this->tables[$table]->columns)){
				$this->model->error('Error while writing data.', 'Database column "'.$table.'.'.$k.'" does not exist! (either that or cache needs to be generated)');
			}
			if($options['checkTypes']){
				if(!$this->tables[$table]->checkType($k, $v, $options)){
					$this->model->error('Error while writing data.', 'Data type for column "'.$table.'.'.$k.'" does not match!<br />'.zkdump($v, true, true));
				}
			}
		}

		return true;
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $opt
	 * @return bool
	 */
	private function canUseCache($table, $where=array(), $opt=array()){
		if(!in_array($table, $this->options['listCache'])) return false;
		if(!is_array($where)) return false;
		foreach($where as $k=>$v){
			if(is_numeric($k) and is_string($v))
				return false;
			if(is_array($v))
				return false;
		}
		if(!in_array($opt['operator'], array('AND', 'OR'))) return false;
		if($opt['order_by']!==false){
			$ordinamento = str_word_count($opt['order_by'], 1, '0123456789_');
			if(count($ordinamento)>2) return false;
			if(count($ordinamento)==2 and !in_array(strtolower($ordinamento[1]), array('asc', 'desc'))) return false;
		}

		$multilang = $this->model->isLoaded('Multilang') ? $this->model->getModule('Multilang') : false;
		$lang = $multilang ? $multilang->lang : 'it';

		if($opt['lang']!==$lang) return false;
		if(!empty($opt['joins'])) return false;
		if($opt['limit']){
			if(!preg_match('/^[0-9]+(,[0-9+])?$/', $opt['limit']))
				return false;
		}
		return true;
	}

	/**
	 * @param string $table
	 * @param array $where
	 * @param array $opt
	 * @return array|bool
	 */
	private function select_cache($table, $where=array(), $opt=array()){
		if(!isset($this->n_tables[$table.'-cache']))
			$this->n_tables[$table.'-cache'] = 1;
		else
			$this->n_tables[$table.'-cache']++;

		$results = array();
		foreach($this->cachedLists[$table] as $row){
			if(empty($where))
				$verified = true;
			else{
				switch($opt['operator']){
					case 'AND':
						$verified = true;
						foreach($where as $k=>$v)
							if((string) $row[$k]!=(string) $v) $verified = false;
						break;
					case 'OR':
						$verified = false;
						foreach($where as $k=>$v)
							if((string) $row[$k]==(string) $v) $verified = true;
						break;
				}
			}
			if($verified){
				if($opt['multiple']){
					if(isset($row['id'])) $results[$row['id']] = $row;
					else $results[] = $row;
				}else{
					if($opt['field']) return $row[$opt['field']];
					else return $row;
				}
			}
		}
		if($opt['multiple']){
			if($opt['order_by']){
				$ordinamento = str_word_count($opt['order_by'], 1, '0123456789_');
				if(count($ordinamento)==1) $ordinamento = array($ordinamento[0], 'ASC');
				$ordinamento0 = $ordinamento[0];
				$ordinamento1 = strtoupper($ordinamento[1]);

				uasort($results, function($a, $b) use($ordinamento0, $ordinamento1){
					if($a[$ordinamento0]==$b[$ordinamento0]) return 0;
					if(is_numeric($a[$ordinamento0]) and is_numeric($b[$ordinamento0])){
						switch($ordinamento1){
							case 'DESC': return $a[$ordinamento0]<$b[$ordinamento0] ? 1 : -1; break;
							default: return $a[$ordinamento0]>$b[$ordinamento0] ? 1 : -1; break;
						}
					}else{
						$cmp = strcasecmp($a[$ordinamento0], $b[$ordinamento0]);
						if($ordinamento1=='DESC') $cmp *= -1;
						return $cmp;
					}
				});
			}
			if($opt['limit']){
				if(is_numeric($opt['limit'])) return array_slice($results, 0, $opt['limit']);
				else{
					$limit = explode(',', $opt['limit']);
					return array_slice($results, $limit[0], $limit[1]);
				}
			}else return $results;
		}else return false;
	}

	/**
	 * @param string $table
	 */
	private function changedTable($table){
		if(in_array($table, $this->options['listCache']) and isset($this->cachedLists[$table]))
			unset($this->cachedLists[$table]);
		if(isset($this->queryCache[$table]))
			$this->queryCache[$table] = array();

		$this->trigger('changedTable', [
			'table' => $table,
		]);
	}

	/**
	 * @param string $t
	 * @return string
	 */
	private function makeSafe($t){
		return preg_replace('/[^a-zA-Z0-9_.,()!=<> -]+/', '', $t);
	}

	/**
	 * @param string $table
	 * @param string $k
	 * @param array $opt
	 * @return string
	 */
	private function elaborateField($table, $k, $opt=array()){
		$options = array_merge(array('auto_ml'=>false, 'main_alias'=>false, 'joins'=>array()), $opt);
		$kr = '`'.$this->makeSafe($k).'`';

		$multilang = $this->model->isLoaded('Multilang') ? $this->model->getModule('Multilang') : false;

		$changed = false;
		if($multilang and $options['auto_ml'] and array_key_exists($table, $multilang->tables)){
			$ml = $multilang->tables[$table];
			if(in_array($k, $ml['fields'])){
				$kr = 'lang.'.$kr;
				$changed = true;
			}
		}

		$cj = 0;
		foreach($options['joins'] as $join){
			if(!isset($join['full_fields'])){
				if(!is_array($join['fields']))
					$join['fields'] = array($join['fields']);
				foreach($join['fields'] as $nf => $f){
					if(is_array($f) and isset($f['as']))
						$ff = $f['as'];
					else
						$ff = $f;

					if($ff==$k){
						if(!is_array($f) and is_numeric($nf)){
							$kr = 'j'.$cj.'.'.$kr;
						}else{
							if(is_array($f)) $kr = 'j'.$cj.'.'.$f['field'];
							else $kr = 'j'.$cj.'.'.$nf;
						}
						$changed = true;
					}
				}
			}
			$cj++;
		}

		if(!$changed and $options['main_alias'])
			$kr = $options['main_alias'].'.'.$kr;

		return $kr;
	}

	/**
	 * @param mixed $v
	 * @return string
	 */
	private function elaborateValue($v){
		if(is_object($v)){
			if(get_class($v)=='DateTime') $v = $v->format('Y-m-d H:i:s');
			else throw $this->model->error('Errore Core: Errore nella tipologia di dato.', 'Tipo di oggetto non riconosciuto.');
		}

		return $this->db->quote($v);
	}

	/**
	 * @param string $table
	 * @param mixed $array
	 * @param string $collante
	 * @param array $opt
	 * @return string
	 */
	public function makeSqlString($table, $array, $collante, $opt=array()){
		if(is_string($array)){
			return $array;
		}
		if(!is_array($array)){
			$this->model->error('Can\t elaborate where string.');
		}

		$options = array_merge(array('for_where'=>true, 'auto_ml'=>false, 'main_alias'=>false, 'joins'=>array()), $opt);

		$str = array();
		foreach($array as $k => $v){
			$alreadyParsed = false;

			if(is_array($v)){
				if(!is_numeric($k) and (strtoupper($k)==='OR' or strtoupper($k)==='AND')){
					$sub_str = $this->makeSqlString($table, $v, $k, $options);
					if(!empty($sub_str))
						$str[] = '('.$sub_str.')';
					continue;
				}elseif(isset($v['operator'], $v['sub'])){
					$sub_str = $this->makeSqlString($table, $v['sub'], $v['operator'], $options);
					if(!empty($sub_str))
						$str[] = '('.$sub_str.')';
					continue;
				}else{
					$n_elementi = count($v);
					if($n_elementi<2 or $n_elementi>4 or count(array_filter(array_keys($v), 'is_numeric'))<$n_elementi) continue;

					switch($n_elementi){
						case 2:
							if(is_numeric($k)){
								$k = $v[0];
								$operator = '=';
							}else{
								$operator = $v[0];

								if(strtoupper($operator)==='IN'){
									if(!is_array($v[1]))
										$this->model->error('Expected array after a "in" clause');

									$alreadyParsed = true;
									$v[1] = '('.implode(',', array_map(function($el){ return $this->elaborateValue($el); }, $v[1])).')';
								}
							}
							$v1 = $v[1];
							break;
						case 3:
							if($v[0]=='BETWEEN'){
								$operator = $v[0];
								$v1 = $v[1];
								$v2 = $v[2];
							}else{
								$k = $v[0];
								$operator = $v[1];
								$v1 = $v[2];
							}
							break;
						case 4:
							if($v[1]!='BETWEEN') continue;
							$k = $v[0];
							$operator = $v[1];
							$v1 = $v[2];
							$v2 = $v[3];
							break;
					}
				}
			}else{
				if(is_numeric($k)){
					$str[] = '('.$v.')';
					continue;
				}else{
					$v1 = $v;
					$operator = '=';
				}
			}

			$k = $this->elaborateField($table, $k, $options);

			if(!$alreadyParsed){
				if($v1===null){
					$v1 = 'NULL';
					if($options['for_where']){
						if($operator=='=') $operator = 'IS';
						elseif($operator=='!=') $operator = 'IS NOT';
					}
				}else{
					$v1 = $this->elaborateValue($v1);
				}
			}

			if($operator=='BETWEEN'){
				if($v2===null) $v2 = 'NULL';
				else $v2 = $this->elaborateValue($v2);

				$str[] = $k.' BETWEEN '.$v1.' AND '.$v2;
			}else
				$str[] = $k.' '.$operator.' '.$v1;
		}

		return implode(' '.$collante.' ', $str);
	}

	/**
	 * @param string $type
	 * @param int $n
	 * @return bool
	 */
	public function setQueryLimit($type, $n){
		switch($type){
			case 'query':
				$this->options['query-limit'] = $n;
				break;
			case 'table':
				$this->options['query-limit-table'] = $n;
				break;
			default:
				return false;
				break;
		}

		return true;
	}

	/* Dealing with table models */

	/** @return Table|bool */
	private function loadTable($table){
		if(!isset($this->tables[$table])){
			if(file_exists(__DIR__.'/data/'.$this->unique_id.'/'.$table.'.php')){
				include(__DIR__.'/data/'.$this->unique_id.'/'.$table.'.php');
				if(!isset($foreign_keys))
					$foreign_keys = array();
				$this->tables[$table] = new Table($table_columns, $foreign_keys);
			}else
				$this->tables[$table] = false;
		}
		return $this->tables[$table];
	}

	/**
	 * @param string $table
	 * @param array $data
	 * @return array
	 */
	private function filterColumns($table, $data){
		if($this->tables[$table]===false)
			return $data;

		$realData = array();
		foreach($data as $k => $v){
			if(array_key_exists($k, $this->tables[$table]->columns))
				$realData[$k] = $v;
		}
		return $realData;
	}

	/**
	 * @return Table|bool
	 */
	public function getTable($table){
		$this->loadTable($table);
		return $this->tables[$table];
	}
}