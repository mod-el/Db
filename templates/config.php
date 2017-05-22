<style>
	td{
		padding: 5px;
	}
</style>

<form action="?" method="post">
	<?php csrfInput(); ?>
	<table id="cont-packages">
		<tr style="color: #2693FF">
			<td>
				Delete?
			</td>
			<td>
				Idx
			</td>
			<td>
				Host
			</td>
			<td>
				User
			</td>
			<td>
				Password
			</td>
			<td>
				Database
			</td>
		</tr>
		<?php
		$databases = isset($this->options['config']['databases']) ? $this->options['config']['databases'] : [];
		foreach($databases as $idx=>$db){
			?>
			<tr>
				<td>
					<input type="checkbox" name="delete-<?=$idx?>" value="yes" />
				</td>
				<td>
					<input type="text" name="<?=$idx?>-idx" placeholder="idx" value="<?=entities($idx)?>" />
				</td>
				<td>
					<input type="text" name="<?=$idx?>-host" placeholder="host" value="<?=entities($db['host'])?>" />
				</td>
				<td>
					<input type="text" name="<?=$idx?>-username" placeholder="username" value="<?=entities($db['username'])?>" />
				</td>
				<td>
					<input type="password" name="<?=$idx?>-password" placeholder="empty to keep current" />
				</td>
				<td>
					<input type="text" name="<?=$idx?>-database" placeholder="database" value="<?=entities($db['database'])?>" />
				</td>
			</tr>
			<?php
		}
		?>
		<tr>
			<td>
				New:
			</td>
			<td>
				<input type="text" name="new-idx" placeholder="idx" value="<?=$databases ? '' : 'primary'?>" />
			</td>
			<td>
				<input type="text" name="new-host" placeholder="host" value="<?=$databases ? '' : '127.0.0.1'?>" />
			</td>
			<td>
				<input type="text" name="new-username" placeholder="username" value="<?=$databases ? '' : 'root'?>" />
			</td>
			<td>
				<input type="password" name="new-password" placeholder="password" />
			</td>
			<td>
				<input type="text" name="new-database" placeholder="database" />
			</td>
		</tr>
	</table>

	<p>
		<input type="submit" value="Save" />
	</p>
</form>