create table logs.logs_deltas (
	log_id serial,
	delta_id int,
	start_date timestamp,
	end_date timestamp,
	table_name varchar(30),
	
	primary key (log_id)
);
commit;

-- drop table logs_deltas;
-- commit;

select * from logs.logs_deltas;
	
	
	
