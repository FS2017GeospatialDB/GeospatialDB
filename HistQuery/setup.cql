truncate node_plevel19;
truncate inserted_p19;
truncate deleted_p19;

insert into node_plevel19 (part_lv19, id, time, feature) values (1,1,now(),'hello');
insert into node_plevel19 (part_lv19, id, time, feature) values (2,2,now(),'world');
