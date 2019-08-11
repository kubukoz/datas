create table users(id bigserial primary key, name text not null, age int not null);
-- create table genres(id bigserial primary key, name text);
create table books(id bigserial primary key,  user_id bigint references users(id) not null, parent_id bigint references books(id));
insert into users(id, name, age) values(1, 'Jon', 36), (2, 'Jakub', 23), (3, 'John', 40);
insert into books(id, user_id, parent_id) values (1, 2, null), (2, 3, 1);
