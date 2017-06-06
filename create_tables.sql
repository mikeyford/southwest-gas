--States table creation
CREATE TABLE states (
	abbr        varchar(2)	NOT NULL,
	name        varchar(40) NOT NULL,
	PRIMARY KEY (abbr)
);
SELECT AddGeometryColumn('states', 'state_bounding_polygon', 4326, 'POLYGON', 2 );
ALTER TABLE states ALTER COLUMN state_bounding_polygon SET NOT NULL;


--Consumption table creation
CREATE TABLE consumption (
	tier_id		int NOT NULL,
	name		varchar(40) NOT NULL,
	rate		real NOT NULL,
	PRIMARY KEY (tier_id),
	CONSTRAINT positive_rate CHECK(rate>0),	
	CONSTRAINT valid_tier CHECK(tier_id BETWEEN 1 and 4)
);


--Cities table creation
CREATE TABLE cities (
	city_id		int NOT NULL,
	city_name	varchar(40) NOT NULL,
	state_abbr	char(2) references states(abbr)	NOT NULL,
	population 	int NOT NULL,
	gas_tier	int references consumption(tier_id) NOT NULL,
	PRIMARY KEY (city_id),
	CONSTRAINT positive_population CHECK(population>0),
	CONSTRAINT valid_tier CHECK(gas_tier BETWEEN 1 and 4) 
);
SELECT AddGeometryColumn('cities', 'location', 4326, 'POINT', 2 );
ALTER TABLE cities ALTER COLUMN location SET NOT NULL;


--Operators table creation
CREATE TABLE operators (
	op_id		int NOT NULL,
	op_name 	varchar(40) NOT NULL,
	address_1 	varchar(100) NOT NULL,
	address_2 	varchar(100),
	zip			int NOT NULL ,
	state_abbr 	char(2) references states(abbr)	NOT NULL,
	PRIMARY KEY (op_id),
	CONSTRAINT southwest_zip CHECK(zip BETWEEN 80000 and 89999)
);


--Pipelines table creation
CREATE TABLE pipelines (
	pipeline_id		integer NOT NULL,
	pipeline_name	varchar(40) NOT NULL,
	pipeline_op_id	int references operators(op_id) NOT NULL,
	nodes			integer[] NOT NULL,
	capacity		real NOT NULL,
	PRIMARY KEY (pipeline_id),
	CONSTRAINT positive_capacity CHECK(capacity>0)
);	


--Storage table creation
CREATE TABLE storage (
	storage_id	int NOT NULL,
	city_id		int references cities(city_id) NOT NULL,
	storage_name varchar(40), 
	height		real NOT NULL,
	PRIMARY KEY (storage_id),
	CONSTRAINT positive_height CHECK(height>0)
);
SELECT AddGeometryColumn('storage', 'bounding_polygon_2d', 4326, 'POLYGON', 2 );
SELECT AddGeometryColumn('storage', 'bounding_polyhedral_surface_3d', 4326, 'POLYHEDRALSURFACE', 3 );
ALTER TABLE storage ALTER COLUMN bounding_polygon_2d SET NOT NULL;


