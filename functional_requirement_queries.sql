--Requirement 1: Which companies operate pipelines in New Mexico?
SELECT DISTINCT op_name AS company
FROM(
	SELECT *,unnest(nodes) AS node
	FROM (
		SELECT *
		FROM pipelines, operators
		WHERE pipeline_op_id = op_id 
	) AS unnestedNodes
) AS opNames
WHERE node IN ( 
	SELECT city_id
	FROM(
		SELECT city_id, location, state_bounding_polygon
		FROM cities, states
		WHERE abbr = 'NM'
	) AS pointsInStatePolygon
WHERE ST_Within(location, state_bounding_polygon) = TRUE
)
	

--Requirement 2: Total length of pipeline owned by each company?
SELECT op_name AS company, round(sum) AS metres
FROM (
	SELECT pipeline_op_id, sum(distance)
	FROM (
		SELECT pipeline_op_id, ST_Distance(ST_Transform(location,2761), lag(ST_Transform(location,2761)) OVER (PARTITION BY pipeline_name ORDER BY i)) AS distance
		FROM pipelines
		CROSS JOIN unnest(nodes) i
		JOIN cities ON city_id = i
	) AS pipelineLengths
	GROUP BY pipeline_op_id
)as sumTable, operators
WHERE pipeline_op_id = op_id



--Requirement 3: Which companies are location in Arizona?
SELECT op_name AS company
FROM operators
INNER JOIN states ON operators.state_abbr = states.abbr
WHERE states.name = 'Arizona'


--Requirement 4: How long will the supply in Tucson last?  
SELECT round((ST_Area(ST_Transform(bounding_polygon_2d,2761))*height)/gasPerDay) AS days_Remaining
FROM (
	SELECT city_name, city_id, population*rate AS gasPerDay
	FROM cities 
	INNER JOIN consumption ON cities.gas_tier = consumption.tier_id
	WHERE city_name = 'Tucson'
) AS t
INNER JOIN storage ON t.city_id = storage.city_id



--Requirement 5: Which city supplied by one pipeline has the largest population?
SELECT city_name, states.name AS state, population
FROM cities INNER JOIN (
	SELECT count(*), unnest(nodes) AS i
	FROM pipelines	
	GROUP BY i
) AS pipelineCount
ON cities.city_id = i
JOIN states on cities.state_abbr = abbr
WHERE count = 1
ORDER BY population DESC LIMIT 1


--Requirement 6: Population growth of Santa Fe grow before the pileline doesn't meet demand
SELECT round((capacity*60*24)-(population*rate)) AS pop_growth_limit
FROM  (
	SELECT capacity, unnest(nodes) AS node
	FROM pipelines
) AS unnested, (
	SELECT city_id, population, rate
	FROM cities INNER JOIN consumption ON cities.gas_tier = consumption.tier_id
	WHERE city_name = 'Santa Fe'
) AS tierJoin 
WHERE node = city_id 


-- Requirement 7: Combined storage in each state
SELECT name AS state, round(volume) AS total_storage
FROM (
	SELECT state_abbr, sum(ST_Area(ST_Transform(bounding_polygon_2d,2761))*height) AS volume
	FROM (
		SELECT state_abbr, city_name, city_id
		FROM cities 
		INNER JOIN consumption ON cities.gas_tier = consumption.tier_id
	) AS t
	INNER JOIN storage ON t.city_id = storage.city_id
	GROUP BY state_abbr
) AS v
JOIN states ON v.state_abbr = states.abbr


--Requirement 8: Which pipelines share routes?
SELECT DISTINCT a.pipeline_name, b.pipeline_name
FROM(
	SELECT i, pipeline_name, ST_MakeLine(ST_Transform(location,2761), lag(ST_Transform(location,2761)) OVER (PARTITION BY pipeline_name ORDER BY i)) AS line
	FROM pipelines
	CROSS JOIN unnest(nodes) i
	JOIN cities ON city_id = i
) AS a,(
	SELECT i, pipeline_name, ST_MakeLine(ST_Transform(location,2761), lag(ST_Transform(location,2761)) OVER (PARTITION BY pipeline_name ORDER BY i)) AS line
	FROM pipelines
	CROSS JOIN unnest(nodes) i
	JOIN cities ON city_id = i
) AS b
WHERE ST_Intersects(a.line, b.line) AND a.i < b.i AND a.pipeline_name != b.pipeline_name
GROUP BY a.pipeline_name, b.pipeline_name


--Requirement 9: Which companies own pipelines cross each other?
SELECT DISTINCT a.op_name, b.op_name
FROM
(
	SELECT op_name, pipeline_op_id, ST_MakeLine(ST_Transform(location,2761), lag(ST_Transform(location,2761)) OVER (PARTITION BY pipeline_name ORDER BY i)) AS line
	FROM pipelines, operators
	CROSS JOIN unnest(nodes) i
	JOIN cities ON city_id = i
	WHERE pipelines.pipeline_op_id = operators.op_id 
) AS a,(
	SELECT op_name, pipeline_op_id, ST_MakeLine(ST_Transform(location,2761), lag(ST_Transform(location,2761)) OVER (PARTITION BY pipeline_name ORDER BY i)) AS line
	FROM pipelines, operators
	CROSS JOIN unnest(nodes) i
	JOIN cities ON city_id = i
	WHERE pipelines.pipeline_op_id = operators.op_id 
) AS b
WHERE ST_Crosses(a.line, b.line) AND a.pipeline_op_id > b.pipeline_op_id


--Requirement 4: How long will the supply in Tuscon last? (using SFCGAL backend)
SELECT (ST_Volume(ST_MakeSolid(ST_Transform(bounding_polyhedral_surface_3d,2761))))/gasPerDay AS daysRemaining 
FROM (
	SELECT city_name, city_id, population*rate AS gasPerDay
	FROM cities 
	INNER JOIN consumption ON cities.gas_tier = consumption.tier_id
	WHERE city_name = 'Tucson'
) AS t
INNER JOIN storage ON t.city_id = storage.city_id


--Requirement 7: Combined Storage in each state (using SFCGAL backend)
SELECT name AS state, round(volume) AS total_storage
FROM (
	SELECT state_abbr, sum(ST_Volume(ST_MakeSolid(ST_Transform(bounding_polyhedral_surface_3d,2761)))) AS volume
	FROM (
		SELECT state_abbr, city_name, city_id
		FROM cities 
		INNER JOIN consumption ON cities.gas_tier = consumption.tier_id
	) AS t
	INNER JOIN storage ON t.city_id = storage.city_id
	GROUP BY state_abbr
) AS v
JOIN states ON v.state_abbr = states.abbr







