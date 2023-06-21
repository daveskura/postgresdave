/*
  -- Dave Skura, 2022
*/

CREATE TABLE films_recent AS
	SELECT * 
	FROM films 
	WHERE date_prod >= '2002-01-01'
;