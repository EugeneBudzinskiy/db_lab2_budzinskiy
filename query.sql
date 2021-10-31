SELECT airlines.airline_id, COALESCE(tab.count, 0) AS airline_flight_count 
 	FROM airlines LEFT JOIN (
 		SELECT airline_id, COUNT(*) FROM flight_airline LEFT JOIN airlines 
 			ON flight_airline.airline = airlines.airline_id GROUP BY airline_id
 	) AS tab
 	ON airlines.airline_id = tab.airline_id ORDER BY airlines.airline_id;


SELECT airports.airport_id, COALESCE(tab.count, 0) AS flight_count
	FROM airports LEFT JOIN (
		SELECT origin_airport, COUNT(*) FROM flight_route GROUP BY origin_airport
	) AS tab
	ON airports.airport_id = tab.origin_airport ORDER BY airports.airport_id;


SELECT airports.airport_id, COALESCE(tab.count, 0) AS flight_count
	FROM airports LEFT JOIN (
		SELECT destination_airport, COUNT(*) FROM flight_route GROUP BY destination_airport
	) AS tab
	ON airports.airport_id = tab.destination_airport ORDER BY airports.airport_id;
	
