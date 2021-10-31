import psycopg2
import pandas as pd


def main():
    def connect_to_db(connect_data_src: str = 'connection.ini'):
        with open(connect_data_src) as f:
            connect_data = dict(map(lambda x: (x.replace(' ', '').replace('\n', '').split(':')), f.readlines()))
            connection = psycopg2.connect(**connect_data)
        return connection

    def execute_query(connection, sql_query) -> pd.DataFrame:
        buff = list()
        with connection.cursor() as curs:
            curs.execute(sql_query)
            col_names = [desc[0] for desc in curs.description]
            for row in curs:
                buff.append(tuple(map(lambda x: x.replace(' ', '') if type(x) is str else x, row)))
        return pd.DataFrame(buff, columns=col_names)

    query_1 = '''
    SELECT airlines.airline_id, COALESCE(tab.count, 0) AS airline_flight_count 
 	    FROM airlines LEFT JOIN (
 		    SELECT airline_id, COUNT(*) FROM flight_airline LEFT JOIN airlines 
 			    ON flight_airline.airline = airlines.airline_id GROUP BY airline_id
 	        ) AS tab
 	    ON airlines.airline_id = tab.airline_id ORDER BY airlines.airline_id;
    '''
    query_2 = '''
        SELECT airports.airport_id, COALESCE(tab.count, 0) AS flight_count
	        FROM airports LEFT JOIN (
		        SELECT origin_airport, COUNT(*) FROM flight_route GROUP BY origin_airport
            ) AS tab
        ON airports.airport_id = tab.origin_airport ORDER BY airports.airport_id;	
    '''

    query_3 = '''
        SELECT airports.airport_id, COALESCE(tab.count, 0) AS flight_count
	        FROM airports LEFT JOIN (
		        SELECT destination_airport, COUNT(*) FROM flight_route GROUP BY destination_airport
            ) AS tab
        ON airports.airport_id = tab.destination_airport ORDER BY airports.airport_id;
    '''

    with connect_to_db() as c:
        res_1 = execute_query(c, query_1)
        print(res_1, '\n')

        res_2 = execute_query(c, query_2)
        print(res_2, '\n')

        res_3 = execute_query(c, query_3)
        print(res_3, '\n')


if __name__ == '__main__':
    main()
